use crate::{
    protocol::{Request, Response},
    thread_pool::ThreadPool,
};
use crate::{KvsEngine, Result};
use log::{debug, error};
use serde_json;
use std::io::{BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// A `KvsServer`
pub struct KvsServer<E, P>
where
    E: KvsEngine,
    P: ThreadPool,
{
    engine: E,
    pool: P,
    state: Arc<AtomicBool>,
}

impl<E, P> KvsServer<E, P>
where
    E: KvsEngine,
    P: ThreadPool,
{
    /// A new `KvsServer`
    pub fn new(engine: E, pool: P, state: Arc<AtomicBool>) -> KvsServer<E, P> {
        KvsServer {
            engine,
            pool,
            state,
        }
    }

    /// start running a `KvsServer`
    /// maintain a store engine,
    // listen for incoming request
    pub fn start<A>(&mut self, addr: A) -> Result<()>
    where
        A: ToSocketAddrs,
    {
        let listener = TcpListener::bind(addr)?;
        for stream in listener.incoming() {
            if !self.state.load(Ordering::SeqCst) {
                break;
            }
            let engine = self.engine.clone();
            self.pool.spawn(move || match stream {
                Err(err) => {
                    error!("Accept failed {}", err);
                }
                Ok(stream) => {
                    if let Err(err) = handle_request(engine, stream) {
                        error!("Error when handle request {}", err);
                    }
                }
            });
        }
        Ok(())
    }
}

/// handle a income connection
fn handle_request(engine: impl KvsEngine, stream: TcpStream) -> Result<()> {
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);
    let request_stream = serde_json::de::Deserializer::from_reader(reader).into_iter::<Request>();
    let addr = stream.peer_addr()?;
    for request in request_stream {
        let request = request?;
        debug!("Recv req {:?} from {}", request, addr);
        let res = match request {
            Request::Get { key } => match engine.get(key) {
                Ok(value) => Response::Ok(value),
                Err(err) => Response::Err(format!("err: {}", err)),
            },
            Request::Set { key, value } => match engine.set(key, value) {
                Ok(_) => Response::Ok(None),
                Err(err) => Response::Err(format!("err: {}", err)),
            },
            Request::Rm { key } => match engine.remove(key) {
                Ok(_) => Response::Ok(None),
                Err(err) => Response::Err(format!("err: {}", err)),
            },
        };
        send_response(&mut writer, res, addr)?;
    }

    Ok(())
}

/// send response to client
fn send_response<W>(writer: &mut W, value: Response, addr: SocketAddr) -> Result<()>
where
    W: Write,
{
    serde_json::to_writer(writer as &mut W, &value)?;
    writer.flush()?;

    debug!("Send response {:?} to {}", value, addr);

    Ok(())
}
