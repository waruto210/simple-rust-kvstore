use crate::protocol::{Request, Response};
use crate::{KvsEngine, Result};
use log::{debug, error};
use serde_json;
use std::io::{BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};

/// A `KvsServer`
pub struct KvsServer<E>
where
    E: KvsEngine,
{
    engine: E,
}

impl<E> KvsServer<E>
where
    E: KvsEngine,
{
    /// A new `KvsServer`
    pub fn new(engine: E) -> KvsServer<E> {
        KvsServer { engine }
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
            match stream {
                Err(err) => {
                    error!("Accept failed {}", err);
                }
                Ok(stream) => {
                    self.handle(stream)?;
                }
            }
        }

        Ok(())
    }

    /// handle a income connection
    fn handle(&mut self, stream: TcpStream) -> Result<()> {
        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        let request_stream =
            serde_json::de::Deserializer::from_reader(reader).into_iter::<Request>();
        let addr = stream.peer_addr()?;
        for request in request_stream {
            let request = request?;
            debug!("Recv req {:?} from {}", request, addr);
            let res = match request {
                Request::Get { key } => match self.engine.get(key) {
                    Ok(value) => Response::Ok(value),
                    Err(err) => Response::Err(format!("err: {}", err)),
                },
                Request::Set { key, value } => match self.engine.set(key, value) {
                    Ok(_) => Response::Ok(None),
                    Err(err) => Response::Err(format!("err: {}", err)),
                },
                Request::Rm { key } => match self.engine.remove(key) {
                    Ok(_) => Response::Ok(None),
                    Err(err) => Response::Err(format!("err: {}", err)),
                },
            };
            send_response(&mut writer, res, addr)?;
        }

        Ok(())
    }
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
