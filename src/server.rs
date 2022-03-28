use crate::protocol::{Request, Response};
use crate::{KvsEngine, Result};
use futures::prelude::*;
use log::{debug, error, info};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::io::{BufReader, BufWriter};

use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_serde::formats::SymmetricalJson;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

/// A `KvsServer`
pub struct KvsServer<E>
where
    E: KvsEngine,
{
    engine: E,
    state: Arc<AtomicBool>,
}

impl<E> KvsServer<E>
where
    E: KvsEngine + Sync,
{
    /// A new `KvsServer`
    pub fn new(engine: E, state: Arc<AtomicBool>) -> KvsServer<E> {
        KvsServer { engine, state }
    }

    /// start running a `KvsServer`
    /// maintain a store engine,
    // listen for incoming request
    pub async fn start<A>(&mut self, addr: A) -> Result<()>
    where
        A: ToSocketAddrs,
    {
        let listener = TcpListener::bind(addr).await?;
        loop {
            let (stream, _) = listener.accept().await?;

            if !self.state.load(Ordering::Relaxed) {
                break;
            }
            let engine = self.engine.clone();
            tokio::spawn(async move {
                if let Err(err) = handle_request(engine, stream).await {
                    error!("{}", err);
                }
            });
        }
        Ok(())
    }
}

/// handle a income connection
async fn handle_request(engine: impl KvsEngine, mut stream: TcpStream) -> Result<()> {
    let addr = stream.peer_addr()?;
    let (read_half, write_half) = stream.split();
    // let buf_reader = BufReader::new(read_half);
    // let buf_writer = BufWriter::new(write_half);

    let mut reader = tokio_serde::SymmetricallyFramed::new(
        FramedRead::new(read_half, LengthDelimitedCodec::new()),
        SymmetricalJson::<Request>::default(),
    );

    let mut writer = tokio_serde::SymmetricallyFramed::new(
        FramedWrite::new(write_half, LengthDelimitedCodec::new()),
        SymmetricalJson::<Response>::default(),
    );

    while let Some(request) = reader.try_next().await? {
        debug!("Recv req {:?} from {}", request, addr);
        let res = match request {
            Request::Get { key } => match engine.get(key).await {
                Ok(value) => Response::Ok(value),
                Err(err) => Response::Err(format!("err: {}", err)),
            },
            Request::Set { key, value } => match engine.set(key, value).await {
                Ok(_) => Response::Ok(None),
                Err(err) => Response::Err(format!("err: {}", err)),
            },
            Request::Rm { key } => match engine.remove(key).await {
                Ok(_) => Response::Ok(None),
                Err(err) => Response::Err(format!("err: {}", err)),
            },
        };
        debug!("Send response {:?} to {}", res, addr);
        writer.send(res).await?;
    }
    // info!("conn close");
    Ok(())
}

/// close the server
pub async fn close_server<A: ToSocketAddrs>(state: Arc<AtomicBool>, addr: A) {
    state.store(false, Ordering::Relaxed);
    TcpStream::connect(addr).await.unwrap();
}
