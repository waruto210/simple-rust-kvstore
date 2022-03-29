use crate::{
    protocol::{Request, Response},
    KvsError, Result,
};
use futures::prelude::*;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpStream, ToSocketAddrs,
};
use tokio_serde::formats::SymmetricalJson;
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
/// A k/v store client
pub struct KvsClient {
    // sender: BufWriter<TcpStream>,
    // because the from_reader method needs input stream end
    // so receiver is not a simple BufReader
    // https://docs.serde.rs/serde_json/fn.from_reader.html
    // receiver: Deserializer<IoRead<BufReader<TcpStream>>>,
    reader: tokio_serde::SymmetricallyFramed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        Response,
        SymmetricalJson<Response>,
    >,
    writer: tokio_serde::SymmetricallyFramed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        Request,
        SymmetricalJson<Request>,
    >,
}

impl KvsClient {
    /// connect to a `KvsServer`
    pub async fn connect<A>(addr: A) -> Result<KvsClient>
    where
        A: ToSocketAddrs,
    {
        let stream = TcpStream::connect(addr).await?;

        let (read_half, write_half) = stream.into_split();

        let reader = tokio_serde::SymmetricallyFramed::new(
            FramedRead::new(read_half, LengthDelimitedCodec::new()),
            SymmetricalJson::<Response>::default(),
        );
        let writer = tokio_serde::SymmetricallyFramed::new(
            FramedWrite::new(write_half, LengthDelimitedCodec::new()),
            SymmetricalJson::<Request>::default(),
        );

        Ok(KvsClient { reader, writer })
    }

    /// Set the string value of a given string key.
    pub async fn set(&mut self, key: String, value: String) -> Result<()> {
        let resp = self.send_and_receive(Request::Set { key, value }).await?;
        match resp {
            Response::Ok(_) => Ok(()),
            Response::Err(e) => Err(KvsError::OtherError(e).into()),
        }
    }

    /// Get the string value of a given string key.
    pub async fn get(&mut self, key: String) -> Result<Option<String>> {
        let resp = self.send_and_receive(Request::Get { key }).await?;
        match resp {
            Response::Ok(v) => Ok(v),
            Response::Err(e) => Err(KvsError::OtherError(e).into()),
        }
    }

    /// Remove a given string key.
    pub async fn remove(&mut self, key: String) -> Result<()> {
        let resp = self.send_and_receive(Request::Rm { key }).await?;
        match resp {
            Response::Ok(_) => Ok(()),
            Response::Err(e) => Err(KvsError::OtherError(e).into()),
        }
    }

    /// send a request and receive a response
    pub async fn send_and_receive(&mut self, req: Request) -> Result<Response> {
        self.writer.send(req).await?;
        match self.reader.try_next().await? {
            Some(resp) => Ok(resp),
            _ => Err(KvsError::OtherError("can't receive".to_string()).into()),
        }
    }

    /// close a client()
    pub fn close(self) {}
}
