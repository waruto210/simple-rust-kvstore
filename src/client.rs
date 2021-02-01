use crate::{protocol, KvsError, Result};
use serde::Deserialize;
use serde_json::de::{Deserializer, IoRead};
use std::{io::Write, net::TcpStream};
use std::{
    io::{BufReader, BufWriter},
    net::ToSocketAddrs,
};

/// A k/v store client
pub struct KvsClient {
    sender: BufWriter<TcpStream>,
    // because the from_reader method needs input stream end
    // so receiver is not a simple BufReader
    // https://docs.serde.rs/serde_json/fn.from_reader.html
    receiver: Deserializer<IoRead<BufReader<TcpStream>>>,
}

impl KvsClient {
    /// connect to a `KvsServer`
    pub fn connect<A>(addr: A) -> Result<KvsClient>
    where
        A: ToSocketAddrs,
    {
        let stream = TcpStream::connect(addr)?;

        Ok(KvsClient {
            sender: BufWriter::new(stream.try_clone()?),
            receiver: Deserializer::from_reader(BufReader::new(stream)),
        })
    }

    /// Set the string value of a given string key.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let req = protocol::Request::Set { key, value };
        serde_json::to_writer(&mut self.sender, &req)?;
        self.sender.flush()?;
        let response = protocol::Response::deserialize(&mut self.receiver)?;
        match response {
            protocol::Response::Ok(_) => Ok(()),
            protocol::Response::Err(err) => Err(KvsError::StringError(err)),
        }
    }

    /// Get the string value of a given string key.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let req = protocol::Request::Get { key };
        serde_json::to_writer(&mut self.sender, &req)?;
        self.sender.flush()?;
        let response = protocol::Response::deserialize(&mut self.receiver)?;
        match response {
            protocol::Response::Ok(value) => Ok(value),
            protocol::Response::Err(err) => Err(KvsError::StringError(err)),
        }
    }

    /// Remove a given string key.
    pub fn remove(&mut self, key: String) -> Result<()> {
        let req = protocol::Request::Rm { key };
        serde_json::to_writer(&mut self.sender, &req)?;
        self.sender.flush()?;
        let response = protocol::Response::deserialize(&mut self.receiver)?;
        match response {
            protocol::Response::Ok(_) => Ok(()),
            protocol::Response::Err(err) => Err(KvsError::StringError(err)),
        }
    }
}
