use serde::{Deserialize, Serialize};
/// Enum represents `Request` to k/v server
#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    Set { key: String, value: String },
    Get { key: String },
    Rm { key: String },
}
/// Enum represents `Response` send from k/v server to client
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Err(String),
    Ok(Option<String>),
}
