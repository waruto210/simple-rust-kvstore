use anyhow::{Context, Result};
use kvs::KvsClient;
use std::net::SocketAddr;
use std::process::exit;
use structopt::StructOpt;
use tokio;

const DEFAULT_ADDRESS: &str = "127.0.0.1:4000";

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-client")]
pub struct ClientArgs {
    #[structopt(subcommand)]
    pub command: Command,
}
#[derive(Debug, StructOpt)]
pub enum Command {
    #[structopt(name = "set", about = "Set the value of a string key to a string")]
    Set {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,

        #[structopt(name = "VALUE", help = "The string value of the key")]
        value: String,

        #[structopt(
            long,
            help = "The server address to be connected.",
            default_value = DEFAULT_ADDRESS,
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },

    #[structopt(name = "get", about = "Get the string value of a given string key")]
    Get {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,

        #[structopt(
            long,
            help = "The server address to be connected.",
            default_value = DEFAULT_ADDRESS,
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },

    #[structopt(name = "rm", about = "Remove a given key")]
    Remove {
        #[structopt(name = "KEY", help = "A string key")]
        key: String,

        #[structopt(
                long,
                help = "The server address to be connected.",
                default_value = DEFAULT_ADDRESS,
                parse(try_from_str)
            )]
        addr: SocketAddr,
    },
}
fn main() -> Result<()> {
    let opt = ClientArgs::from_args();

    let rt = tokio::runtime::Runtime::new().context("failed to create tokio runtime")?;
    rt.block_on(async move {
        if let Err(err) = run(opt).await {
            eprintln!("{}", err);
            exit(1);
        }
    });

    Ok(())
}

async fn run(opt: ClientArgs) -> Result<()> {
    match opt.command {
        Command::Set { key, value, addr } => {
            // println!("set {} {} {}", key, value, addr);
            let mut client = KvsClient::connect(addr).await?;
            client.set(key, value).await?;
        }
        Command::Get { key, addr } => {
            let mut client = KvsClient::connect(addr).await?;
            if let Some(value) = client.get(key).await? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Command::Remove { key, addr } => {
            let mut client = KvsClient::connect(addr).await?;
            client.remove(key).await?;
        }
    }
    Ok(())
}
