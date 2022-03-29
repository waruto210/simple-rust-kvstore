use clap::arg_enum;
use env_logger::Builder;
use kvs::Result;
use kvs::{KvStore, KvsEngine, KvsServer, SledKvsEngine};
use log::{error, info, LevelFilter};
use serde::{Deserialize, Serialize};
use std::fs;
use std::net::SocketAddr;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::{env::current_dir, process::exit};
use structopt::StructOpt;
use tokio;
use tokio::net::ToSocketAddrs;

const DEFAULT_ADDRESS: &str = "127.0.0.1:4000";

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, PartialEq, Eq, Clone, Copy, Serialize, Deserialize)]
    enum Engine {
        kvs,
        sled
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-server")]
pub struct ServerArgs {
    #[structopt(
        long,
        help = "The address to be bind.",
        default_value = DEFAULT_ADDRESS,
        parse(try_from_str)
    )]
    addr: SocketAddr,

    #[structopt(
        long,
        help = "The engine to be used.",
        possible_values = &Engine::variants(),
        case_insensitive = false,
    )]
    engine: Option<Engine>,
}
fn main() {
    Builder::new().filter_level(LevelFilter::Debug).init();
    let opt = ServerArgs::from_args();
    if let Err(err) = init(opt) {
        eprintln!("{}", err);
        exit(1);
    }
}
fn init(opt: ServerArgs) -> Result<()> {
    let engine = determine_engine(&opt)?;
    serde_json::to_writer(
        fs::File::create(current_dir()?.join("engine.log"))?,
        &engine,
    )?;
    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: {}", engine);
    info!("Server address: {}", opt.addr);
    match engine {
        Engine::kvs => start(KvStore::open(current_dir()?)?, opt.addr),
        Engine::sled => start(SledKvsEngine::open(current_dir()?)?, opt.addr),
    }
}

fn start(engine: impl KvsEngine + Sync, addr: impl ToSocketAddrs) -> Result<()> {
    let state = Arc::new(AtomicBool::new(true));
    let mut server = KvsServer::new(engine, state);
    let rt = tokio::runtime::Runtime::new().unwrap();
    let _ = rt.block_on(server.start(addr));
    Ok(())
}

fn determine_engine(opt: &ServerArgs) -> Result<Engine> {
    let previous_engine = previous_engine()?;
    let engine = {
        if opt.engine.is_none() && previous_engine.is_none() {
            Engine::kvs
        } else if opt.engine.is_some() && previous_engine.is_none() {
            opt.engine.unwrap()
        } else if opt.engine.is_none() && previous_engine.is_some() {
            previous_engine.unwrap()
        } else {
            if opt.engine != previous_engine {
                error!(
                    "Engine inconsistent, previous engine: {:?}, choosen engine: {:?}",
                    previous_engine.unwrap(),
                    opt.engine.unwrap()
                );
                exit(1);
            }
            previous_engine.unwrap()
        }
    };
    Ok(engine)
}

fn previous_engine() -> Result<Option<Engine>> {
    let engine_log = current_dir()?.join("engine.log");

    if !engine_log.exists() {
        return Ok(None);
    }
    let engine: Engine = serde_json::from_reader(fs::File::open(engine_log)?)?;
    Ok(Some(engine))
}
