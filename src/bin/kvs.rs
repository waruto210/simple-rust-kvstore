use clap::{self, load_yaml, App};
use kvs::KvStore;
use kvs::{KvsError, Result};
use std::env::current_dir;
use std::process;

fn main() -> Result<()> {
    let yaml = load_yaml!("cli.yaml");
    let matches = App::from(yaml)
        .name(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .get_matches();

    match matches.subcommand() {
        ("set", Some(_matches)) => {
            let key = _matches.value_of("KEY").expect("KEY argument missing");
            let value = _matches.value_of("VALUE").expect("VALUE argument missing");
            let mut store = KvStore::open(current_dir()?)?;
            store.set(key.to_string(), value.to_string())?;
        }
        ("get", Some(_matches)) => {
            // let key = _matches.value_of("KEY").expect("VALUE argument missing");
            // println!("key: {}", key);
            let key = _matches.value_of("KEY").expect("KEY argument missing");
            let mut store = KvStore::open(current_dir()?)?;
            if let Some(value) = store.get(key.to_string())? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        ("rm", Some(_matches)) => {
            let key = _matches.value_of("KEY").expect("KEY argument missing");
            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key.to_string()) {
                Ok(()) => {}
                Err(KvsError::KeyNotFound) => {
                    println!("Key not found");
                    process::exit(1);
                }
                Err(err) => return Err(err),
            };
        }
        _ => unreachable!(),
    }

    Ok(())
}
