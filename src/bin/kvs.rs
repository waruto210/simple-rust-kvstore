use std::process;

use clap::{self, load_yaml, App};

fn main() {
    let yaml = load_yaml!("cli.yaml");
    let matches = App::from(yaml)
        .name(env!("CARGO_PKG_NAME"))
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .get_matches();

    match matches.subcommand() {
        ("set", Some(_matches)) => {
            // let key = _matches.value_of("KEY").expect("KEY argument missing");
            // let value = _matches.value_of("VALUE").expect("VALUE argument missing");
            // println!("key: {}, value: {}", key, value);
            eprintln!("unimplemented");
            process::exit(1);
        }
        ("get", Some(_matches)) => {
            // let key = _matches.value_of("KEY").expect("VALUE argument missing");
            // println!("key: {}", key);
            eprintln!("unimplemented");
            process::exit(1);
        }
        ("rm", Some(_matches)) => {
            // let key = _matches.value_of("KEY").expect("VALUE argument missing");
            // println!("key: {}", key);
            eprintln!("unimplemented");
            process::exit(1);
        }
        _ => unreachable!(),
    }
}
