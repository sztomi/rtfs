use std::env;
use std::ffi::OsStr;
use std::process;

use dotenv;
use envy;
use reqwest;

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;

mod artifactory;
mod rtfs;

#[derive(Deserialize, Debug)]
struct Env {
  rtfs_host: String,
  rtfs_user: String,
  rtfs_token: String,
}

struct ConsoleLogger;

impl log::Log for ConsoleLogger {
  fn enabled(&self, _metadata: &log::Metadata) -> bool {
    true
  }

  fn log(&self, record: &log::Record) {
    println!("{}: {}: {}", record.target(), record.level(), record.args());
  }

  fn flush(&self) {}
}

static LOGGER: ConsoleLogger = ConsoleLogger;

fn main() -> Result<(), reqwest::Error> {
  log::set_logger(&LOGGER).unwrap();
  log::set_max_level(log::LevelFilter::Debug);
  dotenv::dotenv().ok();
  let args: Vec<String> = env::args().collect();
  if args.len() != 3 {
    println!("Usage: rtfs <repo-name> <mount-point>");
    process::exit(1);
  }
  let env = envy::from_env::<Env>().unwrap_or_else(|e| {
    println!("Could not read environment or .env: {}", e);
    process::exit(1);
  });

  let rt = Box::new(artifactory::Artifactory::new(
    &env.rtfs_host,
    &env.rtfs_user,
    &env.rtfs_token,
  ));
  let filesystem = rtfs::RtFS::new(rt, args[1].clone());
  let fuse_args: Vec<&OsStr> =
    vec![&OsStr::new("-o"), &OsStr::new("auto_unmount")];
  fuse_mt::mount(fuse_mt::FuseMT::new(filesystem, 1), &args[2], &fuse_args)
    .unwrap();
  Ok(())
}
