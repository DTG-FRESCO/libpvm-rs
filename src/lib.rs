#![feature(box_patterns)]
#![feature(nll)]
#![feature(specialization)]

pub extern crate pvm_cfg as cfg;
pub extern crate pvm_data as data;
pub extern crate pvm_views as views;

extern crate chrono;
extern crate either;
#[macro_use]
extern crate lazy_static;
extern crate lending_library;
extern crate libc;
#[macro_use]
extern crate maplit;
extern crate neo4j;
extern crate rayon;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate transactions;
extern crate nix;
extern crate serde_json;
extern crate uuid;
extern crate zip;

const VERSION: &str = env!("CARGO_PKG_VERSION");

macro_rules! timeit {
    ($E:expr) => {{
        use std::time::Instant;
        let now = Instant::now();
        let ret = { $E };
        let dur = now.elapsed();
        eprintln!(
            "{} took {:.3}",
            stringify!($E),
            dur.as_secs() as f64 + f64::from(dur.subsec_nanos()) * 1e-9
        );
        ret
    }};
}

pub use c_api::*;

pub mod c_api;
pub mod engine;
pub mod ingest;
pub mod invbloom;
pub mod iostream;
pub mod neo4j_glue;
pub mod persistance;
pub mod query;
pub mod trace;
