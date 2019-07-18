#![feature(box_patterns)]
#![feature(specialization)]
#![feature(duration_float)]
#![feature(custom_attribute)]

pub extern crate pvm_data as data;
pub extern crate pvm_plugins as plugins;
pub extern crate pvm_views as view;

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[macro_export]
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

#[cfg(feature = "capi")]
pub use c_api::*;

#[cfg(feature = "capi")]
pub mod c_api;

pub mod cfg;
pub mod engine;
pub mod ingest;
pub mod invbloom;
pub mod iostream;
pub mod neo4j_glue;
pub mod query;
pub mod trace;
