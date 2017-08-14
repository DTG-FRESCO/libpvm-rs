use iostream::IOStream;
use libc::c_char;
use std::ffi::CStr;
use std::ops::FnOnce;
use std::os::unix::io::{RawFd, FromRawFd};
use std::ptr;
use std::sync::mpsc;
use std::thread;
use std;

use neo4j::cypher::CypherStream;
use serde_json::Deserializer;

use persist;
use trace::TraceEvent;
use invbloom::InvBloom;

#[repr(C)]
#[derive(Debug)]
#[derive(PartialEq)]
pub enum CfgMode {
    Auto,
    Advanced,
}

#[repr(C)]
#[derive(Debug)]
pub struct AdvancedConfig {
    consumer_threads: usize,
    persistence_threads: usize,
}

#[repr(C)]
pub struct Config {
    cfg_mode: CfgMode,
    db_server: *mut c_char,
    db_user: *mut c_char,
    db_password: *mut c_char,
    cfg_detail: *const AdvancedConfig,
}

#[derive(Debug)]
pub struct RConfig {
    cfg_mode: CfgMode,
    db_server: String,
    db_user: String,
    db_password: String,
    cfg_detail: Option<AdvancedConfig>,
}

pub struct LibOpus {
    cfg: RConfig,
}


#[repr(C)]
pub struct OpusHdl(LibOpus);

fn string_from_c_char<F>(str_p: *mut c_char, default: F) -> String
where
    F: FnOnce(std::ffi::IntoStringError) -> String,
{
    unsafe { CStr::from_ptr(str_p) }
        .to_owned()
        .into_string()
        .unwrap_or_else(default)
}


#[no_mangle]
pub unsafe extern "C" fn opus_init(cfg: Config) -> *mut OpusHdl {
    let hdl = Box::new(OpusHdl(LibOpus {
        cfg: RConfig {
            cfg_mode: cfg.cfg_mode,
            db_server: string_from_c_char(cfg.db_server, |_| String::from("localhost:7687")),
            db_user: string_from_c_char(cfg.db_user, |_| String::from("neo4j")),
            db_password: string_from_c_char(cfg.db_password, |_| String::from("opus")),
            cfg_detail: if cfg.cfg_detail.is_null() {
                Option::None
            } else {
                Option::Some(ptr::read(cfg.cfg_detail))
            },
        },
    }));
    Box::into_raw(hdl)
}

#[no_mangle]
pub unsafe extern "C" fn print_cfg(hdl: *const OpusHdl) {
    let hdl = &(*hdl).0;
    println!("LibOpus {:?}", hdl.cfg);
}

#[no_mangle]
pub unsafe extern "C" fn process_events(hdl: *mut OpusHdl, fd: RawFd) {
    let hdl = &mut (&mut (*hdl).0);
    let stream = IOStream::from_raw_fd(fd);
    let mut db =
        match CypherStream::connect(&hdl.cfg.db_server, &hdl.cfg.db_user, &hdl.cfg.db_password) {
            Ok(conn) => conn,
            Err(ref s) => {
                println!("Database connection error: {}", s);
                return;
            }
        };
    let evt_str = Deserializer::from_reader(stream).into_iter::<TraceEvent>();

    let cache = InvBloom::new();

    let (mut send, recv) = mpsc::sync_channel(1024);

    let db_worker = thread::spawn(move || for tr in recv.iter() {
        if let Err(e) = persist::execute(&mut db, &tr) {
            println!("{}", e);
        }
    });

    for res in evt_str {
        match res {
            Ok(evt) => {
                if let Err(perr) = persist::parse_trace(&evt, &mut send, &cache) {
                    println!("PVM parsing error {}", perr);
                }
            }
            Err(perr) => {
                println!("Parsing error: {}", perr);
                break;
            }
        }
    }
    drop(send);
    if let Err(e) = db_worker.join() {
        println!("Database thread panicked: {:?}", e);
    }
}

#[no_mangle]
pub unsafe extern "C" fn opus_cleanup(hdl: *mut OpusHdl) {
    drop(Box::from_raw(hdl));
    println!("Cleaning up..");
}
