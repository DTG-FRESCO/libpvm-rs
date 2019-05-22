mod db;
pub mod pvm;

use std::{
    fmt::Display,
    io::{BufRead, BufReader, Read},
    sync::mpsc::sync_channel,
};

use rayon::prelude::*;
use serde_json;

use crossbeam_utils::thread::scope;

use serde::de::DeserializeOwned;

use self::pvm::{PVMError, PVM};

const BATCH_SIZE: usize = 0x80_000;

pub trait Parseable: DeserializeOwned + Display + Send + Sized {
    fn init(pvm: &mut PVM);
    fn parse(&self, pvm: &mut PVM) -> Result<(), PVMError>;
    fn update(&mut self) {}
    fn set_offset(&mut self, offset: usize);
}

pub fn ingest_stream<R: Read + Send + 'static, T: Parseable + 'static>(stream: R, pvm: &mut PVM) {
    scope(|s| {
        let lines = BufReader::new(stream).lines().enumerate();

        T::init(pvm);

        let (f_out, p_in) = sync_channel(BATCH_SIZE);

        s.spawn(move |_| {
            for (n, l) in lines {
                match l {
                    Ok(mut l) => {
                        if l.is_empty() {
                            continue;
                        }
                        if l == "[" || l == "]" {
                            continue;
                        }
                        if l.starts_with(", ") {
                            l.drain(0..2);
                        }
                        f_out.send((n, l)).unwrap();
                    }
                    Err(perr) => {
                        eprintln!("Line: {}", n + 1);
                        eprintln!("File Reading error: {}", perr);
                        continue;
                    }
                }
            }
        });

        let (p_out, c_in) = sync_channel(BATCH_SIZE);

        s.spawn(move |_| {
            let mut pre_vec: Vec<(usize, String)> = Vec::with_capacity(BATCH_SIZE);
            let mut post_vec: Vec<(usize, Option<T>)> = Vec::with_capacity(BATCH_SIZE);

            loop {
                pre_vec.extend(p_in.iter().take(BATCH_SIZE));
                pre_vec
                    .par_iter()
                    .map(|(n, s)| match serde_json::from_slice::<T>(s.as_bytes()) {
                        Ok(mut evt) => {
                            evt.set_offset(*n);
                            evt.update();
                            (*n, Some(evt))
                        }
                        Err(perr) => {
                            eprintln!("Line: {}", n + 1);
                            eprintln!("JSON Parsing error: {}", perr);
                            eprintln!("{}", s);
                            (*n, None)
                        }
                    })
                    .collect_into(&mut post_vec);
                for e in post_vec.drain(..) {
                    p_out.send(e).unwrap();
                }
                if pre_vec.len() < BATCH_SIZE {
                    break;
                }
            }
        });

        s.spawn(|_| {
            for (n, tr) in c_in {
                if let Some(tr) = tr {
                    if let Err(e) = tr.parse(pvm) {
                        eprintln!("Line: {}", n + 1);
                        eprintln!("PVM Parsing error: {}", e);
                        eprintln!("{}", tr);
                    }
                }
            }
        });
    }).unwrap();

    println!("Missing Events:");
    for evt in pvm.unparsed_events.drain() {
        println!("{}", evt);
    }
}
