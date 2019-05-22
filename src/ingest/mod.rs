mod db;
pub mod pvm;

use std::{
    fmt::Display,
    io::{BufRead, BufReader, Read},
};

use rayon::prelude::*;
use serde_json;

use serde::de::DeserializeOwned;

use self::pvm::{PVMError, PVM};

const BATCH_SIZE: usize = 0x80_000;

pub trait Parseable: DeserializeOwned + Display + Send + Sized {
    fn init(pvm: &mut PVM);
    fn parse(&self, pvm: &mut PVM) -> Result<(), PVMError>;
    fn update(&mut self) {}
    fn set_offset(&mut self, offset: usize);
}

pub fn ingest_stream<R: Read, T: Parseable>(stream: R, pvm: &mut PVM) {
    let mut pre_vec: Vec<(usize, String)> = Vec::with_capacity(BATCH_SIZE);
    let mut post_vec: Vec<(usize, Option<T>)> = Vec::with_capacity(BATCH_SIZE);
    let mut lines = BufReader::new(stream).lines().enumerate();

    T::init(pvm);

    loop {
        pre_vec.clear();
        while pre_vec.len() < BATCH_SIZE {
            let (n, mut l) = match lines.next() {
                Some((n, l)) => match l {
                    Ok(l) => (n, l),
                    Err(perr) => {
                        eprintln!("Line: {}", n + 1);
                        eprintln!("File Reading error: {}", perr);
                        continue;
                    }
                },
                None => {
                    break;
                }
            };
            if l.is_empty() {
                continue;
            }
            if l == "[" || l == "]" {
                continue;
            }
            if l.starts_with(", ") {
                l.drain(0..2);
            }
            pre_vec.push((n, l));
        }

        pre_vec
            .par_iter()
            .map(|(n, s)| match serde_json::from_slice::<T>(s.as_bytes()) {
                Ok(mut evt) => {
                    evt.set_offset(*n);
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

        for (n, tr) in post_vec.drain(..) {
            if let Some(mut tr) = tr {
                tr.update();
                if let Err(e) = tr.parse(pvm) {
                    eprintln!("Line: {}", n + 1);
                    eprintln!("PVM Parsing error: {}", e);
                    eprintln!("{}", tr);
                }
            }
        }
        if pre_vec.len() < BATCH_SIZE {
            break;
        }
    }
    println!("Missing Events:");
    for evt in pvm.unparsed_events.drain() {
        println!("{}", evt);
    }
}
