mod db;
mod parse;
pub mod persist;
pub mod pvm;

use std::io::{BufRead, BufReader, Read};

use rayon::prelude::*;
use serde_json;

use self::pvm::PVM;

use trace::TraceEvent;

const BATCH_SIZE: usize = 0x80_000;

pub fn ingest_stream<R>(stream: R, pvm: &mut PVM)
where
    R: Read,
{
    let mut pre_vec: Vec<(usize, String)> = Vec::with_capacity(BATCH_SIZE);
    let mut post_vec: Vec<(usize, Option<TraceEvent>)> = Vec::with_capacity(BATCH_SIZE);
    let mut lines = BufReader::new(stream).lines().enumerate();

    loop {
        pre_vec.clear();
        while pre_vec.len() < BATCH_SIZE {
            let (n, mut l) = match lines.next() {
                Some((n, l)) => match l {
                    Ok(l) => (n, l),
                    Err(perr) => {
                        eprintln!("Line: {}", n+1);
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
            .map(|(n, s)| match serde_json::from_slice(s.as_bytes()) {
                Ok(evt) => (*n, Some(evt)),
                Err(perr) => {
                    eprintln!("Line: {}", n+1);
                    eprintln!("JSON Parsing error: {}", perr);
                    eprintln!("{}", s);
                    (*n, None)
                }
            })
            .collect_into(&mut post_vec);

        for (n, tr) in post_vec.drain(..) {
            if let Some(tr) = tr {
                if let Err(e) = parse::parse_trace(&tr, pvm){
                    eprintln!("Line: {}", n+1);
                    eprintln!("PVM Parsing error: {}", e);
                    eprintln!("{:?}", tr);
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
