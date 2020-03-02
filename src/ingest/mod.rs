//! Various elements defining the ingestion process

use std::{
    fmt::Display,
    io::{BufRead, BufReader, Read},
};

use self::pvm::{PVMError, PVM};

use rayon::prelude::*;
use serde::de::DeserializeOwned;
use serde_json;

mod db;
pub mod pvm;

const BATCH_SIZE: usize = 0x10_000;

/// Defines a type that libpvm can ingest into the PVM model
///
/// Any trace format that libpvm is going to parse must implement this trait and allow
/// for deserialisation from source data via serde::Deserialize
pub trait Mapped: DeserializeOwned + Display + Send + Sized {
    /// Initialize the PVM object for the trace format.
    ///
    /// This method must be called at least once by the ingesting code before any further calls to
    /// 'process' for this trace format are made. Usually the function is called a single time at the
    /// beginning of parsing, followed by repeated deserialisation and calls to 'process' for
    /// subsequent records. This function is usually used to setup any ConcreteTypes that the
    /// format requires.
    ///
    /// ### Example
    /// ```
    ///     
    /// ```
    fn init(pvm: &mut PVM);

    /// Apply the record to the model via mapping
    ///
    /// Implements the PVM mapping for records of the given type, should evaluate the record and
    /// call functions on the PVM object as appropriate.
    fn process(&self, pvm: &mut PVM) -> Result<(), PVMError>;

    /// Applies corrections needed after deserialisation but before processing
    ///
    /// Will be called at least once for each record after it has been deserialised but before it
    /// has it's parse function called. Used to apply fixes and correction to the loaded format.
    fn update(&mut self) {}

    /// Provision an offset
    ///
    /// This may be called by the ingesting code, if so the code will supply an offset value in
    /// it's data stream that identifies where this record starts. This is allowed to vary for
    /// different data sources, but should generally be something that could sensibly be added to
    /// the context for the record.
    fn set_offset(&mut self, offset: usize);
}

pub fn ingest_stream<R: Read, T: Mapped>(stream: R, pvm: &mut PVM) {
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
            .collect_into_vec(&mut post_vec);
        for (n, tr) in post_vec.drain(..) {
            if let Some(tr) = tr {
                if let Err(e) = tr.process(pvm) {
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
