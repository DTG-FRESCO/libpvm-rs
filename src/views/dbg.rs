use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Write},
    sync::{mpsc::Receiver, Arc},
    thread,
};

use crate::{
    cfg,
    view::{DBTr, View, ViewInst, ViewParams, ViewParamsExt},
};

use maplit::hashmap;

#[derive(Debug)]
pub struct DBGView {
    id: usize,
}

impl View for DBGView {
    fn new(id: usize) -> DBGView {
        DBGView { id }
    }
    fn id(&self) -> usize {
        self.id
    }
    fn name(&self) -> &'static str {
        "DBGView"
    }
    fn desc(&self) -> &'static str {
        "View presenting debug output."
    }
    fn params(&self) -> HashMap<&'static str, &'static str> {
        hashmap!("output" => "Output file location")
    }
    fn create(
        &self,
        id: usize,
        params: ViewParams,
        _cfg: &cfg::Config,
        stream: Receiver<Arc<DBTr>>,
    ) -> ViewInst {
        let path = params.get_or_def("output", "./dbg.trace");
        let mut out = BufWriter::new(File::create(path).unwrap());
        let thr = thread::Builder::new()
            .name("DBGView".to_string())
            .spawn(move || {
                for tr in stream {
                    writeln!(out, "{:?}", tr).unwrap();
                }
            })
            .unwrap();
        ViewInst {
            id,
            vtype: self.id,
            params,
            handle: thr,
        }
    }
}
