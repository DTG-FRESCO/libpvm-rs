extern crate pvm_cfg as cfg;
extern crate pvm_data as data;

use std::{
    any::Any,
    collections::HashMap,
    fmt::Debug,
    sync::{mpsc, Arc, Mutex},
    thread::{Builder as ThreadBuilder, JoinHandle},
};

use crate::{
    cfg::Config,
    data::{node_types::Node, rel_types::Rel},
};

#[derive(Clone, Debug)]
pub enum DBTr {
    CreateNode(Node),
    CreateRel(Rel),
    UpdateNode(Node),
    UpdateRel(Rel),
}

pub type ViewParams = HashMap<String, Box<Any>>;

pub trait ViewParamsExt {
    fn get_or_def<'a>(&'a self, key: &str, def: &'a str) -> &'a str;
}

impl ViewParamsExt for ViewParams {
    fn get_or_def<'a>(&'a self, key: &str, def: &'a str) -> &'a str {
        self.get(key)
            .and_then(|val| val.downcast_ref::<String>())
            .map(|val| val as &str)
            .unwrap_or(def)
    }
}

#[derive(Debug)]
pub struct ViewInst {
    pub id: usize,
    pub vtype: usize,
    pub params: ViewParams,
    pub handle: JoinHandle<()>,
}

impl ViewInst {
    pub fn id(&self) -> usize {
        self.id
    }
    pub fn vtype(&self) -> usize {
        self.vtype
    }
    pub fn params(&self) -> &ViewParams {
        &self.params
    }
    fn join(self) {
        self.handle.join().unwrap()
    }
}

pub trait View: Debug {
    fn new(id: usize) -> Self
    where
        Self: Sized;
    fn id(&self) -> usize;
    fn name(&self) -> &'static str;
    fn desc(&self) -> &'static str;
    fn params(&self) -> HashMap<&'static str, &'static str>;
    fn create(
        &self,
        id: usize,
        params: ViewParams,
        cfg: &Config,
        stream: mpsc::Receiver<Arc<DBTr>>,
    ) -> ViewInst;
}

#[derive(Debug)]
pub struct ViewCoordinator {
    views: HashMap<usize, Box<View>>,
    insts: Vec<ViewInst>,
    streams: Arc<Mutex<Vec<mpsc::SyncSender<Arc<DBTr>>>>>,
    thread: JoinHandle<()>,
    vid_gen: usize,
    viid_gen: usize,
}

impl ViewCoordinator {
    pub fn new(recv: mpsc::Receiver<DBTr>) -> Self {
        let streams: Arc<Mutex<Vec<mpsc::SyncSender<Arc<DBTr>>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let thread_streams = streams.clone();
        ViewCoordinator {
            thread: ThreadBuilder::new()
                .name("ViewCoordinator".to_string())
                .spawn(move || {
                    for evt in recv {
                        {
                            let v = Arc::new(evt);
                            let mut strs = thread_streams.lock().unwrap();
                            for stream in strs.iter_mut() {
                                stream.send(v.clone()).unwrap();
                            }
                            drop(v);
                        }
                    }
                })
                .unwrap(),
            views: HashMap::new(),
            insts: Vec::new(),
            streams,
            vid_gen: 0,
            viid_gen: 0,
        }
    }

    pub fn register_view_type<T: View + 'static>(&mut self) -> usize {
        let id = self.vid_gen;
        self.vid_gen += 1;
        let view = Box::new(T::new(id));
        self.views.insert(id, view);
        id
    }

    pub fn list_view_types(&self) -> Vec<&View> {
        self.views.values().map(|v| v.as_ref()).collect()
    }

    pub fn list_view_insts(&self) -> Vec<&ViewInst> {
        self.insts.iter().collect()
    }

    pub fn create_view_inst(&mut self, id: usize, params: ViewParams, cfg: &Config) -> usize {
        let iid = self.viid_gen;
        self.viid_gen += 1;
        let (w, r) = mpsc::sync_channel(1000);
        let view = self.views[&id].create(iid, params, cfg, r);
        self.insts.push(view);
        self.streams.lock().unwrap().push(w);
        iid
    }

    pub fn shutdown(self) {
        self.thread.join().unwrap();
        self.streams.lock().unwrap().clear();
        for view in self.insts {
            view.join();
        }
    }
}
