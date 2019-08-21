extern crate pvm_cfg as cfg;
extern crate pvm_data as data;

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{mpsc, Arc, Mutex},
    thread::{spawn, JoinHandle},
};

use data::{
    node_types::{ConcreteType, Node},
    rel_types::Rel,
};

use cfg::Config;

#[derive(Clone, Debug)]
pub enum DBTr {
    CreateNode(Node),
    CreateRel(Rel),
    UpdateNode(Node),
    UpdateRel(Rel),
    NewNodeType(&'static ConcreteType),
}

#[derive(Debug)]
pub struct ViewInst {
    pub id: usize,
    pub vtype: usize,
    pub params: HashMap<String, String>,
    pub handle: JoinHandle<()>,
}

impl ViewInst {
    pub fn id(&self) -> usize {
        self.id
    }
    pub fn vtype(&self) -> usize {
        self.vtype
    }
    pub fn params(&self) -> &HashMap<String, String> {
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
        params: HashMap<String, String>,
        cfg: &Config,
        stream: mpsc::Receiver<Arc<DBTr>>,
    ) -> Result<ViewInst, String>;
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
            thread: spawn(move || {
                for evt in recv {
                    {
                        let v = Arc::new(evt);
                        let mut strs = thread_streams.lock().unwrap();
                        for stream in strs.iter_mut() {
                            stream.send(v.clone()).unwrap();
                        }
                    }
                }
            }),
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

    pub fn create_view_inst(
        &mut self,
        id: usize,
        params: HashMap<String, String>,
        cfg: &Config,
    ) -> Result<usize, String> {
        let iid = self.viid_gen;
        self.viid_gen += 1;
        let (w, r) = mpsc::sync_channel(1000);
        let view = self.views[&id].create(iid, params, cfg, r)?;
        self.insts.push(view);
        self.streams.lock().unwrap().push(w);
        Ok(iid)
    }

    pub fn shutdown(self) {
        self.thread.join().unwrap();
        self.streams.lock().unwrap().clear();
        for view in self.insts {
            view.join();
        }
    }
}
