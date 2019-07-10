pub extern crate pvm_data as data;

use std::{
    any::Any,
    collections::HashMap,
    fmt::Debug,
    io,
    sync::{mpsc, Arc, Mutex},
    thread::{Builder as ThreadBuilder, JoinHandle},
};

pub use crate::data::{node_types::Node, rel_types::Rel};

use quick_error::quick_error;

quick_error! {
    #[derive(Debug)]
    pub enum ViewError {
        DuplicateViewName(name: &'static str) {
            description("Attempting to register duplicate view")
            display("View with name {} already exists.", name)
        }
        MissingViewName(name: String){
            description("Missing view with name")
            display("No View type registered under name {}.", name)
        }
        MissingViewID(id: usize){
            description("Missing view with ID")
            display("No View type registered with id {}.", id)
        }
        ThreadingErr(err: io::Error) {
            source(err)
            from()
            description(err.description())
            display("Error spawning thread: {}", err)
        }
    }
}

#[derive(Clone, Debug)]
pub enum DBTr {
    CreateNode(Node),
    CreateRel(Rel),
    UpdateNode(Node),
    UpdateRel(Rel),
}

pub type ViewParams = HashMap<String, Box<dyn Any>>;

pub trait ViewParamsExt {
    fn insert_param<K: ToString, V: Any>(&mut self, key: K, val: V);
    fn get_or_def<'a>(&'a self, key: &str, def: &'a str) -> &'a str;
}

impl ViewParamsExt for ViewParams {
    fn insert_param<K: ToString, V: Any>(&mut self, key: K, val: V) {
        self.insert(key.to_string(), Box::new(val));
    }

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
    fn create(&self, id: usize, params: ViewParams, stream: mpsc::Receiver<Arc<DBTr>>) -> ViewInst;
}

type Result<T> = std::result::Result<T, ViewError>;

#[derive(Debug)]
pub struct ViewCoordinator {
    views: HashMap<usize, Box<dyn View>>,
    view_name_map: HashMap<&'static str, usize>,
    insts: Vec<ViewInst>,
    streams: Arc<Mutex<Vec<mpsc::SyncSender<Arc<DBTr>>>>>,
    thread: JoinHandle<()>,
    vid_gen: usize,
    viid_gen: usize,
}

impl ViewCoordinator {
    pub fn new(recv: mpsc::Receiver<DBTr>) -> Result<Self> {
        let streams: Arc<Mutex<Vec<mpsc::SyncSender<Arc<DBTr>>>>> =
            Arc::new(Mutex::new(Vec::new()));
        let thread_streams = streams.clone();
        Ok(ViewCoordinator {
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
                })?,
            views: HashMap::new(),
            view_name_map: HashMap::new(),
            insts: Vec::new(),
            streams,
            vid_gen: 0,
            viid_gen: 0,
        })
    }

    pub fn register_view_type<T: View + 'static>(&mut self) -> Result<usize> {
        let id = self.vid_gen;
        let view = Box::new(T::new(id));
        if self.view_name_map.contains_key(view.name()) {
            Err(ViewError::DuplicateViewName(view.name()))
        } else {
            self.vid_gen += 1;
            self.view_name_map.insert(view.name(), id);
            self.views.insert(id, view);
            Ok(id)
        }
    }

    pub fn list_view_types(&self) -> Vec<&dyn View> {
        self.views.values().map(|v| v.as_ref()).collect()
    }

    pub fn list_view_insts(&self) -> Vec<&ViewInst> {
        self.insts.iter().collect()
    }

    pub fn create_view_with_id(&mut self, id: usize, params: ViewParams) -> Result<usize> {
        if self.views.contains_key(&id) {
            let iid = self.viid_gen;
            self.viid_gen += 1;
            let (w, r) = mpsc::sync_channel(1000);
            let view = self.views[&id].create(iid, params, r);
            self.insts.push(view);
            self.streams.lock().unwrap().push(w);
            Ok(iid)
        } else {
            Err(ViewError::MissingViewID(id))
        }
    }

    pub fn create_view_with_name(&mut self, name: &str, params: ViewParams) -> Result<usize> {
        if self.view_name_map.contains_key(name) {
            self.create_view_with_id(self.view_name_map[name], params)
        } else {
            Err(ViewError::MissingViewName(name.to_string()))
        }
    }

    pub fn shutdown(self) {
        self.thread.join().unwrap();
        self.streams.lock().unwrap().clear();
        for view in self.insts {
            view.join();
        }
    }
}
