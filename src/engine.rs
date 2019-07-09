use std::{borrow::Cow, ffi::OsStr, path::Path, sync::mpsc};

use crate::{
    cfg::Config,
    ingest::{ingest_stream, pvm::PVM, Parseable},
    iostream::IOStream,
    neo4j_glue::Neo4JView,
    plugins::{Plugin, PluginInit},
    //    query::low::count_processes,
    trace::cadets::TraceEvent,
    view::{View, ViewCoordinator, ViewInst, ViewParams},
};

use libloading::{Library, Symbol};
use maplit::hashmap;
//use neo4j::Neo4jDB;

type EngineResult<T> = Result<T, Cow<'static, str>>;

pub struct PluginManager {
    plugins: Vec<(Box<dyn Plugin>, Library)>,
}

impl PluginManager {
    fn new() -> Self {
        PluginManager {
            plugins: Vec::new(),
        }
    }

    fn load(&mut self, path: &Path) -> EngineResult<()> {
        let lib = Library::new(path).map_err(|e| Cow::from(e.to_string()))?;
        unsafe {
            let init: Symbol<PluginInit> = lib
                .get(b"_pvm_plugin_init")
                .map_err(|e| Cow::from(e.to_string()))?;
            let plugin = Box::from_raw(init());
            self.plugins.push((plugin, lib));
        }
        Ok(())
    }

    fn load_all(&mut self, path: &Path) -> EngineResult<()> {
        let dylib_ext = Some(OsStr::new("so"));

        for entry in path.read_dir().map_err(|e| Cow::from(e.to_string()))? {
            let entry = entry.map_err(|e| Cow::from(e.to_string()))?;

            if entry.path().extension() == dylib_ext {
                self.load(&entry.path())?;
            }
        }
        Ok(())
    }

    fn init_view_coordinator(&self, vc: &mut ViewCoordinator) {
        for (p, _) in &self.plugins {
            p.view_ops(vc);
        }
    }
}

pub struct Pipeline {
    pvm: PVM,
    view_ctrl: ViewCoordinator,
}

pub struct Engine {
    cfg: Config,
    plugins: PluginManager,
    pipeline: Option<Pipeline>,
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.shutdown_pipeline().ok();
    }
}

impl Engine {
    pub fn new(cfg: Config) -> Engine {
        let mut plugins = PluginManager::new();
        if let Some(plugin_dir) = &cfg.plugin_dir {
            plugins.load_all(Path::new(plugin_dir)).unwrap();
        }
        Engine {
            cfg,
            plugins,
            pipeline: None,
        }
    }

    pub fn init_pipeline(&mut self) -> EngineResult<()> {
        if self.pipeline.is_some() {
            return Err("Pipeline already running".into());
        }
        let (send, recv) = mpsc::sync_channel(100_000);
        let mut view_ctrl = ViewCoordinator::new(recv);
        let neo4j_view_id = view_ctrl.register_view_type::<Neo4JView>();
        if !self.cfg.suppress_default_views {
            view_ctrl.create_view_inst(neo4j_view_id, hashmap!());
        }
        self.plugins.init_view_coordinator(&mut view_ctrl);
        self.pipeline = Some(Pipeline {
            pvm: PVM::new(send),
            view_ctrl,
        });
        Ok(())
    }

    pub fn shutdown_pipeline(&mut self) -> EngineResult<()> {
        if let Some(pipeline) = self.pipeline.take() {
            pipeline.pvm.shutdown();
            pipeline.view_ctrl.shutdown();
            Ok(())
        } else {
            Err("Pipeline not running".into())
        }
    }

    pub fn print_cfg(&self) {
        println!("libPVM Config: {:?}", self.cfg);
    }

    pub fn list_view_types(&self) -> EngineResult<Vec<&dyn View>> {
        if let Some(pipeline) = &self.pipeline {
            Ok(pipeline.view_ctrl.list_view_types())
        } else {
            Err("Pipeline not running".into())
        }
    }

    pub fn register_view_type<T: View + Sized + 'static>(&mut self) -> EngineResult<usize> {
        if let Some(ref mut pipeline) = self.pipeline {
            Ok(pipeline.view_ctrl.register_view_type::<T>())
        } else {
            Err("Pipeline not running".into())
        }
    }

    pub fn create_view_by_id(&mut self, view_id: usize, params: ViewParams) -> EngineResult<usize> {
        if let Some(ref mut pipeline) = self.pipeline {
            Ok(pipeline.view_ctrl.create_view_inst(view_id, params))
        } else {
            Err("Pipeline not running".into())
        }
    }

    pub fn list_running_views(&self) -> EngineResult<Vec<&ViewInst>> {
        if let Some(ref pipeline) = self.pipeline {
            Ok(pipeline.view_ctrl.list_view_insts())
        } else {
            Err("Pipeline not running".into())
        }
    }

    pub fn ingest_stream(&mut self, stream: IOStream) -> EngineResult<()> {
        if let Some(ref mut pipeline) = self.pipeline {
            ingest_stream::<_, TraceEvent>(stream, &mut pipeline.pvm);
            Ok(())
        } else {
            Err("Pipeline not running".into())
        }
    }

    pub fn init_record<T: Parseable>(&mut self) -> EngineResult<()> {
        if let Some(ref mut pipeline) = self.pipeline {
            T::init(&mut pipeline.pvm);
            Ok(())
        } else {
            Err("Pipeline not running".into())
        }
    }

    pub fn ingest_record<T: Parseable>(&mut self, rec: &mut T) -> EngineResult<()> {
        if let Some(ref mut pipeline) = self.pipeline {
            rec.parse(&mut pipeline.pvm)
                .map_err(|e| e.to_string().into())
        } else {
            Err("Pipeline not running".into())
        }
    }

    pub fn count_processes(&self) -> i64 {
        /*let mut db = Neo4jDB::connect(
            &self.cfg.db_server,
            &self.cfg.db_user,
            &self.cfg.db_password,
        )
        .unwrap();
        count_processes(&mut db)*/
        unimplemented!()
    }
}
