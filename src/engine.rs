use std::{ffi::OsStr, io::Read, path::Path, sync::mpsc};

use crate::{
    cfg::Config,
    ingest::{
        ingest_stream,
        pvm::{PVMError, PVM},
        Mapped,
    },
    iostream::IOStream,
    neo4j_glue::Neo4JView,
    plugins::{plugin_version, Plugin, PluginInit},
    //    query::low::count_processes,
    trace::cadets::TraceEvent,
    view::{View, ViewCoordinator, ViewError, ViewInst, ViewParams, ViewParamsExt},
};

use libloading::{Library, Symbol};
//use neo4j::Neo4jDB;
use quick_error::quick_error;

quick_error! {
    #[derive(Debug)]
    pub enum EngineError {
        PipelineRunning {
            description("Pipeline already running")
        }
        PipelineNotRunning {
            description("Pipeline not yet running")
        }
        PluginVersionMismatch(path: String) {
            description("Attempted to load a plugin with a mismatched plugin API version")
            display("Failed to load plugin {} due to a mismatched plugin API version", path)
        }
        PluginError(err: std::io::Error) {
            source(err)
            from()
            description(err.description())
            display("Plugin error: {}", err)
        }
        ProcessingError(err: PVMError) {
            source(err)
            from()
            description(err.description())
            display("Processing error: {}", err)
        }
        ViewError(err: ViewError) {
            source(err)
            from()
            description(err.description())
            display("View Orchestration error: {}", err)
        }
    }
}

type Result<T> = std::result::Result<T, EngineError>;

pub struct PluginManager {
    plugins: Vec<(Box<dyn Plugin>, Library)>,
}

impl PluginManager {
    fn new() -> Self {
        PluginManager {
            plugins: Vec::new(),
        }
    }

    fn load(&mut self, path: &Path) -> Result<()> {
        let lib = Library::new(path)?;
        unsafe {
            let init: Symbol<PluginInit> = lib.get(b"_pvm_plugin_init")?;
            let plugin = Box::from_raw(init());
            if plugin.build_version() != plugin_version() {
                return Err(EngineError::PluginVersionMismatch(
                    path.to_string_lossy().into_owned(),
                ));
            }
            self.plugins.push((plugin, lib));
        }
        Ok(())
    }

    fn load_all(&mut self, path: &Path) -> Result<()> {
        let dylib_ext = Some(OsStr::new("so"));

        for entry in path.read_dir()? {
            let entry = entry?;

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
    pub fn new(cfg: Config) -> Result<Engine> {
        let mut plugins = PluginManager::new();
        if let Some(plugin_dir) = &cfg.plugin_dir {
            plugins.load_all(Path::new(plugin_dir))?;
        }
        Ok(Engine {
            cfg,
            plugins,
            pipeline: None,
        })
    }

    pub fn init_pipeline(&mut self) -> Result<()> {
        if self.pipeline.is_some() {
            return Err(EngineError::PipelineRunning);
        }
        let (send, recv) = mpsc::sync_channel(100_000);
        let mut view_ctrl = ViewCoordinator::new(recv)?;
        view_ctrl.register_view_type::<Neo4JView>()?;
        self.plugins.init_view_coordinator(&mut view_ctrl);
        self.pipeline = Some(Pipeline {
            pvm: PVM::new(send),
            view_ctrl,
        });
        Ok(())
    }

    pub fn shutdown_pipeline(&mut self) -> Result<()> {
        if let Some(pipeline) = self.pipeline.take() {
            pipeline.pvm.shutdown();
            pipeline.view_ctrl.shutdown();
            Ok(())
        } else {
            Err(EngineError::PipelineNotRunning)
        }
    }

    fn get_pipeline(&self) -> Result<&Pipeline> {
        self.pipeline
            .as_ref()
            .ok_or(EngineError::PipelineNotRunning)
    }

    fn get_pipeline_mut(&mut self) -> Result<&mut Pipeline> {
        self.pipeline
            .as_mut()
            .ok_or(EngineError::PipelineNotRunning)
    }

    pub fn print_cfg(&self) {
        println!("libPVM Config: {:?}", self.cfg);
    }

    pub fn init_persistance(
        &mut self,
        addr: Option<String>,
        user: Option<String>,
        pass: Option<String>,
    ) -> Result<()> {
        let pipeline = self.get_pipeline_mut()?;
        let mut params = ViewParams::new();
        if let Some(addr) = addr {
            params.insert_param("addr", addr);
        }
        if let Some(user) = user {
            params.insert_param("user", user);
        }
        if let Some(pass) = pass {
            params.insert_param("pass", pass);
        }
        pipeline
            .view_ctrl
            .create_view_with_name("Neo4JView", params)?;
        Ok(())
    }

    pub fn list_view_types(&self) -> Result<Vec<&dyn View>> {
        let pipeline = self.get_pipeline()?;
        Ok(pipeline.view_ctrl.list_view_types())
    }

    pub fn register_view_type<T: View + Sized + 'static>(&mut self) -> Result<usize> {
        let pipeline = self.get_pipeline_mut()?;
        Ok(pipeline.view_ctrl.register_view_type::<T>()?)
    }

    pub fn create_view_by_name(&mut self, view_name: &str, params: ViewParams) -> Result<usize> {
        let pipeline = self.get_pipeline_mut()?;
        Ok(pipeline
            .view_ctrl
            .create_view_with_name(view_name, params)?)
    }

    pub fn create_view_by_id(&mut self, view_id: usize, params: ViewParams) -> Result<usize> {
        let pipeline = self.get_pipeline_mut()?;
        Ok(pipeline.view_ctrl.create_view_with_id(view_id, params)?)
    }

    pub fn list_running_views(&self) -> Result<Vec<&ViewInst>> {
        let pipeline = self.get_pipeline()?;
        Ok(pipeline.view_ctrl.list_view_insts())
    }

    pub fn ingest_stream(&mut self, stream: IOStream) -> Result<()> {
        let pipeline = self.get_pipeline_mut()?;
        ingest_stream::<_, TraceEvent>(stream, &mut pipeline.pvm);
        Ok(())
    }

    pub fn ingest_reader<R: Read>(&mut self, reader: R) -> Result<()> {
        let pipeline = self.get_pipeline_mut()?;
        ingest_stream::<_, TraceEvent>(reader, &mut pipeline.pvm);
        Ok(())
    }

    pub fn init_record<T: Mapped>(&mut self) -> Result<()> {
        let pipeline = self.get_pipeline_mut()?;
        T::init(&mut pipeline.pvm);
        Ok(())
    }

    pub fn ingest_record<T: Mapped>(&mut self, rec: &mut T) -> Result<()> {
        let pipeline = self.get_pipeline_mut()?;
        rec.process(&mut pipeline.pvm)?;
        Ok(())
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
