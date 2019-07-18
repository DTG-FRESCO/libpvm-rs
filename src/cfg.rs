#[repr(C)]
#[derive(Debug, PartialEq)]
pub enum CfgMode {
    Auto,
    Advanced,
}

#[repr(C)]
#[derive(Debug)]
pub struct AdvancedConfig {
    consumer_threads: usize,
    persistence_threads: usize,
}

impl Default for AdvancedConfig {
    fn default() -> Self {
        AdvancedConfig {
            consumer_threads: 8,
            persistence_threads: 1,
        }
    }
}

#[derive(Debug)]
pub struct Config {
    pub(crate) cfg_mode: CfgMode,
    pub(crate) plugin_dir: Option<String>,
    pub(crate) cfg_detail: Option<AdvancedConfig>,
}

impl Config {
    pub fn build() -> ConfigBuilder {
        ConfigBuilder::default()
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            cfg_mode: CfgMode::Auto,
            plugin_dir: None,
            cfg_detail: None,
        }
    }
}

#[derive(Debug, Default)]
pub struct ConfigBuilder(Config);

impl ConfigBuilder {
    pub fn finish(self) -> Config {
        self.0
    }

    pub fn plugin_dir<S: ToString>(mut self, dir: S) -> Self {
        self.0.plugin_dir = Some(dir.to_string());
        self
    }

    pub fn advanced(self) -> AdvancedConfigBuilder {
        AdvancedConfigBuilder::new(self)
    }
}

#[derive(Debug)]
pub struct AdvancedConfigBuilder(Config);

impl AdvancedConfigBuilder {
    fn new(val: ConfigBuilder) -> Self {
        let mut cfg = val.0;
        cfg.cfg_mode = CfgMode::Advanced;
        cfg.cfg_detail = Some(AdvancedConfig::default());
        AdvancedConfigBuilder(cfg)
    }

    pub fn finish(self) -> Config {
        self.0
    }

    pub fn plugin_dir<S: ToString>(mut self, dir: S) -> Self {
        self.0.plugin_dir = Some(dir.to_string());
        self
    }

    pub fn consumer_threads(mut self, threads: usize) -> Self {
        self.0.cfg_detail.as_mut().unwrap().consumer_threads = threads;
        self
    }

    pub fn persistence_threads(mut self, threads: usize) -> Self {
        self.0.cfg_detail.as_mut().unwrap().persistence_threads = threads;
        self
    }
}
