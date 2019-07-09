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

#[derive(Debug)]
pub struct Config {
    pub cfg_mode: CfgMode,
    pub suppress_default_views: bool,
    pub plugin_dir: Option<String>,
    pub cfg_detail: Option<AdvancedConfig>,
}
