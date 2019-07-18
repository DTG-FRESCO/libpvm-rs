pub extern crate pvm_views as views;

use std::{collections::hash_map::DefaultHasher, hash::Hasher};

use views::{data::version as data_version, version as views_version, ViewCoordinator};

mod built_info {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

const fn version() -> &'static str {
    built_info::PKG_VERSION
}

pub fn plugin_version() -> u64 {
    let mut h = DefaultHasher::new();
    h.write(data_version().as_bytes());
    h.write(views_version().as_bytes());
    h.write(version().as_bytes());
    h.finish()
}

pub trait Plugin {
    fn init() -> Self
    where
        Self: Sized;
    fn build_version(&self) -> u64 {
        plugin_version()
    }
    fn view_ops(&self, vc: &mut ViewCoordinator);
}

pub type PluginInit = unsafe extern "C" fn() -> *mut dyn Plugin;

#[macro_export]
macro_rules! define_plugin {
    ($t:ty) => {
        #[no_mangle]
        pub unsafe extern "C" fn _pvm_plugin_init() -> *mut dyn $crate::Plugin {
            use $crate::Plugin;
            Box::into_raw(Box::new(<$t>::init()))
        }
    };
    (views => [$($v:ty),+ $(,)*]) => {
        struct MyPlugin;

        impl $crate::Plugin for MyPlugin {
            fn init() -> Self {
                MyPlugin
            }

            fn view_ops(&self, vc: &mut $crate::views::ViewCoordinator) {
                $(vc.register_view_type::<$v>().unwrap();)*
            }
        }

        define_plugin!(MyPlugin);
    };
}
