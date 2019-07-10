pub extern crate pvm_views as views;

use views::ViewCoordinator;

pub trait Plugin {
    fn init() -> Self
    where
        Self: Sized;
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
