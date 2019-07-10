#![allow(unused_attributes)]

use std::{
    collections::HashMap,
    ffi::CStr,
    mem::size_of,
    os::{
        raw::c_char,
        unix::io::{FromRawFd, RawFd},
    },
    ptr, slice,
};

use crate::{
    cfg::{self, AdvancedConfig, CfgMode},
    engine::{Engine, EngineError},
    iostream::IOStream,
    view::{ViewError, ViewParams, ViewParamsExt},
};

use libc::malloc;

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub enum PVMErr {
    EUNKNOWN = 1,
    EAMBIGUOUSVIEWNAME = 2,
    ENOVIEWWITHNAME = 3,
    ENOVIEWWITHID = 5,
    EINVALIDARG = 4,
    EPIPELINENOTRUNNING = 6,
    EPIPELINERUNNING = 7,
    EPLUGINLOAD = 8,
    ETHREADSTARTUP = 9,
}

impl From<EngineError> for PVMErr {
    fn from(val: EngineError) -> Self {
        match val {
            EngineError::PipelineRunning => PVMErr::EPIPELINERUNNING,
            EngineError::PipelineNotRunning => PVMErr::EPIPELINENOTRUNNING,
            EngineError::PluginError(_) => PVMErr::EPLUGINLOAD,
            EngineError::ProcessingError(_) => PVMErr::EUNKNOWN,
            EngineError::ViewError(e) => match e {
                ViewError::ThreadingErr(_) => PVMErr::ETHREADSTARTUP,
                ViewError::DuplicateViewName(_) => PVMErr::EAMBIGUOUSVIEWNAME,
                ViewError::MissingViewID(_) => PVMErr::ENOVIEWWITHID,
                ViewError::MissingViewName(_) => PVMErr::ENOVIEWWITHNAME,
            },
        }
    }
}

fn ret<T: Into<PVMErr>>(err: T) -> isize {
    -(err.into() as isize)
}

#[repr(C)]
#[derive(Debug)]
pub struct KeyVal {
    key: *mut c_char,
    val: *mut c_char,
}

#[repr(C)]
#[derive(Debug)]
pub struct View {
    id: usize,
    name: *mut c_char,
    desc: *mut c_char,
    num_parameters: usize,
    parameters: *mut KeyVal,
}

#[repr(C)]
#[derive(Debug)]
pub struct ViewInst {
    id: usize,
    vtype: usize,
    num_parameters: usize,
    parameters: *mut KeyVal,
}

#[repr(C)]
pub struct Config {
    cfg_mode: CfgMode,
    plugin_dir: *mut c_char,
    cfg_detail: *const AdvancedConfig,
}

pub struct PVMHdl(Engine);

fn keyval_arr_to_hashmap(ptr: *const KeyVal, n: usize) -> ViewParams {
    let mut ret = ViewParams::with_capacity(n);
    if !ptr.is_null() {
        let s = unsafe { slice::from_raw_parts(ptr, n) };
        for kv in s {
            ret.insert_param(
                string_from_c_char(kv.key).unwrap(),
                string_from_c_char(kv.val).unwrap(),
            );
        }
    }
    ret
}

fn view_params_to_keyval_arr(h: &HashMap<&'static str, &'static str>) -> (*mut KeyVal, usize) {
    iter_to_keyval_arr(h.iter().map(|(k, v)| (*k, *v)), h.len())
}

fn view_inst_params_to_keyval_arr(h: &ViewParams) -> (*mut KeyVal, usize) {
    iter_to_keyval_arr(
        h.iter().map(|(k, v)| match v.downcast_ref::<String>() {
            Some(r) => (k as &str, r as &str),
            None => (k as &str, "<non-string>"),
        }),
        h.len(),
    )
}

fn iter_to_keyval_arr<'a, 'b, T: IntoIterator<Item = (&'a str, &'b str)>>(
    h: T,
    len: usize,
) -> (*mut KeyVal, usize) {
    let data = unsafe { malloc(len * size_of::<KeyVal>()) as *mut KeyVal };
    let s = unsafe { slice::from_raw_parts_mut(data, len) };
    for ((k, v), kv) in h.into_iter().zip(s) {
        kv.key = string_to_c_char(k);
        kv.val = string_to_c_char(v);
    }
    (data, len)
}

fn string_to_c_char(val: &str) -> *mut c_char {
    if val.contains('\0') {
        panic!("Trying to convert a string containing nulls to a C-string");
    }
    unsafe {
        let data = malloc((val.len() + 1) * size_of::<c_char>()) as *mut c_char;
        ptr::copy(val.as_ptr() as *const c_char, data, val.len());
        *data.offset(val.len() as isize) = 0x00 as c_char;
        data
    }
}

fn string_from_c_char(str_p: *const c_char) -> Option<String> {
    if str_p.is_null() {
        return None;
    }
    Some(
        unsafe { CStr::from_ptr(str_p) }
            .to_string_lossy()
            .into_owned(),
    )
}

#[no_mangle]
pub unsafe extern "C" fn pvm_init(cfg: Config) -> *mut PVMHdl {
    let r_cfg = cfg::Config {
        cfg_mode: cfg.cfg_mode,
        plugin_dir: string_from_c_char(cfg.plugin_dir),
        cfg_detail: if cfg.cfg_detail.is_null() {
            Option::None
        } else {
            Option::Some(ptr::read(cfg.cfg_detail))
        },
    };
    let e = match Engine::new(r_cfg) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {}", e);
            return ptr::null_mut();
        }
    };
    let hdl = Box::new(PVMHdl(e));
    Box::into_raw(hdl)
}

#[no_mangle]
pub unsafe extern "C" fn pvm_start_pipeline(hdl: *mut PVMHdl) -> isize {
    let engine = &mut (*hdl).0;
    match engine.init_pipeline() {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Error: {}", e);
            ret(e)
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn pvm_shutdown_pipeline(hdl: *mut PVMHdl) -> isize {
    let engine = &mut (*hdl).0;
    match engine.shutdown_pipeline() {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Error: {}", e);
            ret(e)
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn pvm_init_persistance(
    hdl: *mut PVMHdl,
    addr: *const c_char,
    user: *const c_char,
    pass: *const c_char,
) -> isize {
    let engine = &mut (*hdl).0;
    match engine.init_persistance(
        string_from_c_char(addr),
        string_from_c_char(user),
        string_from_c_char(pass),
    ) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Error: {}", e);
            ret(e)
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn pvm_print_cfg(hdl: *const PVMHdl) {
    let engine = &(*hdl).0;
    engine.print_cfg();
}

#[no_mangle]
pub unsafe extern "C" fn pvm_list_view_types(hdl: *const PVMHdl, out: *mut *mut View) -> isize {
    let engine = &(*hdl).0;
    let views = match engine.list_view_types() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {}", e);
            return ret(e);
        }
    };
    let len = views.len();
    *out = malloc(len * size_of::<View>()) as *mut View;
    let s = slice::from_raw_parts_mut(*out, len);
    for (view, c_view) in views.into_iter().zip(s) {
        c_view.id = view.id();
        c_view.name = string_to_c_char(view.name());
        c_view.desc = string_to_c_char(view.desc());
        let (params, num) = view_params_to_keyval_arr(&view.params());
        c_view.num_parameters = num;
        c_view.parameters = params;
    }
    len as isize
}

#[no_mangle]
pub unsafe extern "C" fn pvm_create_view_by_id(
    hdl: *mut PVMHdl,
    view_id: usize,
    params: *const KeyVal,
    n_params: usize,
) -> isize {
    let engine = &mut (*hdl).0;
    let rparams = keyval_arr_to_hashmap(params, n_params);
    match engine.create_view_by_id(view_id, rparams) {
        Ok(vid) => vid as isize,
        Err(e) => {
            eprintln!("Error: {}", e);
            ret(e)
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn pvm_create_view_by_name(
    hdl: *mut PVMHdl,
    name: *const c_char,
    params: *const KeyVal,
    n_params: usize,
) -> isize {
    let engine = &mut (*hdl).0;
    let rparams = keyval_arr_to_hashmap(params, n_params);
    let name = match string_from_c_char(name) {
        Some(s) => s,
        None => {
            return ret(PVMErr::EINVALIDARG);
        }
    };
    match engine.create_view_by_name(&name, rparams) {
        Ok(vid) => vid as isize,
        Err(e) => {
            eprintln!("Error: {}", e);
            ret(e)
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn pvm_list_view_inst(hdl: *const PVMHdl, out: *mut *mut ViewInst) -> isize {
    let engine = &(*hdl).0;
    let views = match engine.list_running_views() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {}", e);
            return ret(e);
        }
    };
    let len = views.len();
    *out = malloc(len * size_of::<ViewInst>()) as *mut ViewInst;
    let s = slice::from_raw_parts_mut(*out, len);
    for (view, c_view) in views.into_iter().zip(s) {
        c_view.id = view.id();
        c_view.vtype = view.vtype();
        let (params, num) = view_inst_params_to_keyval_arr(view.params());
        c_view.num_parameters = num;
        c_view.parameters = params;
    }
    len as isize
}

#[no_mangle]
pub unsafe extern "C" fn pvm_ingest_fd(hdl: *mut PVMHdl, fd: i32) -> isize {
    let engine = &mut (*hdl).0;
    let stream = IOStream::from_raw_fd(fd as RawFd);
    match timeit!(engine.ingest_stream(stream)) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Error: {}", e);
            ret(e)
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn pvm_cleanup(hdl: *mut PVMHdl) {
    drop(Box::from_raw(hdl));
    println!("Cleaning up..");
}

#[no_mangle]
pub unsafe extern "C" fn pvm_count_processes(hdl: *const PVMHdl) -> i64 {
    let engine = &(*hdl).0;
    engine.count_processes()
}
