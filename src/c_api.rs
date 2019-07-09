#![allow(unused_attributes)]

use std::{
    any::Any,
    collections::HashMap,
    ffi::CStr,
    mem::size_of,
    os::unix::io::{FromRawFd, RawFd},
    ptr, slice,
};

use crate::{
    cfg::{self, AdvancedConfig, CfgMode},
    engine,
    iostream::IOStream,
};

use libc::{c_char, malloc};

#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub enum OpusErr {
    EUNKNOWN = 1,
    EAMBIGUOUSVIEWNAME = 2,
    ENOVIEWWITHNAME = 3,
    EINVALIDARG = 4,
}

fn ret(err: OpusErr) -> isize {
    -(err as isize)
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
    db_server: *mut c_char,
    db_user: *mut c_char,
    db_password: *mut c_char,
    suppress_default_views: bool,
    cfg_detail: *const AdvancedConfig,
}

pub struct OpusHdl(engine::Engine);

fn keyval_arr_to_hashmap(ptr: *const KeyVal, n: usize) -> HashMap<String, Box<dyn Any>> {
    let mut ret = HashMap::with_capacity(n);
    if !ptr.is_null() {
        let s = unsafe { slice::from_raw_parts(ptr, n) };
        for kv in s {
            ret.insert(
                string_from_c_char(kv.key).unwrap(),
                Box::new(string_from_c_char(kv.val).unwrap()) as Box<dyn Any>,
            );
        }
    }
    ret
}

fn view_params_to_keyval_arr(h: &HashMap<&'static str, &'static str>) -> (*mut KeyVal, usize) {
    iter_to_keyval_arr(h.iter().map(|(k, v)| (*k, *v)), h.len())
}

fn view_inst_params_to_keyval_arr(h: &HashMap<String, Box<dyn Any>>) -> (*mut KeyVal, usize) {
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
    unsafe { CStr::from_ptr(str_p) }
        .to_str()
        .ok()
        .map(|s| s.to_string())
}

#[no_mangle]
pub unsafe extern "C" fn opus_init(cfg: Config) -> *mut OpusHdl {
    let r_cfg = cfg::Config {
        cfg_mode: cfg.cfg_mode,
        db_server: string_from_c_char(cfg.db_server)
            .unwrap_or_else(|| "localhost:7687".to_string()),
        db_user: string_from_c_char(cfg.db_user).unwrap_or_else(|| "neo4j".to_string()),
        db_password: string_from_c_char(cfg.db_password).unwrap_or_else(|| "opus".to_string()),
        suppress_default_views: cfg.suppress_default_views,
        cfg_detail: if cfg.cfg_detail.is_null() {
            Option::None
        } else {
            Option::Some(ptr::read(cfg.cfg_detail))
        },
    };
    let e = engine::Engine::new(r_cfg);
    let hdl = Box::new(OpusHdl(e));
    Box::into_raw(hdl)
}

#[no_mangle]
pub unsafe extern "C" fn opus_start_pipeline(hdl: *mut OpusHdl) -> isize {
    let engine = &mut (*hdl).0;
    match engine.init_pipeline() {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Error: {}", e);
            ret(OpusErr::EUNKNOWN)
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn opus_shutdown_pipeline(hdl: *mut OpusHdl) -> isize {
    let engine = &mut (*hdl).0;
    match engine.shutdown_pipeline() {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Error: {}", e);
            ret(OpusErr::EUNKNOWN)
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn opus_print_cfg(hdl: *const OpusHdl) {
    let engine = &(*hdl).0;
    engine.print_cfg();
}

#[no_mangle]
pub unsafe extern "C" fn opus_list_view_types(hdl: *const OpusHdl, out: *mut *mut View) -> isize {
    let engine = &(*hdl).0;
    let views = match engine.list_view_types() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {}", e);
            return ret(OpusErr::EUNKNOWN);
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
pub unsafe extern "C" fn opus_create_view_by_id(
    hdl: *mut OpusHdl,
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
            ret(OpusErr::EUNKNOWN)
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn opus_create_view_by_name(
    hdl: *mut OpusHdl,
    name: *const c_char,
    params: *const KeyVal,
    n_params: usize,
) -> isize {
    let engine = &mut (*hdl).0;
    let rparams = keyval_arr_to_hashmap(params, n_params);
    let name = match string_from_c_char(name) {
        Some(s) => s,
        None => {
            return ret(OpusErr::EINVALIDARG);
        }
    };
    let views_with_name = match engine.list_view_types() {
        Ok(vtypes) => vtypes
            .into_iter()
            .filter(|v| v.name() == name)
            .map(|v| v.id())
            .collect::<Vec<usize>>(),
        Err(e) => {
            eprintln!("Error: {}", e);
            return ret(OpusErr::EUNKNOWN);
        }
    };

    if views_with_name.is_empty() {
        ret(OpusErr::ENOVIEWWITHNAME)
    } else if views_with_name.len() > 1 {
        ret(OpusErr::EAMBIGUOUSVIEWNAME)
    } else {
        match engine.create_view_by_id(views_with_name[0], rparams) {
            Ok(vid) => vid as isize,
            Err(e) => {
                eprintln!("Error: {}", e);
                ret(OpusErr::EUNKNOWN)
            }
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn opus_list_view_inst(
    hdl: *const OpusHdl,
    out: *mut *mut ViewInst,
) -> isize {
    let engine = &(*hdl).0;
    let views = match engine.list_running_views() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: {}", e);
            return ret(OpusErr::EUNKNOWN);
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
pub unsafe extern "C" fn opus_ingest_fd(hdl: *mut OpusHdl, fd: i32) -> isize {
    let engine = &mut (*hdl).0;
    let stream = IOStream::from_raw_fd(fd as RawFd);
    match timeit!(engine.ingest_stream(stream)) {
        Ok(_) => 0,
        Err(e) => {
            eprintln!("Error: {}", e);
            ret(OpusErr::EUNKNOWN)
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn opus_cleanup(hdl: *mut OpusHdl) {
    drop(Box::from_raw(hdl));
    println!("Cleaning up..");
}

#[no_mangle]
pub unsafe extern "C" fn opus_count_processes(hdl: *const OpusHdl) -> i64 {
    let engine = &(*hdl).0;
    engine.count_processes()
}
