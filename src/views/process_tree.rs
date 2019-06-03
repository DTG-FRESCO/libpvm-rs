use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    sync::{mpsc::Receiver, Arc},
    thread,
};

use crate::{
    cfg,
    data::{
        node_types::{CtxNode, Node, PVMDataType},
        rel_types::Rel,
        HasDst, HasID, HasSrc, ID,
    },
    view::{DBTr, View, ViewInst, ViewParams, ViewParamsExt},
};

use maplit::hashmap;
use serde_json::to_writer;

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum Record<'a> {
    Node {
        id: ID,
        cmd: Option<&'a str>,
        host: Option<i32>,
        trace_idx: Option<&'a str>,
        ts: Option<&'a str>,
    },
    Edge {
        src: ID,
        dst: ID,
    },
    HostVal {
        uuid: &'a str,
        idx: i32,
    },
}

#[derive(Debug)]
pub struct ProcTreeView {
    id: usize,
}

fn neq(a: &Option<&str>, b: &Option<String>) -> bool {
    match a {
        Some(va) => match b {
            Some(vb) => *va != *vb,
            None => true,
        },
        None => b.is_some(),
    }
}

impl View for ProcTreeView {
    fn new(id: usize) -> ProcTreeView {
        ProcTreeView { id }
    }
    fn id(&self) -> usize {
        self.id
    }
    fn name(&self) -> &'static str {
        "ProcTreeView"
    }
    fn desc(&self) -> &'static str {
        "View for storing a process tree."
    }
    fn params(&self) -> HashMap<&'static str, &'static str> {
        hashmap!("output" => "Output file location",
                 "meta_key" => "Metadata key for process name")
    }
    fn create(
        &self,
        id: usize,
        params: ViewParams,
        _cfg: &cfg::Config,
        stream: Receiver<Arc<DBTr>>,
    ) -> ViewInst {
        let path = params.get_or_def("output", "./proc_tree.json");
        let meta_key = params.get_or_def("meta_key", "cmdline").to_string();
        let mut out = File::create(path).unwrap();
        let thr = thread::Builder::new()
            .name("ProcTreeView".to_string())
            .spawn(move || {
                let mut nodes = HashMap::new();
                let mut ctx_store: HashMap<ID, CtxNode> = HashMap::new();
                let mut cur_ctx: Option<CtxNode> = None;
                let mut host_map = HashMap::new();
                let mut host_count = 0;
                for tr in stream {
                    match *tr {
                        DBTr::CreateNode(ref n) | DBTr::UpdateNode(ref n) => match n {
                            Node::Data(n) if *n.pvm_ty() == PVMDataType::Actor => {
                                let id = n.get_db_id();
                                let cmd = n.meta.cur(&meta_key);
                                if !nodes.contains_key(&id) || neq(&cmd, &nodes[&id]) {
                                    if let Some(c) = &cur_ctx {
                                        if c.get_db_id() == n.ctx() {
                                            ctx_store
                                                .insert(c.get_db_id(), cur_ctx.take().unwrap());
                                        }
                                    }
                                    let ctx = ctx_store.get(&n.ctx());
                                    let trace_idx = ctx
                                        .and_then(|c| c.cont.get("trace_offset"))
                                        .map(|v| &v[..]);
                                    let ts = ctx.and_then(|c| c.cont.get("time")).map(|v| &v[..]);
                                    let host = ctx.and_then(|c| c.cont.get("host"));

                                    let host = if let Some(h) = host {
                                        if host_map.contains_key(h) {
                                            Some(host_map[h])
                                        } else {
                                            host_count += 1;
                                            host_map.insert(h.clone(), host_count);
                                            to_writer(
                                                &mut out,
                                                &Record::HostVal {
                                                    uuid: h,
                                                    idx: host_count,
                                                },
                                            )
                                            .unwrap();
                                            writeln!(out).unwrap();
                                            Some(host_count)
                                        }
                                    } else {
                                        None
                                    };

                                    to_writer(
                                        &mut out,
                                        &Record::Node {
                                            id,
                                            cmd,
                                            host,
                                            trace_idx,
                                            ts,
                                        },
                                    )
                                    .unwrap();
                                    writeln!(out).unwrap();
                                    out.flush().unwrap();
                                    nodes.insert(id, cmd.map(|v| v.to_string()));
                                }
                            }
                            Node::Ctx(n) => {
                                cur_ctx = Some(n.clone());
                            }
                            _ => {}
                        },
                        DBTr::CreateRel(ref r) => {
                            if let Rel::Inf(r) = r {
                                let src = r.get_src();
                                let dst = r.get_dst();
                                if nodes.contains_key(&src) && nodes.contains_key(&dst) {
                                    to_writer(&mut out, &Record::Edge { src, dst }).unwrap();
                                    writeln!(out).unwrap();
                                    out.flush().unwrap();
                                }
                            }
                        }
                        _ => {}
                    }
                }
            })
            .unwrap();
        ViewInst {
            id,
            vtype: self.id,
            params,
            handle: thr,
        }
    }
}
