use std::{
    borrow::Cow,
    collections::HashMap,
    fs::File,
    io::Write,
    mem,
    sync::{mpsc::Receiver, Arc},
    thread,
};

use crate::{
    cfg::Config,
    data::{
        node_types::{NameNode, Node, PVMDataType::*, SchemaNode},
        rel_types::Rel,
        HasDst, HasID, HasSrc, ID,
    },
    view::*,
};

use maplit::hashmap;
use serde_json;
use zip::{write::FileOptions, ZipWriter};

const HYDRATE_SH_PRE: &str = r#"#! /bin/bash
export NEO4J_USER=neo4j
export NEO4J_PASS=opus

if ! which "neo4j-admin" >/dev/null || ! which "cypher-shell" >/dev/null ; then
    echo "Cannot find neo4j binaries"
    echo "Please make sure that the neo4j binaries are in \$PATH"
    exit 1
fi

echo "Preparing to hydrate database"
read -p "Ensure neo4j is stopped and that any database files have been removed. Then press enter."
echo "Importing data"
"#;

const HYDRATE_SH_POST: &str = r#"echo "Data import complete"
read -p "Now start neo4j, wait for it to come up, then press enter."
echo -n "Building indexes..."
cypher-shell -u$NEO4J_USER -p$NEO4J_PASS >/dev/null <<EOF
CREATE INDEX ON :Node(db_id);
CREATE INDEX ON :Actor(uuid);
CREATE INDEX ON :Object(uuid);
CREATE INDEX ON :Store(uuid);
CREATE INDEX ON :EditSession(uuid);
CREATE INDEX ON :Conduit(uuid);
CREATE INDEX ON :Path(path);
CREATE INDEX ON :Net(addr);
CALL db.awaitIndexes();
EOF
echo "Done"
echo "Database hydrated"
"#;

#[derive(Debug)]
pub struct CSVView {
    id: usize,
}

fn write_str<W: Write>(f: &mut W, s: &str) {
    write!(f, ",\"{}\"", s.replace("\"", "\"\"")).unwrap();
}

impl View for CSVView {
    fn new(id: usize) -> CSVView {
        CSVView { id }
    }
    fn id(&self) -> usize {
        self.id
    }
    fn name(&self) -> &'static str {
        "CSVView"
    }
    fn desc(&self) -> &'static str {
        "View for writing a static csv files for later consumption."
    }
    fn params(&self) -> HashMap<&'static str, &'static str> {
        hashmap!("path" => "The file to write the csv data to.")
    }
    fn create(
        &self,
        id: usize,
        params: ViewParams,
        _cfg: &Config,
        stream: Receiver<Arc<DBTr>>,
    ) -> ViewInst {
        let path = params.get_or_def("path", "./prov_csv.zip");
        let mut out = ZipWriter::new(File::create(path).unwrap());
        let thr = thread::spawn(move || {
            out.start_file("db/n_dbinfo.csv", FileOptions::default())
                .unwrap();
            writeln!(out, ":LABEL,pvm_version:int,source").unwrap();
            writeln!(out, "DBInfo,2,libPVM-{}", crate::VERSION).unwrap();

            let mut nodes: HashMap<Cow<'static, str>, HashMap<ID, Node>> = HashMap::new();
            let mut rels: HashMap<Cow<'static, str>, HashMap<ID, Rel>> = HashMap::new();

            for evt in stream {
                match *evt {
                    DBTr::CreateNode(ref node) | DBTr::UpdateNode(ref node) => {
                        nodes
                            .entry(node.fname())
                            .or_insert_with(HashMap::new)
                            .insert(node.get_db_id(), node.clone());
                    }
                    DBTr::CreateRel(ref rel) | DBTr::UpdateRel(ref rel) => {
                        rels.entry(rel.fname())
                            .or_insert_with(HashMap::new)
                            .insert(rel.get_db_id(), rel.clone());
                    }
                }
            }

            out.start_file(
                "db/hydrate.sh",
                FileOptions::default().unix_permissions(0o755),
            )
            .unwrap();
            {
                write!(out, "{}", HYDRATE_SH_PRE).unwrap();
                let mut options = vec![
                    "--id-type=INTEGER".to_string(),
                    "--multiline-fields=true".to_string(),
                    "--nodes n_dbinfo.csv".to_string(),
                ];
                options.extend(nodes.keys().map(|k| format!("--nodes {}", k)));
                options.extend(rels.keys().map(|k| format!("--relationships {}", k)));
                writeln!(out, "neo4j-admin import {}", options.join(" "),).unwrap();
                write!(out, "{}", HYDRATE_SH_POST).unwrap();
            }

            for (fname, rlist) in rels {
                out.start_file(format!("db/{}", fname), FileOptions::default())
                    .unwrap();
                for (i, r) in rlist.values().enumerate() {
                    if i == 0 {
                        write!(out, "db_id,:START_ID,:END_ID,:TYPE").unwrap();
                        match r {
                            Rel::Inf(_) => {
                                writeln!(out, ",pvm_op,ctx:long,byte_count:long").unwrap()
                            }
                            Rel::Named(_) => writeln!(out, ",start:long,end:long").unwrap(),
                        }
                    }
                    write!(
                        out,
                        "{},{},{},{}",
                        format_id(r.get_db_id()),
                        format_id(r.get_src()),
                        format_id(r.get_dst()),
                        r._lab(),
                    )
                    .unwrap();
                    match r {
                        Rel::Inf(i) => writeln!(
                            out,
                            ",{:?},\"{}\",{}",
                            i.pvm_op,
                            format_id(i.ctx),
                            i.byte_count
                        )
                        .unwrap(),
                        Rel::Named(n) => {
                            writeln!(out, ",{},\"{}\"", format_id(n.start), format_id(n.end),)
                                .unwrap()
                        }
                    }
                }
            }
            for (fname, nlist) in nodes {
                out.start_file(format!("db/{}", fname), FileOptions::default())
                    .unwrap();
                for (i, n) in nlist.values().enumerate() {
                    if i == 0 {
                        write!(out, "db_id:ID,:LABEL").unwrap();
                        match n {
                            Node::Data(d) => {
                                write!(out, ",uuid,ty,ctx:long,meta_hist").unwrap();
                                for k in d.ty().props.keys() {
                                    write!(out, ",{}", k).unwrap();
                                }
                                writeln!(out).unwrap();
                            }
                            Node::Ctx(c) => {
                                write!(out, ",ty").unwrap();
                                for f in &c.ty().props {
                                    write!(out, ",{}", f).unwrap();
                                }
                                writeln!(out).unwrap();
                            }
                            Node::Name(n) => match n {
                                NameNode::Path(..) => writeln!(out, ",path").unwrap(),
                                NameNode::Net(..) => writeln!(out, ",addr,port:int").unwrap(),
                            },
                            Node::Schema(_) => writeln!(out, ",name,base,props:string[]").unwrap(),
                        }
                    }
                    write!(out, "{},{}", format_id(n.get_db_id()), n._lab()).unwrap();
                    match n {
                        Node::Data(d) => {
                            write!(out, ",{},{},{}", d.uuid(), d.ty().name, format_id(d.ctx()))
                                .unwrap();
                            write_str(&mut out, &serde_json::to_string(&d.meta).unwrap());
                            for k in d.ty().props.keys() {
                                let val = d.meta.cur(k);
                                match val {
                                    Some(v) => write_str(&mut out, v),
                                    None => write!(out, ",").unwrap(),
                                }
                            }
                        }
                        Node::Ctx(c) => {
                            write!(out, ",{}", c.ty().name).unwrap();
                            for f in &c.ty().props {
                                write!(out, ",{}", c.cont[f]).unwrap();
                            }
                            writeln!(out).unwrap();
                        }
                        Node::Name(n) => match n {
                            NameNode::Path(_, path) => {
                                write_str(&mut out, path);
                            }
                            NameNode::Net(_, addr, port) => {
                                write_str(&mut out, addr);
                                write!(out, ",{}", port).unwrap();
                            }
                        },
                        Node::Schema(s) => match s {
                            SchemaNode::Data(_, ty) => {
                                write_str(&mut out, ty.name);
                                let v: Vec<&str> = ty.props.keys().cloned().collect();
                                write!(out, ",{},{}", ty.pvm_ty, v.join(";")).unwrap();
                            }
                            SchemaNode::Context(_, ty) => {
                                write_str(&mut out, ty.name);
                                write!(out, ",Context,{}", ty.props.join(";")).unwrap();
                            }
                        },
                    }
                    writeln!(out).unwrap();
                }
            }
            out.finish().unwrap();
        });
        ViewInst {
            id,
            vtype: self.id,
            params,
            handle: thr,
        }
    }
}

fn format_id(v: ID) -> i64 {
    format_u64(v.inner())
}

fn format_u64(v: u64) -> i64 {
    unsafe { mem::transmute::<u64, i64>(v) }
}

trait ToCSV {
    fn fname(&self) -> Cow<'static, str>;
    fn _lab(&self) -> &str;
}

impl ToCSV for Node {
    fn fname(&self) -> Cow<'static, str> {
        match self {
            Node::Data(d) => match d.pvm_ty() {
                Actor => format!("n_actor_{}.csv", d.ty().name),
                Store => format!("n_store_{}.csv", d.ty().name),
                Conduit => format!("n_conduit_{}.csv", d.ty().name),
                EditSession => format!("n_es_{}.csv", d.ty().name),
            }
            .into(),
            Node::Ctx(n) => format!("n_ctx_{}.csv", n.ty().name).into(),
            Node::Name(n) => match n {
                NameNode::Path(..) => "n_paths.csv",
                NameNode::Net(..) => "n_net.csv",
            }
            .into(),
            Node::Schema(_) => "n_schema.csv".into(),
        }
    }

    fn _lab(&self) -> &str {
        match self {
            Node::Data(d) => match d.pvm_ty() {
                Actor => "Node;Actor",
                Store => "Node;Store",
                EditSession => "Node;EditSession",
                Conduit => "Node;Conduit",
            },
            Node::Ctx(_) => "Node;Context",
            Node::Name(n) => match n {
                NameNode::Path(..) => "Node;Name;Path",
                NameNode::Net(..) => "Node;Name;Net",
            },
            Node::Schema(_) => "Node;Schema",
        }
    }
}

impl ToCSV for Rel {
    fn fname(&self) -> Cow<'static, str> {
        match self {
            Rel::Inf(_) => "r_inf.csv",
            Rel::Named(_) => "r_named.csv",
        }
        .into()
    }

    fn _lab(&self) -> &str {
        match self {
            Rel::Inf(_) => "INF",
            Rel::Named(_) => "NAMED",
        }
    }
}
