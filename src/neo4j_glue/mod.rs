mod csv_view;
mod neo4j_view;

pub use self::{csv_view::CSVView, neo4j_view::Neo4JView};

use std::{borrow::Cow, collections::HashMap, mem};

use neo4j::Value;

use serde_json;

use data::{
    node_types::{NameNode, Node, PVMDataType, PVMDataType::*, SchemaNode},
    rel_types::{PVMOps, Rel},
    HasDst, HasID, HasSrc, MetaStore, ID,
};

use uuid::Uuid;

use chrono::{DateTime, Utc};

pub trait Val2UUID {
    fn into_uuid(self) -> Option<Uuid>;
}

pub trait IntoVal {
    fn into_val(self) -> Value;
}

impl Val2UUID for Value {
    fn into_uuid(self) -> Option<Uuid> {
        match self {
            Value::String(s) => Uuid::parse_str(&s).ok(),
            _ => None,
        }
    }
}

impl IntoVal for Uuid {
    fn into_val(self) -> Value {
        Value::String(self.to_hyphenated_ref().to_string())
    }
}

impl IntoVal for ID {
    fn into_val(self) -> Value {
        Value::Integer(unsafe { mem::transmute::<u64, i64>(self.inner()) })
    }
}

impl IntoVal for DateTime<Utc> {
    fn into_val(self) -> Value {
        Value::Integer(self.timestamp_nanos())
    }
}

impl IntoVal for PVMOps {
    fn into_val(self) -> Value {
        match self {
            PVMOps::Sink => "Sink".into(),
            PVMOps::Source => "Source".into(),
            PVMOps::Connect => "Connect".into(),
            PVMOps::Version => "Version".into(),
            PVMOps::Unknown => "Unknown".into(),
        }
    }
}

impl IntoVal for PVMDataType {
    fn into_val(self) -> Value {
        self.to_string().into()
    }
}

pub trait IntoID {
    fn into_id(self) -> Option<ID>;
}

impl IntoID for Value {
    fn into_id(self) -> Option<ID> {
        match self {
            Value::Integer(i) => Some(ID::new(unsafe { mem::transmute::<i64, u64>(i) })),
            _ => None,
        }
    }
}

fn into_props(meta: &MetaStore) -> HashMap<Cow<'static, str>, Value> {
    let mut ret = HashMap::new();
    for (k, v, _, _) in meta.iter_latest() {
        ret.insert(k.to_string().into(), Value::from(v));
    }
    ret.insert(
        "meta_hist".into(),
        serde_json::to_string(&meta).unwrap().into(),
    );
    ret
}

pub trait ToDBNode: HasID {
    fn get_labels(&self) -> Vec<&'static str>;
    fn get_props(&self) -> HashMap<Cow<'static, str>, Value>;
    fn to_db(&self) -> (ID, Vec<&'static str>, HashMap<Cow<'static, str>, Value>) {
        let mut props = self.get_props();
        props.insert("db_id".into(), self.get_db_id().into_val());
        (self.get_db_id(), self.get_labels(), props)
    }
}

impl ToDBNode for Node {
    fn get_labels(&self) -> Vec<&'static str> {
        match self {
            Node::Data(d) => match d.pvm_ty() {
                EditSession => vec!["Node", "EditSession"],
                Store => vec!["Node", "Store"],
                Actor => vec!["Node", "Actor"],
                Conduit => vec!["Node", "Conduit"],
            },
            Node::Ctx(_) => vec!["Node", "Context"],
            Node::Name(n) => match n {
                NameNode::Path(..) => vec!["Node", "Name", "Path"],
                NameNode::Net(..) => vec!["Node", "Name", "Net"],
            },
            Node::Schema(_) => vec!["Node", "Schema"],
        }
    }

    fn get_props(&self) -> HashMap<Cow<'static, str>, Value> {
        match self {
            Node::Data(d) => {
                let mut props = into_props(&d.meta);
                props.insert("uuid".into(), d.uuid().into_val());
                props.insert("type".into(), d.ty().name.into());
                props.insert("ctx".into(), d.ctx().into_val());
                props
            }
            Node::Ctx(c) => {
                let mut props: HashMap<Cow<'static, str>, Value> = c
                    .cont
                    .iter()
                    .map(|(k, v)| (k.to_string().into(), Value::from(v as &str)))
                    .collect();
                props.insert("type".into(), c.ty().name.into());
                props
            }
            Node::Name(n) => match n {
                NameNode::Path(_, path) => hashmap!("path".into() => Value::from(path.clone())),
                NameNode::Net(_, addr, port) => {
                    hashmap!("addr".into() => Value::from(addr.clone()),
                                                         "port".into() => Value::from(*port))
                }
            },
            Node::Schema(s) => match s {
                SchemaNode::Data(_, ty) => {
                    let props: Vec<&str> = ty.props.keys().cloned().collect();
                    hashmap!("name".into() => Value::from(ty.name),
                             "base".into() => ty.pvm_ty.into_val(),
                             "props".into() => Value::from(props))
                }
                SchemaNode::Context(_, ty) => hashmap!("name".into() => Value::from(ty.name),
                             "base".into() => Value::from("Context"),
                             "props".into() => Value::from(ty.props.clone())),
            },
        }
    }
}

pub trait ToDBRel {
    fn to_db(&self) -> (ID, Value);
}

impl ToDBRel for Rel {
    fn to_db(&self) -> (ID, Value) {
        match self {
            Rel::Inf(i) => {
                let props: HashMap<&str, Value> = hashmap!("db_id" => i.get_db_id().into_val(),
                                                           "pvm_op" => i.pvm_op.into_val(),
                                                           "ctx" => i.ctx.into_val(),
                                                           "byte_count" => Value::from(i.byte_count));
                (
                    i.get_db_id(),
                    hashmap!("src" => i.get_src().into_val(),
                             "dst" => i.get_dst().into_val(),
                             "type" => Value::from("INF"),
                             "props" => Value::from(props))
                    .into(),
                )
            }
            Rel::Named(n) => {
                let props: HashMap<&str, Value> = hashmap!("db_id" => n.get_db_id().into_val(),
                                                           "start" => n.start.into_val(),
                                                           "end" => n.end.into_val());
                (
                    n.get_db_id(),
                    hashmap!("src" => n.get_src().into_val(),
                             "dst" => n.get_dst().into_val(),
                             "type" => Value::from("NAMED"),
                             "props" => Value::from(props))
                    .into(),
                )
            }
        }
    }
}
