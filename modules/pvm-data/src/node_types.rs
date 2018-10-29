use super::{meta_store::MetaStore, Enumerable, HasID, ID};
use std::{
    collections::HashMap,
    fmt,
    hash::{Hash, Hasher},
};
use uuid::Uuid;

#[derive(Debug, Eq)]
pub struct ConcreteType {
    pub pvm_ty: PVMDataType,
    pub name: &'static str,
    pub props: HashMap<&'static str, bool>,
}

impl Hash for ConcreteType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        <&'static str as Hash>::hash(&self.name, state)
    }
}

impl PartialEq for ConcreteType {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

#[derive(Debug, Eq, Hash, PartialEq)]
pub struct ContextType {
    pub name: &'static str,
    pub props: Vec<&'static str>,
}

#[derive(Clone, Debug)]
pub enum Node {
    Data(DataNode),
    Ctx(CtxNode),
    Name(NameNode),
    Schema(SchemaNode),
}

impl HasID for Node {
    fn get_db_id(&self) -> ID {
        match self {
            Node::Data(n) => n.get_db_id(),
            Node::Ctx(n) => n.get_db_id(),
            Node::Name(n) => n.get_db_id(),
            Node::Schema(n) => n.get_db_id(),
        }
    }
}

impl Enumerable for Node {
    type Target = Node;
    fn enumerate(self) -> Node {
        self
    }
}

impl Enumerable for NameNode {
    type Target = Node;
    fn enumerate(self) -> Node {
        Node::Name(self)
    }
}

impl Enumerable for CtxNode {
    type Target = Node;
    fn enumerate(self) -> Node {
        Node::Ctx(self)
    }
}

impl Enumerable for DataNode {
    type Target = Node;
    fn enumerate(self) -> Node {
        Node::Data(self)
    }
}

impl Enumerable for SchemaNode {
    type Target = Node;
    fn enumerate(self) -> Node {
        Node::Schema(self)
    }
}

#[derive(Clone, Debug)]
pub struct CtxNode {
    id: ID,
    ty: &'static ContextType,
    pub cont: HashMap<&'static str, String>,
}

impl CtxNode {
    pub fn new(
        id: ID,
        ty: &'static ContextType,
        cont: HashMap<&'static str, String>,
    ) -> Result<CtxNode, String> {
        for k in cont.keys() {
            if !ty.props.contains(k) {
                return Err(format!(
                    "Error: {} is not a valid property for a {:?}",
                    k, ty
                ));
            }
        }
        Ok(CtxNode { id, ty, cont })
    }

    pub fn ty(&self) -> &'static ContextType {
        self.ty
    }
}

impl HasID for CtxNode {
    fn get_db_id(&self) -> ID {
        self.id
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PVMDataType {
    Actor,
    Store,
    Conduit,
    EditSession,
    StoreCont,
}

impl fmt::Display for PVMDataType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                PVMDataType::Actor => "Actor",
                PVMDataType::Conduit => "Conduit",
                PVMDataType::EditSession => "EditSession",
                PVMDataType::Store => "Store",
                PVMDataType::StoreCont => "StoreCont",
            }
        )
    }
}

impl PVMDataType {
    pub fn compatible_concrete(self, ty: &ConcreteType) -> bool {
        ty.pvm_ty == self
            || (self == PVMDataType::EditSession && ty.pvm_ty == PVMDataType::Store)
            || (self == PVMDataType::StoreCont && ty.pvm_ty == PVMDataType::Store)
    }
}

#[derive(Clone, Debug)]
pub struct DataNode {
    pvm_ty: PVMDataType,
    ty: &'static ConcreteType,
    id: ID,
    uuid: Uuid,
    ctx: ID,
    pub meta: MetaStore,
}

impl HasID for DataNode {
    fn get_db_id(&self) -> ID {
        self.id
    }
}

impl DataNode {
    pub fn new(
        pvm_type: PVMDataType,
        ty: &'static ConcreteType,
        id: ID,
        uuid: Uuid,
        ctx: ID,
        meta: Option<MetaStore>,
    ) -> DataNode {
        if !pvm_type.compatible_concrete(ty) {
            panic!(
                "Invalid PVMDataType for given ConcreteType: {:?} cannot be a {:?}.",
                ty.name, pvm_type
            );
        }
        DataNode {
            pvm_ty: pvm_type,
            id,
            uuid,
            ctx,
            ty,
            meta: meta.unwrap_or_else(MetaStore::new),
        }
    }

    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    pub fn ty(&self) -> &'static ConcreteType {
        self.ty
    }

    pub fn ctx(&self) -> ID {
        self.ctx
    }

    pub fn pvm_ty(&self) -> &PVMDataType {
        &self.pvm_ty
    }
}

#[derive(Clone, Debug, Hash, PartialEq)]
pub enum Name {
    Path(String),
    Net(String, u16),
}

#[derive(Clone, Debug)]
pub enum NameNode {
    Path(ID, String),
    Net(ID, String, u16),
}

impl HasID for NameNode {
    fn get_db_id(&self) -> ID {
        match self {
            NameNode::Path(id, _) => *id,
            NameNode::Net(id, _, _) => *id,
        }
    }
}

impl NameNode {
    pub fn generate(id: ID, init: Name) -> Self {
        match init {
            Name::Path(s) => NameNode::Path(id, s),
            Name::Net(s, p) => NameNode::Net(id, s, p),
        }
    }
}

#[derive(Clone, Debug)]
pub enum SchemaNode {
    Data(ID, &'static ConcreteType),
    Context(ID, &'static ContextType),
}

impl HasID for SchemaNode {
    fn get_db_id(&self) -> ID {
        match self {
            SchemaNode::Data(i, _) => *i,
            SchemaNode::Context(i, _) => *i,
        }
    }
}

impl SchemaNode {
    pub fn from_ctx(id: ID, val: &'static ContextType) -> Self {
        SchemaNode::Context(id, val)
    }

    pub fn from_data(id: ID, val: &'static ConcreteType) -> Self {
        SchemaNode::Data(id, val)
    }
}
