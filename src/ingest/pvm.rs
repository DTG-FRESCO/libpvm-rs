use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter, Result as FMTResult},
    sync::mpsc::SyncSender,
};

use data::{
    node_types::{
        ConcreteType, ContextType, CtxNode, DataNode, Name, NameNode, PVMDataType, PVMDataType::*,
        SchemaNode,
    },
    rel_types::{Inf, InfInit, Named, NamedInit, PVMOps, Rel},
    Denumerate, Enumerable, HasID, MetaStore, RelGenerable, ID,
};
use id_counter::IDCounter;
use persistence::Persistence;
use views::DBTr;

use either::Either;
use lending_library::Loan;
use transactions::hash_wrap::HashWrap;
use uuid::Uuid;

use super::db::DB;

pub enum PVMError {
    AssertionFailure { cont: String },
    MissingField { evt: String, field: &'static str },
}

impl Display for PVMError {
    fn fmt(&self, f: &mut Formatter) -> FMTResult {
        match self {
            PVMError::AssertionFailure { cont } => write!(f, "Assertion failed, {}", cont),
            PVMError::MissingField { evt, field } => {
                write!(f, "Event {} missing needed field {}", evt, field)
            }
        }
    }
}

pub type PVMResult<T> = Result<T, PVMError>;

#[derive(Clone, Copy, Debug)]
pub enum ConnectDir {
    Mono,
    BiDirectional,
}

pub struct PVM {
    store: Persistence,
    type_cache: HashSet<&'static ConcreteType>,
    ctx_type_cache: HashSet<&'static ContextType>,
    open_cache: HashMap<Uuid, HashSet<Uuid>>,
    pub unparsed_events: HashSet<String>,
}

pub struct PVMTransaction<'a> {
    store: &'a mut Persistence,
    type_cache: &'a HashSet<&'static ConcreteType>,
    id: IDCounter,
    open_cache: HashWrap<'a, Uuid, HashSet<Uuid>>,
    ctx_node: CtxNode,
}

impl<'a> PVMTransaction<'a> {
    fn start(
        base: &'a mut PVM,
        ctx_ty: &'static ContextType,
        ctx_cont: HashMap<&'static str, String>,
    ) -> Self {
        assert!(base.ctx_type_cache.contains(ctx_ty));
        let id = base.store.id_snap();
        let ctx_node = CtxNode::new(id.get(), ctx_ty, ctx_cont).unwrap();
        PVMTransaction {
            store: &mut base.store,
            type_cache: &base.type_cache,
            id,
            open_cache: HashWrap::new(&mut base.open_cache),
            ctx_node,
        }
    }

    pub fn commit(mut self) {}

    pub fn rollback(self) {}

    pub fn release(&mut self, uuid: &Uuid) {
        if let Some(nid) = self.uuid_cache.remove(uuid) {
            self.node_cache.remove(&nid);
        }
    }

    fn _decl_rel<T: RelGenerable + Enumerable<Target = Rel>, S: Fn(ID) -> T::Init>(
        &mut self,
        src: ID,
        dst: ID,
        init: S,
    ) -> ID {
        let triple = (stringify!(T), src, dst);
        if self.rel_src_dst_cache.contains_key(&triple) {
            self.rel_src_dst_cache[&triple]
        } else {
            let id = self.store.id();
            let rel = T::new(id, src, dst, init(self.ctx)).enumerate();
            self.db.create_rel(&rel);
            self.rel_src_dst_cache.insert(triple, id);
            self.rel_cache.insert(id, rel);
            id
        }
    }

    fn _inf(&mut self, src: impl HasID, dst: impl HasID, pvm_op: PVMOps) -> ID {
        self._decl_rel::<Inf, _>(src.get_db_id(), dst.get_db_id(), |ctx| InfInit {
            pvm_op,
            ctx,
            byte_count: 0,
        })
    }

    fn _named(&mut self, src: impl HasID, dst: &NameNode) -> ID {
        self._decl_rel::<Named, _>(src.get_db_id(), dst.get_db_id(), |ctx| NamedInit {
            start: ctx,
            end: ID::new(0),
        })
    }

    pub fn add(
        &mut self,
        pvm_ty: PVMDataType,
        ty: &'static ConcreteType,
        uuid: Uuid,
        init: Option<MetaStore>,
    ) -> PVMResult<ID> {
        if !self.type_cache.contains(&ty) {
            return Err(PVMError::AssertionFailure {
                cont: format!("Unregistered node type {:?}", ty),
            });
        }
        let id = self.id.get();
        let node = DataNode::new(pvm_ty, ty, id, uuid, self.ctx, init);
        if let Some(nid) = self.uuid_cache.insert(uuid, id) {
            self.node_cache.remove(&nid);
        }
        self.db.create_node(&node);
        self.node_cache.insert(id, node);
        Ok(id)
    }

    pub fn declare(
        &mut self,
        ty: &'static ConcreteType,
        uuid: Uuid,
        init: Option<HashMap<&'static str, String>>,
    ) -> PVMResult<ID> {
        if !self.uuid_cache.contains_key(&uuid) {
            let init = match init {
                Some(v) => Some(MetaStore::from_map(v, self.ctx, ty)),
                None => None,
            };
            self.add(ty.pvm_ty, ty, uuid, init)
        } else {
            Ok(self.uuid_cache[&uuid])
        }
    }

    fn _version(&mut self, src: &DataNode, choice: Either<Uuid, PVMDataType>) -> PVMResult<ID> {
        let ctx = self.ctx;
        let dst = match choice {
            Either::Left(uuid) => {
                let dst_id = self.declare(src.ty(), uuid, None)?;
                let mut dst = self._node(dst_id);
                dst.meta.merge(&src.meta.snapshot(ctx));
                self.db.update_node(&*dst);
                dst_id
            }
            Either::Right(pvm_ty) => {
                self.add(pvm_ty, src.ty(), src.uuid(), Some(src.meta.snapshot(ctx)))?
            }
        };
        self._inf(src, dst, PVMOps::Version);
        Ok(dst)
    }

    pub fn derive(&mut self, src: ID, dst: Uuid) -> PVMResult<ID> {
        let src = self._node(src);
        self._version(&src, Either::Left(dst))
    }

    pub fn source(&mut self, act: ID, ent: ID) -> PVMResult<ID> {
        if self._node(act).pvm_ty() != &Actor {
            return Err(PVMError::AssertionFailure {
                cont: "source with non actor".into(),
            });
        }
        Ok(self._inf(ent, act, PVMOps::Source))
    }

    pub fn source_nbytes<T: Into<i64>>(&mut self, act: ID, ent: ID, bytes: T) -> PVMResult<ID> {
        if self._node(act).pvm_ty() != &Actor {
            return Err(PVMError::AssertionFailure {
                cont: "source with non actor".into(),
            });
        }
        let id = self.source(act, ent)?;
        let mut r = self._rel(id);
        Inf::denumerate_mut(&mut r).byte_count += bytes.into();
        self.db.update_rel(&*r);
        Ok(id)
    }

    pub fn sink(&mut self, act: ID, ent: ID) -> PVMResult<ID> {
        let ent = self._node(ent);
        if self._node(act).pvm_ty() != &Actor {
            return Err(PVMError::AssertionFailure {
                cont: "sink with non actor".into(),
            });
        }
        Ok(match ent.pvm_ty() {
            Store => {
                let f = self._version(&ent, Either::Right(Store))?;
                self._inf(act, f, PVMOps::Sink)
            }
            _ => self._inf(act, &*ent, PVMOps::Sink),
        })
    }

    pub fn sinkstart(&mut self, act: ID, ent: ID) -> PVMResult<ID> {
        let act = self._node(act);
        let ent = self._node(ent);
        if act.pvm_ty() != &Actor {
            return Err(PVMError::AssertionFailure {
                cont: "sinkstart with non actor".into(),
            });
        }
        Ok(match ent.pvm_ty() {
            Store => {
                let es = self._version(&ent, Either::Right(EditSession))?;
                self.open_cache.insert(ent.uuid(), hashset!(act.uuid()));
                self._inf(&*act, es, PVMOps::Sink)
            }
            EditSession => {
                self.open_cache
                    .get_mut(&ent.uuid())
                    .unwrap()
                    .insert(act.uuid());
                self._inf(&*act, &*ent, PVMOps::Sink)
            }
            _ => self._inf(&*act, &*ent, PVMOps::Sink),
        })
    }

    pub fn sinkstart_nbytes<T: Into<i64>>(&mut self, act: ID, ent: ID, bytes: T) -> PVMResult<ID> {
        if self._node(act).pvm_ty() != &Actor {
            return Err(PVMError::AssertionFailure {
                cont: "sinkstart with non actor".into(),
            });
        }
        let id = self.sinkstart(act, ent)?;
        let mut r = self._rel(id);
        Inf::denumerate_mut(&mut r).byte_count += bytes.into();
        self.db.update_rel(&*r);
        Ok(id)
    }

    pub fn sinkend(&mut self, act: ID, ent: ID) -> PVMResult<()> {
        let ent = self._node(ent);
        let act = self._node(act);
        if act.pvm_ty() != &Actor {
            return Err(PVMError::AssertionFailure {
                cont: "sinkend with non actor".into(),
            });
        }
        if let EditSession = ent.pvm_ty() {
            self.open_cache
                .get_mut(&ent.uuid())
                .unwrap()
                .remove(&act.uuid());
            if self.open_cache[&ent.uuid()].is_empty() {
                self._version(&ent, Either::Right(Store))?;
            }
        }
        Ok(())
    }

    fn decl_name(&mut self, name: Name) -> Loan<Name, NameNode> {
        if !self.name_cache.contains_key(&name) {
            let n = NameNode::generate(self.id.get(), name.clone());
            self.db.create_node(&n);
            self.name_cache.insert(name.clone(), n);
        }
        self.name_cache.lend(&name).unwrap()
    }

    pub fn name(&mut self, obj: ID, name: Name) -> PVMResult<ID> {
        let n_node = self.decl_name(name);
        Ok(self._named(obj, &n_node))
    }

    pub fn unname(&mut self, obj: ID, name: Name) -> PVMResult<ID> {
        let id = self.name(obj, name)?;
        let mut rel = self._rel(id);
        if let Rel::Named(ref mut n_rel) = *rel {
            n_rel.end = self.ctx;
            self.db.update_rel(&*rel);
        }
        Ok(id)
    }

    pub fn meta<T: ToString + ?Sized>(
        &mut self,
        ent: ID,
        key: &'static str,
        val: &T,
    ) -> PVMResult<()> {
        let mut ent = self._node(ent);
        if !ent.ty().props.contains_key(key) {
            panic!("Setting unknown property on concrete type: {:?} does not have a property named {}.", ent.ty(), key);
        }
        let heritable = ent.ty().props[key];
        ent.meta.update(key, val, self.ctx, heritable);
        self.db.update_node(&*ent);
        Ok(())
    }

    pub fn connect(&mut self, first: ID, second: ID, dir: ConnectDir) -> PVMResult<()> {
        if self._node(first).pvm_ty() != &Conduit {
            return Err(PVMError::AssertionFailure {
                cont: "connect with primary non conduit".into(),
            });
        }
        if self._node(second).pvm_ty() != &Conduit {
            return Err(PVMError::AssertionFailure {
                cont: "connect with secondary non conduit".into(),
            });
        }
        self._inf(first, second, PVMOps::Connect);
        if let ConnectDir::BiDirectional = dir {
            self._inf(second, first, PVMOps::Connect);
        }
        Ok(())
    }
}

impl PVM {
    pub fn new(db: SyncSender<DBTr>) -> Self {
        PVM {
            db: DB::create(db),
            type_cache: HashSet::new(),
            ctx_type_cache: HashSet::new(),
            uuid_cache: HashMap::new(),
            rel_src_dst_cache: HashMap::new(),
            open_cache: HashMap::new(),
            unparsed_events: HashSet::new(),
        }
    }

    pub fn transaction(
        &mut self,
        ctx_ty: &'static ContextType,
        ctx_cont: HashMap<&'static str, String>,
    ) -> PVMTransaction {
        PVMTransaction::start(self, ctx_ty, ctx_cont)
    }

    pub fn register_data_type(&mut self, ty: &'static ConcreteType) {
        self.type_cache.insert(ty);
        self.db
            .create_node(SchemaNode::from_data(self.id.get(), ty));
    }

    pub fn register_ctx_type(&mut self, ty: &'static ContextType) {
        self.ctx_type_cache.insert(ty);
        self.db.create_node(SchemaNode::from_ctx(self.id.get(), ty));
    }

    pub fn shutdown(self) {}
}
