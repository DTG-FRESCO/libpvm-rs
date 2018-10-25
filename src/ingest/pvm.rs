use std::{
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter, Result as FMTResult},
    mem,
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::SyncSender,
    },
};

use data::{
    node_types::{
        ConcreteType, ContextType, CtxNode, DataNode, Name, NameNode, PVMDataType, PVMDataType::*,
        SchemaNode,
    },
    rel_types::{Inf, InfInit, Named, NamedInit, PVMOps, Rel},
    Denumerate, Enumerable, HasID, MetaStore, RelGenerable, ID,
};
use views::DBTr;

use lending_library::{LendingLibrary, Loan};
use uuid::Uuid;

use super::db::DB;

pub enum PVMError {
    MissingField { evt: String, field: &'static str },
}

impl Display for PVMError {
    fn fmt(&self, f: &mut Formatter) -> FMTResult {
        match self {
            PVMError::MissingField { evt, field } => {
                write!(f, "Event {} missing needed field {}", evt, field)
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ConnectDir {
    Mono,
    BiDirectional,
}

pub type NodeGuard = Loan<ID, DataNode>;
pub type RelGuard = Loan<(&'static str, ID, ID), Rel>;
type NameGuard = Loan<Name, NameNode>;

enum CtxStore {
    Node(ID),
    Lazy(&'static ContextType, HashMap<&'static str, String>),
}

pub struct PVM {
    db: DB,
    type_cache: HashSet<&'static ConcreteType>,
    ctx_type_cache: HashSet<&'static ContextType>,
    uuid_cache: HashMap<Uuid, ID>,
    node_cache: LendingLibrary<ID, DataNode>,
    rel_cache: LendingLibrary<(&'static str, ID, ID), Rel>,
    id_counter: AtomicUsize,
    open_cache: HashMap<Uuid, HashSet<Uuid>>,
    name_cache: LendingLibrary<Name, NameNode>,
    cont_cache: HashMap<Uuid, ID>,
    cur_ctx: CtxStore,
    pub unparsed_events: HashSet<String>,
}

impl PVM {
    pub fn new(db: SyncSender<DBTr>) -> Self {
        PVM {
            db: DB::create(db),
            type_cache: HashSet::new(),
            ctx_type_cache: HashSet::new(),
            uuid_cache: HashMap::new(),
            node_cache: LendingLibrary::new(),
            rel_cache: LendingLibrary::new(),
            id_counter: AtomicUsize::new(1),
            open_cache: HashMap::new(),
            name_cache: LendingLibrary::new(),
            cont_cache: HashMap::new(),
            cur_ctx: CtxStore::Node(ID::new(0)),
            unparsed_events: HashSet::new(),
        }
    }

    pub fn new_ctx(&mut self, ty: &'static ContextType, cont: HashMap<&'static str, String>) {
        assert!(self.ctx_type_cache.contains(ty));
        self.cur_ctx = CtxStore::Lazy(ty, cont);
    }

    pub fn ctx(&mut self) -> ID {
        match self.cur_ctx {
            CtxStore::Node(i) => i,
            CtxStore::Lazy(..) => {
                let id = self._nextid();
                let (ty, cont) = match mem::replace(&mut self.cur_ctx, CtxStore::Node(id)) {
                    CtxStore::Lazy(ty, c) => (ty, c),
                    CtxStore::Node(_) => unreachable!(),
                };
                self.db.create_node(CtxNode::new(id, ty, cont).unwrap());
                id
            }
        }
    }

    pub fn release(&mut self, uuid: &Uuid) {
        if let Some(nid) = self.uuid_cache.remove(uuid) {
            self.node_cache.remove(&nid);
        }
    }

    fn _nextid(&mut self) -> ID {
        ID::new(self.id_counter.fetch_add(1, Ordering::Relaxed) as u64)
    }

    fn _decl_rel<T: RelGenerable + Enumerable<Target = Rel>, S: Fn(ID) -> T::Init>(
        &mut self,
        src: ID,
        dst: ID,
        init: S,
    ) -> RelGuard {
        let triple = (stringify!(T), src, dst);
        if self.rel_cache.contains_key(&triple) {
            self.rel_cache.lend(&triple).unwrap()
        } else {
            let id = self._nextid();
            let rel = T::new(id, src, dst, init(self.ctx())).enumerate();
            self.rel_cache.insert(triple, rel);
            let r = self.rel_cache.lend(&triple).unwrap();
            self.db.create_rel(&*r);
            r
        }
    }

    fn _inf(&mut self, src: &impl HasID, dst: &impl HasID, pvm_op: PVMOps) -> RelGuard {
        self._decl_rel::<Inf, _>(src.get_db_id(), dst.get_db_id(), |ctx| InfInit {
            pvm_op,
            ctx,
            byte_count: 0,
        })
    }

    fn _named(&mut self, src: &impl HasID, dst: &NameNode) -> RelGuard {
        self._decl_rel::<Named, _>(src.get_db_id(), dst.get_db_id(), |ctx| NamedInit {
            start: ctx,
            end: ID::new(0),
        })
    }

    pub fn register_data_type(&mut self, ty: &'static ConcreteType) {
        self.type_cache.insert(ty);
        let id = self._nextid();
        self.db.create_node(SchemaNode::from_data(id, ty));
    }

    pub fn register_ctx_type(&mut self, ty: &'static ContextType) {
        self.ctx_type_cache.insert(ty);
        let id = self._nextid();
        self.db.create_node(SchemaNode::from_ctx(id, ty));
    }

    pub fn add(
        &mut self,
        pvm_ty: PVMDataType,
        ty: &'static ConcreteType,
        uuid: Uuid,
        init: Option<MetaStore>,
    ) -> NodeGuard {
        assert!(self.type_cache.contains(&ty));
        let id = self._nextid();
        let node = DataNode::new(pvm_ty, ty, id, uuid, self.ctx(), init);
        if let Some(nid) = self.uuid_cache.insert(uuid, id) {
            self.node_cache.remove(&nid);
        }
        self.node_cache.insert(id, node);
        let n = self.node_cache.lend(&id).unwrap();
        self.db.create_node(&*n);
        n
    }

    pub fn declare(
        &mut self,
        ty: &'static ConcreteType,
        uuid: Uuid,
        init: Option<HashMap<&'static str, String>>,
    ) -> NodeGuard {
        if !self.uuid_cache.contains_key(&uuid) {
            let init = match init {
                Some(v) => Some(MetaStore::from_map(v, self.ctx(), ty)),
                None => None,
            };
            self.add(ty.pvm_ty, ty, uuid, init)
        } else {
            self.node_cache.lend(&self.uuid_cache[&uuid]).unwrap()
        }
    }

    pub fn source(&mut self, act: &DataNode, ent: &DataNode) -> RelGuard {
        assert_eq!(act.pvm_ty(), &Actor);
        self._inf(ent, act, PVMOps::Source)
    }

    pub fn source_nbytes<T: Into<i64>>(
        &mut self,
        act: &DataNode,
        ent: &DataNode,
        bytes: T,
    ) -> RelGuard {
        assert_eq!(act.pvm_ty(), &Actor);
        let mut r = self.source(act, ent);
        Inf::denumerate_mut(&mut r).byte_count += bytes.into();
        self.db.update_rel(&*r);
        r
    }

    pub fn sink(&mut self, act: &DataNode, ent: &DataNode) -> RelGuard {
        assert_eq!(act.pvm_ty(), &Actor);
        match ent.pvm_ty() {
            Store => {
                let ctx = self.ctx();
                let f = self.add(Store, ent.ty(), ent.uuid(), Some(ent.meta.snapshot(ctx)));
                self._inf(ent, &*f, PVMOps::Version);
                self._inf(act, &*f, PVMOps::Sink)
            }
            _ => self._inf(act, ent, PVMOps::Sink),
        }
    }

    pub fn sinkstart(&mut self, act: &DataNode, ent: &DataNode) -> RelGuard {
        assert_eq!(act.pvm_ty(), &Actor);
        match ent.pvm_ty() {
            Store => {
                let ctx = self.ctx();
                let es = self.add(
                    EditSession,
                    ent.ty(),
                    ent.uuid(),
                    Some(ent.meta.snapshot(ctx)),
                );
                self.open_cache.insert(ent.uuid(), hashset!(act.uuid()));
                self._inf(ent, &*es, PVMOps::Version);
                self._inf(act, &*es, PVMOps::Sink)
            }
            EditSession => {
                self.open_cache
                    .get_mut(&ent.uuid())
                    .unwrap()
                    .insert(act.uuid());
                self._inf(act, ent, PVMOps::Sink)
            }
            _ => self._inf(act, ent, PVMOps::Sink),
        }
    }

    pub fn sinkstart_nbytes<T: Into<i64>>(
        &mut self,
        act: &DataNode,
        ent: &DataNode,
        bytes: T,
    ) -> RelGuard {
        assert_eq!(act.pvm_ty(), &Actor);
        let mut r = self.sinkstart(act, ent);
        Inf::denumerate_mut(&mut r).byte_count += bytes.into();
        self.db.update_rel(&*r);
        r
    }

    pub fn sinkend(&mut self, act: &DataNode, ent: &DataNode) {
        assert_eq!(act.pvm_ty(), &Actor);
        if let EditSession = ent.pvm_ty() {
            self.open_cache
                .get_mut(&ent.uuid())
                .unwrap()
                .remove(&act.uuid());
            if self.open_cache[&ent.uuid()].is_empty() {
                let ctx = self.ctx();
                let f = self.add(Store, ent.ty(), ent.uuid(), Some(ent.meta.snapshot(ctx)));
                self._inf(ent, &*f, PVMOps::Version);
            }
        }
    }

    fn decl_name(&mut self, name: Name) -> NameGuard {
        if !self.name_cache.contains_key(&name) {
            let n = NameNode::generate(self._nextid(), name.clone());
            self.db.create_node(&n);
            self.name_cache.insert(name.clone(), n);
        }
        self.name_cache.lend(&name).unwrap()
    }

    fn decl_fcont(&mut self, ent: &DataNode) -> NodeGuard {
        let id = {
            if !self.cont_cache.contains_key(&ent.uuid()) {
                let id = self._nextid();
                let node = DataNode::new(StoreCont, ent.ty(), id, ent.uuid(), ID::new(0), None);
                self.cont_cache.insert(ent.uuid(), id);
                self.db.create_node(&node);
                self.node_cache.insert(id, node);
                id
            } else {
                self.cont_cache[&ent.uuid()]
            }
        };
        self.node_cache.lend(&id).unwrap()
    }

    pub fn name(&mut self, obj: &DataNode, name: Name) -> RelGuard {
        let n_node = self.decl_name(name);
        match obj.pvm_ty() {
            Store | EditSession => {
                let cont = self.decl_fcont(obj);
                self._named(&*cont, &n_node)
            }
            _ => self._named(obj, &n_node),
        }
    }

    pub fn unname(&mut self, obj: &DataNode, name: Name) -> RelGuard {
        let mut rel = self.name(obj, name);
        if let Rel::Named(ref mut n_rel) = *rel {
            n_rel.end = self.ctx();
            self.db.update_rel(&*rel);
        }
        rel
    }

    pub fn meta<T: ToString + ?Sized>(&mut self, ent: &mut DataNode, key: &'static str, val: &T) {
        if !ent.ty().props.contains_key(key) {
            panic!("Setting unknown property on concrete type: {:?} does not have a property named {}.", ent.ty(), key);
        }
        ent.meta.update(key, val, self.ctx(), ent.ty().props[key]);
        self.db.update_node(ent);
    }

    pub fn prop(&mut self, ent: &DataNode) {
        self.db.update_node(ent)
    }

    pub fn connect(&mut self, first: &DataNode, second: &DataNode, dir: ConnectDir) {
        assert_eq!(first.pvm_ty(), &Conduit);
        assert_eq!(second.pvm_ty(), &Conduit);
        self._inf(first, second, PVMOps::Connect);
        if let ConnectDir::BiDirectional = dir {
            self._inf(second, first, PVMOps::Connect);
        }
    }

    pub fn shutdown(self) {}
}
