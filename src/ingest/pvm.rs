use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    fmt::{Display, Formatter, Result as FMTResult},
    fs::File,
    io::{Seek, SeekFrom, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc::SyncSender,
    },
    time::Instant,
};

use data::{
    node_types::{
        ConcreteType, ContextType, CtxNode, DataNode, Name, NameNode, PVMDataType, PVMDataType::*,
        SchemaNode,
    },
    rel_types::{Inf, InfInit, Named, NamedInit, PVMOps, Rel},
    Denumerate, Enumerable, HasID, MetaStore, RelGenerable, ID,
};
use view::DBTr;

use bytesize::to_string as to_human_bytes;
use either::Either;
use lending_library::{LendingLibrary, Loan};
use transactions::{hash_wrap::HashWrap, lending_wrap::LendingWrap};
use uuid::Uuid;

use super::db::{DBStore, DB};

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

#[derive(Debug)]
pub struct IDCounter {
    store: AtomicUsize,
}

impl IDCounter {
    pub fn new(init: usize) -> Self {
        IDCounter {
            store: AtomicUsize::new(init),
        }
    }

    pub fn get(&self) -> ID {
        ID::new(self.store.fetch_add(1, Ordering::Relaxed) as u64)
    }

    pub fn snapshot(&self) -> Self {
        IDCounter {
            store: AtomicUsize::new(self.store.load(Ordering::Relaxed)),
        }
    }
}

#[derive(Debug)]
struct IDWrap<'a> {
    inner: &'a mut IDCounter,
    cur: IDCounter,
}

impl<'a> IDWrap<'a> {
    pub fn new(inner: &'a mut IDCounter) -> Self {
        let cur = inner.snapshot();
        IDWrap { inner, cur }
    }

    pub fn get(&self) -> ID {
        self.cur.get()
    }

    pub fn commit(self) {
        self.inner
            .store
            .store(self.cur.store.load(Ordering::SeqCst), Ordering::SeqCst);
    }
}

#[derive(Clone, Copy, Debug)]
pub enum ConnectDir {
    Mono,
    BiDirectional,
}

pub struct PVM {
    db: DB,
    type_cache: HashSet<&'static ConcreteType>,
    ctx_type_cache: HashSet<&'static ContextType>,
    uuid_cache: HashMap<Uuid, ID>,
    node_cache: LendingLibrary<ID, DataNode>,
    rel_src_dst_cache: HashMap<(&'static str, ID, ID), ID>,
    rel_cache: LendingLibrary<ID, Rel>,
    id: IDCounter,
    open_cache: HashMap<Uuid, HashSet<Uuid>>,
    name_cache: LendingLibrary<Name, NameNode>,
    pub unparsed_events: HashSet<String>,
    perf_mon: RefCell<PerfMon>,
}

pub struct PVMTransaction<'a> {
    db: DBStore<'a>,
    type_cache: &'a HashSet<&'static ConcreteType>,
    uuid_cache: HashWrap<'a, Uuid, ID>,
    node_cache: LendingWrap<'a, ID, DataNode>,
    rel_src_dst_cache: HashWrap<'a, (&'static str, ID, ID), ID>,
    rel_cache: LendingWrap<'a, ID, Rel>,
    id: IDWrap<'a>,
    open_cache: HashWrap<'a, Uuid, HashSet<Uuid>>,
    name_cache: LendingWrap<'a, Name, NameNode>,
    ctx: ID,
    ctx_node: CtxNode,
}

impl<'a> PVMTransaction<'a> {
    fn start(
        base: &'a mut PVM,
        ctx_ty: &'static ContextType,
        ctx_cont: HashMap<&'static str, String>,
    ) -> Self {
        let id = IDWrap::new(&mut base.id);
        let ctx = id.get();
        let ctx_node = CtxNode::new(ctx, ctx_ty, ctx_cont).unwrap();
        PVMTransaction {
            db: base.db.store(),
            type_cache: &base.type_cache,
            uuid_cache: HashWrap::new(&mut base.uuid_cache),
            node_cache: LendingWrap::new(&mut base.node_cache),
            rel_src_dst_cache: HashWrap::new(&mut base.rel_src_dst_cache),
            rel_cache: LendingWrap::new(&mut base.rel_cache),
            id,
            open_cache: HashWrap::new(&mut base.open_cache),
            name_cache: LendingWrap::new(&mut base.name_cache),
            ctx,
            ctx_node,
        }
    }

    pub fn commit(mut self) {
        self.uuid_cache.commit();
        self.node_cache.commit();
        self.rel_src_dst_cache.commit();
        self.rel_cache.commit();
        self.open_cache.commit();
        self.name_cache.commit();
        if self.db.len() == 0 {
        } else {
            self.id.commit();
            self.db.create_node(self.ctx_node);
            self.db.commit();
        }
    }

    pub fn rollback(self) {
        self.uuid_cache.rollback();
        self.node_cache.commit();
        self.rel_src_dst_cache.rollback();
        self.rel_cache.commit();
        self.open_cache.rollback();
        self.name_cache.commit();
    }

    pub fn release(&mut self, uuid: &Uuid) {
        if let Some(nid) = self.uuid_cache.remove(uuid) {
            self.node_cache.remove(&nid);
        }
    }

    fn _node(&mut self, id: ID) -> Loan<ID, DataNode> {
        self.node_cache.lend(&id).unwrap()
    }

    fn _rel(&mut self, id: ID) -> Loan<ID, Rel> {
        self.rel_cache.lend(&id).unwrap()
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
            let id = self.id.get();
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
                self.open_cache.remove(&ent.uuid());
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

fn size_of_ll<K: std::hash::Hash, V>(v: &LendingLibrary<K, V>) -> u64 {
    use std::mem::size_of;

    ((v.capacity() * 11 / 10) * (size_of::<K>() + size_of::<V>() + size_of::<u64>() * 2)) as u64
}

fn size_of_hm<K, V>(v: &HashMap<K, V>) -> u64 {
    use std::mem::size_of;

    ((v.capacity() * 11 / 10) * (size_of::<K>() + size_of::<V>() + size_of::<u64>())) as u64
}

impl PVM {
    pub fn new(db: SyncSender<DBTr>) -> Self {
        PVM {
            db: DB::create(db),
            type_cache: HashSet::new(),
            ctx_type_cache: HashSet::new(),
            uuid_cache: HashMap::new(),
            node_cache: LendingLibrary::new(),
            rel_src_dst_cache: HashMap::new(),
            rel_cache: LendingLibrary::new(),
            id: IDCounter::new(1),
            open_cache: HashMap::new(),
            name_cache: LendingLibrary::new(),
            unparsed_events: HashSet::new(),
            perf_mon: RefCell::new(PerfMon::new()),
        }
    }

    pub fn transaction(
        &mut self,
        ctx_ty: &'static ContextType,
        ctx_cont: HashMap<&'static str, String>,
    ) -> PVMTransaction {
        self.perf_mon.borrow_mut().tick(self);
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

struct PerfMon {
    events: i64,
    last_rep: Instant,
    out_file: File,
}

impl PerfMon {
    fn new() -> Self {
        PerfMon {
            events: 0,
            last_rep: Instant::now(),
            out_file: File::create("./perfinfo").unwrap(),
        }
    }

    fn tick(&mut self, pvm: &PVM) {
        self.events += 1;
        if (self.events % 10_000) == 0 {
            let ns_per_ev = (self.last_rep.elapsed() / 10_000).as_nanos();
            writeln!(self.out_file, "Event No: {}", self.events).unwrap();
            writeln!(self.out_file, "ns per event: {}", ns_per_ev).unwrap();

            if ns_per_ev < 1_000_000 {
                let ev_per_s = 1_000_000_000 / ns_per_ev;
                writeln!(self.out_file, "ev per second: {}", ev_per_s).unwrap();
            } else {
                let us_per_ev: u32 = (ns_per_ev / 1_000) as u32;
                let ev_per_s = 1_000_000.0_f64 / f64::from(us_per_ev);
                writeln!(self.out_file, "ev per second: {:0.2}", ev_per_s).unwrap();
            }
            writeln!(
                self.out_file,
                "Uuid_cache: {}",
                to_human_bytes(size_of_hm(&pvm.uuid_cache), true)
            )
            .unwrap();
            writeln!(
                self.out_file,
                "Node_cache: {}",
                to_human_bytes(size_of_ll(&pvm.node_cache), true)
            )
            .unwrap();
            writeln!(
                self.out_file,
                "Rel_src_dst_cache: {}",
                to_human_bytes(size_of_hm(&pvm.rel_src_dst_cache), true)
            )
            .unwrap();
            writeln!(
                self.out_file,
                "Rel_cache: {}",
                to_human_bytes(size_of_ll(&pvm.rel_cache), true)
            )
            .unwrap();
            writeln!(
                self.out_file,
                "Open_cache: {}",
                to_human_bytes((pvm.open_cache.capacity() * 8) as u64, true)
            )
            .unwrap();
            writeln!(
                self.out_file,
                "Name_cache: {}",
                to_human_bytes(size_of_ll(&pvm.name_cache), true)
            )
            .unwrap();
            self.out_file.flush().unwrap();
            self.out_file.seek(SeekFrom::Start(0)).unwrap();
            self.last_rep = Instant::now();
        }
    }
}
