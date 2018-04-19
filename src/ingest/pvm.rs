use std::{collections::{HashMap, HashSet},
          sync::{atomic::{AtomicUsize, Ordering},
                 mpsc::SyncSender}};

use checking_store::{CheckingStore, DropGuard};
use data::{node_types::{EditInit, EditSession, EnumNode, File, FileInit},
           Enumerable,
           Generable,
           HasID,
           HasUUID,
           PVMOps,
           Rel,
           ID};
use uuid::Uuid5;

use super::{db::DB, persist::DBTr};

pub enum ConnectDir {
    Mono,
    BiDirectional,
}

pub type NodeGuard = DropGuard<ID, Box<EnumNode>>;
pub type RelGuard = DropGuard<ID, Rel>;

pub struct PVM {
    db: DB,
    uuid_cache: HashMap<Uuid5, ID>,
    node_cache: CheckingStore<ID, Box<EnumNode>>,
    rel_cache: CheckingStore<ID, Rel>,
    id_counter: AtomicUsize,
    inf_cache: HashMap<(ID, ID), ID>,
    open_cache: HashMap<Uuid5, HashSet<Uuid5>>,
    pub unparsed_events: HashSet<String>,
}

impl PVM {
    pub fn new(db: SyncSender<DBTr>) -> Self {
        PVM {
            db: DB::create(db),
            uuid_cache: HashMap::new(),
            node_cache: CheckingStore::new(),
            rel_cache: CheckingStore::new(),
            id_counter: AtomicUsize::new(0),
            inf_cache: HashMap::new(),
            open_cache: HashMap::new(),
            unparsed_events: HashSet::new(),
        }
    }

    pub fn release(&mut self, uuid: &Uuid5) {
        if let Some(nid) = self.uuid_cache.remove(uuid) {
            self.node_cache.remove(nid);
        }
    }

    fn _nextid(&mut self) -> ID {
        ID::new(self.id_counter.fetch_add(1, Ordering::Relaxed) as i64)
    }

    fn _inf<T, U>(&mut self, src: &T, dst: &U, pvm_op: PVMOps, call: &str) -> RelGuard
    where
        T: HasID,
        U: HasID,
    {
        let id_pair = (src.get_db_id(), dst.get_db_id());
        if self.inf_cache.contains_key(&id_pair) {
            self.rel_cache.checkout(self.inf_cache[&id_pair]).unwrap()
        } else {
            let id = self._nextid();
            let rel = Rel::Inf {
                id,
                src: id_pair.0,
                dst: id_pair.1,
                pvm_op,
                generating_call: call.to_string(),
                byte_count: 0,
            };
            self.rel_cache.insert(id, rel);
            self.inf_cache.insert(id_pair, id);
            let r = self.rel_cache.checkout(id).unwrap();
            self.db.create_rel(&r);
            r
        }
    }

    pub fn add<T>(&mut self, uuid: Uuid5, init: Option<T::Init>) -> NodeGuard
    where
        T: Generable + Enumerable,
    {
        let id = self._nextid();
        let node = Box::new(T::new(id, uuid, init).enumerate());
        if let Some(nid) = self.uuid_cache.insert(uuid, id) {
            self.node_cache.remove(nid);
        }
        self.node_cache.insert(id, node);
        let n = self.node_cache.checkout(id).unwrap();
        self.db.create_node(&**n);
        n
    }

    pub fn declare<T>(&mut self, uuid: Uuid5, init: Option<T::Init>) -> NodeGuard
    where
        T: Generable + Enumerable,
    {
        if !self.uuid_cache.contains_key(&uuid) {
            self.add::<T>(uuid, init)
        } else {
            self.node_cache.checkout(self.uuid_cache[&uuid]).unwrap()
        }
    }

    pub fn source(&mut self, act: &EnumNode, ent: &EnumNode, tag: &str) -> RelGuard {
        self._inf(ent, act, PVMOps::Source, tag)
    }

    pub fn sink(&mut self, act: &EnumNode, ent: &EnumNode, tag: &str) -> RelGuard {
        match *ent {
            EnumNode::File(ref fref) => {
                let f = self.add::<File>(
                    fref.get_uuid(),
                    Some(FileInit {
                        name: fref.name.clone(),
                    }),
                );
                self._inf(fref, &**f, PVMOps::Version, tag);
                self._inf(act, &**f, PVMOps::Sink, tag)
            }
            _ => self._inf(act, ent, PVMOps::Sink, tag),
        }
    }

    pub fn sinkstart(&mut self, act: &EnumNode, ent: &EnumNode, tag: &str) -> RelGuard {
        match *ent {
            EnumNode::File(ref fref) => {
                let es = self.add::<EditSession>(
                    fref.get_uuid(),
                    Some(EditInit {
                        name: fref.name.clone(),
                    }),
                );
                self.open_cache
                    .insert(fref.get_uuid(), hashset!(act.get_uuid()));
                self._inf(fref, &**es, PVMOps::Version, tag);
                self._inf(act, &**es, PVMOps::Sink, tag)
            }
            EnumNode::EditSession(ref eref) => {
                self.open_cache
                    .get_mut(&eref.get_uuid())
                    .unwrap()
                    .insert(act.get_uuid());
                self._inf(act, eref, PVMOps::Sink, tag)
            }
            _ => self._inf(act, ent, PVMOps::Sink, tag),
        }
    }

    pub fn sinkend(&mut self, act: &EnumNode, ent: &EnumNode, tag: &str) {
        if let EnumNode::EditSession(ref eref) = *ent {
            self.open_cache
                .get_mut(&eref.get_uuid())
                .unwrap()
                .remove(&act.get_uuid());
            if self.open_cache[&eref.get_uuid()].is_empty() {
                let f = self.add::<File>(
                    eref.get_uuid(),
                    Some(FileInit {
                        name: eref.name.clone(),
                    }),
                );
                self._inf(eref, &**f, PVMOps::Version, tag);
            }
        }
    }

    pub fn name(&mut self, obj: &mut EnumNode, name: String) {
        match *obj {
            EnumNode::File(ref mut fref) => {
                if fref.name == "" {
                    fref.name = name;
                    self.db.update_node(fref);
                }
            }
            EnumNode::EditSession(ref mut eref) => {
                if eref.name == "" {
                    eref.name = name;
                    self.db.update_node(eref);
                }
            }
            _ => {}
        }
    }

    pub fn prop_node(&mut self, ent: &EnumNode) {
        self.db.update_node(ent)
    }

    pub fn prop_rel(&mut self, ent: &Rel) {
        self.db.update_rel(ent)
    }

    pub fn connect(&mut self, first: &EnumNode, second: &EnumNode, dir: ConnectDir, tag: &str) {
        self._inf(first, second, PVMOps::Connect, tag);
        if let ConnectDir::BiDirectional = dir {
            self._inf(second, first, PVMOps::Connect, tag);
        }
    }

    pub fn shutdown(self) {}
}
