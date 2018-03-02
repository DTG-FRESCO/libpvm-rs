use std::collections::{HashMap, HashSet};
use std::sync::mpsc::SyncSender;
use std::sync::atomic::{AtomicUsize, Ordering};

use packstream::values::Value;

use uuid::Uuid5;
use data::{Enumerable, Generable, HasID, HasUUID, NodeID};
use data::node_types::{EditSession, EnumNode, File};
use checking_store::{CheckingStore, DropGuard};

use super::db::DB;
use super::persist::DBTr;

pub type NodeGuard = DropGuard<NodeID, Box<EnumNode>>;

pub struct PVM {
    db: DB,
    uuid_cache: HashMap<Uuid5, NodeID>,
    node_cache: CheckingStore<NodeID, Box<EnumNode>>,
    id_counter: AtomicUsize,
    inf_cache: HashSet<(NodeID, NodeID)>,
    open_cache: HashMap<Uuid5, HashSet<Uuid5>>,
    pub unparsed_events: HashSet<String>,
}

impl PVM {
    pub fn new(db: SyncSender<DBTr>) -> Self {
        PVM {
            db: DB::create(db),
            uuid_cache: HashMap::new(),
            node_cache: CheckingStore::new(),
            id_counter: AtomicUsize::new(0),
            inf_cache: HashSet::new(),
            open_cache: HashMap::new(),
            unparsed_events: HashSet::new(),
        }
    }

    fn _inf<T, U>(&mut self, src: &T, dst: &U, class: &'static str)
    where
        T: HasID,
        U: HasID,
    {
        if self.inf_cache.insert((src.get_db_id(), dst.get_db_id())) {
            self.db.create_rel(src, dst, "INF", class);
        }
    }

    pub fn release(&mut self, uuid: &Uuid5) {
        self.uuid_cache.remove(uuid);
    }

    pub fn remove(&mut self, guard: NodeGuard) {
        self.node_cache.remove(guard)
    }

    pub fn checkin(&mut self, guard: NodeGuard) {
        self.node_cache.checkin(guard)
    }

    pub fn add<T>(
        &mut self,
        uuid: Uuid5,
        additional: Option<HashMap<&'static str, Value>>,
    ) -> NodeGuard
    where
        T: Generable + Enumerable,
    {
        let id = NodeID::new(self.id_counter.fetch_add(1, Ordering::SeqCst) as i64);
        let node = Box::new(T::new(id, uuid, additional).enumerate());
        self.uuid_cache.insert(uuid, id);
        self.node_cache.insert(id, node);
        let n = self.node_cache.checkout(&id).unwrap();
        self.db.create_node(&**n);
        n
    }

    pub fn declare<T>(
        &mut self,
        uuid: Uuid5,
        additional: Option<HashMap<&'static str, Value>>,
    ) -> NodeGuard
    where
        T: Generable + Enumerable,
    {
        if !self.uuid_cache.contains_key(&uuid) {
            self.add::<T>(uuid, additional)
        } else {
            self.node_cache.checkout(&self.uuid_cache[&uuid]).unwrap()
        }
    }

    pub fn source(&mut self, act: &EnumNode, ent: &EnumNode, tag: &'static str) {
        self._inf(ent, act, tag);
    }

    pub fn sink(&mut self, act: &EnumNode, ent: &EnumNode, tag: &'static str) {}

    pub fn sinkstart(&mut self, act: &EnumNode, ent: &EnumNode, tag: &'static str) {
        match *ent {
            EnumNode::File(ref fref) => {
                let es = self.add::<EditSession>(
                    fref.get_uuid(),
                    Some(hashmap!("name" => Value::from(fref.name.clone()))),
                );
                self.open_cache
                    .insert(fref.get_uuid(), hashset!(act.get_uuid()));
                self.db.create_rel(fref, &**es, "INF", "");
                self._inf(act, &**es, tag);
                self.checkin(es);
            }
            EnumNode::EditSession(ref eref) => {
                if self.open_cache
                    .get_mut(&eref.get_uuid())
                    .unwrap()
                    .insert(act.get_uuid())
                {
                    self._inf(act, eref, tag);
                }
            }
            _ => self._inf(act, ent, tag),
        }
    }

    pub fn sinkend(&mut self, act: &EnumNode, ent: &EnumNode, tag: &'static str) {
        match *ent {
            EnumNode::File(ref fref) => {
                //panic!("Calling sinkend on a File, should not occur.")
            }
            EnumNode::EditSession(ref eref) => {
                if !self.open_cache
                    .get_mut(&eref.get_uuid())
                    .unwrap()
                    .remove(&act.get_uuid())
                {
                    self._inf(act, eref, tag);
                }
                if self.open_cache[&eref.get_uuid()].len() == 0 {
                    let f = self.add::<File>(
                        eref.get_uuid(),
                        Some(hashmap!("name" => Value::from(eref.name.clone()))),
                    );
                    self.db.create_rel(eref, &**f, "INF", "");
                    self.checkin(f);
                }
            }
            _ => {}
        }
    }

    pub fn name(&mut self, obj: &mut EnumNode, name: String) {}

    pub fn unname(&mut self, act: &EnumNode, obj: &EnumNode, name: String) {}

    pub fn prop(&mut self, ent: &EnumNode) {
        self.db.update_node(ent)
    }
}
