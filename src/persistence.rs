use std::{
    collections::HashMap,
};

use lending_library::{LendingLibrary, Loan};
use uuid::Uuid;

use data::{node_types::Node, rel_types::Rel, ID};
use id_counter::IDCounter;

pub struct Persistence {
    node_cache: LendingLibrary<ID, Node>,
    rel_cache: LendingLibrary<ID, Rel>,
    uuid_cache: HashMap<Uuid, ID>,
    rel_src_dst_cache: HashMap<(&'static str, ID, ID), ID>,
    id: IDCounter,
}

impl Persistence {
    pub fn node(&mut self, id: ID) -> Loan<ID, Node> {
        self.node_cache.lend(&id).unwrap()
    }

    pub fn rel(&mut self, id: ID) -> Loan<ID, Rel> {
        self.rel_cache.lend(&id).unwrap()
    }

    pub fn id_snap(&mut self) -> IDCounter {
        self.id.snapshot()
    }
}
