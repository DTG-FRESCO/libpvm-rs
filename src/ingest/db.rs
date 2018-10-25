use std::sync::mpsc::SyncSender;

use data::{node_types::Node, rel_types::Rel, Enumerable};
use views::DBTr;

pub struct DB {
    persist_pipe: SyncSender<DBTr>,
}

impl DB {
    pub fn create(pipe: SyncSender<DBTr>) -> DB {
        DB { persist_pipe: pipe }
    }

    pub fn create_node<N: Enumerable<Target = Node>>(&mut self, node: N) {
        self.persist_pipe
            .send(DBTr::CreateNode(node.enumerate()))
            .expect("Database worker closed queue unexpectadly")
    }

    pub fn create_rel<R: Enumerable<Target = Rel>>(&mut self, rel: R) {
        self.persist_pipe
            .send(DBTr::CreateRel(rel.enumerate()))
            .expect("Database worker closed queue unexpectadly");
    }

    pub fn update_node<N: Enumerable<Target = Node>>(&mut self, node: N) {
        self.persist_pipe
            .send(DBTr::UpdateNode(node.enumerate()))
            .expect("Database worker closed queue unexpectadly")
    }

    pub fn update_rel<R: Enumerable<Target = Rel>>(&mut self, rel: R) {
        self.persist_pipe
            .send(DBTr::UpdateRel(rel.enumerate()))
            .expect("Database worker closed queue unexpectadly")
    }
}
