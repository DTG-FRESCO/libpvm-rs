use std::{mem::swap, sync::mpsc::SyncSender};

use crate::{
    data::{node_types::Node, rel_types::Rel, Enumerable, HasID},
    view::DBTr,
};

pub struct DB {
    persist_pipe: SyncSender<DBTr>,
}

impl DB {
    pub fn create(pipe: SyncSender<DBTr>) -> DB {
        DB { persist_pipe: pipe }
    }

    pub fn store(&mut self) -> DBStore {
        DBStore {
            inner: self,
            ops: Vec::new(),
        }
    }

    pub fn create_node<N: Enumerable<Target = Node>>(&mut self, node: N) {
        self.persist_pipe
            .send(DBTr::CreateNode(node.enumerate()))
            .expect("Database worker closed queue unexpectadly")
    }

    fn op(&mut self, op: DBTr) {
        self.persist_pipe
            .send(op)
            .expect("Database worker closed queue unexpectadly")
    }
}

pub struct DBStore<'a> {
    inner: &'a mut DB,
    ops: Vec<DBTr>,
}

impl<'a> DBStore<'a> {
    pub fn create_node<N: Enumerable<Target = Node>>(&mut self, node: N) {
        self.insert(DBTr::CreateNode(node.enumerate()));
    }

    pub fn _create_node_head<N: Enumerable<Target = Node>>(&mut self, node: N) {
        self.ops.insert(0, DBTr::CreateNode(node.enumerate()));
    }

    pub fn create_rel<R: Enumerable<Target = Rel>>(&mut self, rel: R) {
        self.insert(DBTr::CreateRel(rel.enumerate()));
    }

    pub fn update_node<N: Enumerable<Target = Node>>(&mut self, node: N) {
        self.insert(DBTr::UpdateNode(node.enumerate()));
    }

    pub fn update_rel<R: Enumerable<Target = Rel>>(&mut self, rel: R) {
        self.insert(DBTr::UpdateRel(rel.enumerate()));
    }

    fn insert(&mut self, mut op: DBTr) {
        for rop in &mut self.ops {
            match rop {
                DBTr::CreateNode(cur) => match &mut op {
                    DBTr::CreateNode(new) | DBTr::UpdateNode(new) => {
                        if cur.get_db_id() == new.get_db_id() {
                            swap(cur, new);
                            return;
                        }
                    }
                    _ => {}
                },
                DBTr::UpdateNode(cur) => match &mut op {
                    DBTr::CreateNode(new) => {
                        if cur.get_db_id() == new.get_db_id() {
                            unreachable!();
                        }
                    }
                    DBTr::UpdateNode(new) => {
                        if cur.get_db_id() == new.get_db_id() {
                            swap(cur, new);
                            return;
                        }
                    }
                    _ => {}
                },
                DBTr::CreateRel(cur) => match &mut op {
                    DBTr::CreateRel(new) | DBTr::UpdateRel(new) => {
                        if cur.get_db_id() == new.get_db_id() {
                            swap(cur, new);
                            return;
                        }
                    }
                    _ => {}
                },
                DBTr::UpdateRel(cur) => match &mut op {
                    DBTr::CreateRel(new) => {
                        if cur.get_db_id() == new.get_db_id() {
                            unreachable!();
                        }
                    }
                    DBTr::UpdateRel(new) => {
                        if cur.get_db_id() == new.get_db_id() {
                            swap(cur, new);
                            return;
                        }
                    }
                    _ => {}
                },
            }
        }
        self.ops.push(op);
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn commit(self) {
        for op in self.ops {
            self.inner.op(op)
        }
    }
}
