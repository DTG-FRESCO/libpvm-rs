use std::{
    collections::{hash_map::Entry, HashMap},
    sync::{mpsc::Receiver, Arc},
    thread,
};

use crate::{
    data::ID,
    neo4j_glue::{ToDBNode, ToDBRel},
    view::*,
};

use maplit::hashmap;
use neo4j::{Neo4jDB, Neo4jOperations, Value};

const BATCH_SIZE: usize = 1000;
const TR_SIZE: usize = 100_000;

#[derive(Debug)]
pub struct Neo4JView {
    id: usize,
}

impl View for Neo4JView {
    fn new(id: usize) -> Neo4JView {
        Neo4JView { id }
    }
    fn id(&self) -> usize {
        self.id
    }
    fn name(&self) -> &'static str {
        "Neo4jView"
    }
    fn desc(&self) -> &'static str {
        "View for streaming data to a Neo4j database instance."
    }
    fn params(&self) -> HashMap<&'static str, &'static str> {
        hashmap!("addr" => "The Neo4j server address to connect to.",
                 "user" => "The username to auth with.",
                 "pass" => "The password to auth with.")
    }
    fn create(&self, id: usize, params: ViewParams, stream: Receiver<Arc<DBTr>>) -> ViewInst {
        let mut db = {
            let addr = params.get_or_def("addr", "localhost:7687");
            let user = params.get_or_def("user", "neo4j");
            let pass = params.get_or_def("pass", "opus");
            Neo4jDB::connect(addr, user, pass).unwrap()
        };
        let thr = thread::Builder::new().name("Neo4jView".to_string()).spawn(move || {
            let mut nodes = CreateNodes::new();
            let mut edges = CreateRels::new();
            let mut up_node = UpdateNodes::new();
            let mut up_rel = UpdateRels::new();
            let mut ups = 0;
            let mut btc = 0;
            let mut trs = 0;
            let mut rel_up_base = 0;
            let mut rel_up_node = 0;
            let mut rel_up_rel = 0;

            let mut tr = db.transaction();

            tr.run_unchecked("CREATE INDEX ON :Node(db_id)", HashMap::new());
            tr.run_unchecked("CREATE INDEX ON :Actor(uuid)", HashMap::new());
            tr.run_unchecked("CREATE INDEX ON :Object(uuid)", HashMap::new());
            tr.run_unchecked("CREATE INDEX ON :Store(uuid)", HashMap::new());
            tr.run_unchecked("CREATE INDEX ON :EditSession(uuid)", HashMap::new());
            tr.run_unchecked("CREATE INDEX ON :Conduit(uuid)", HashMap::new());
            tr.run_unchecked("CREATE INDEX ON :Path(path)", HashMap::new());
            tr.run_unchecked("CREATE INDEX ON :Net(addr)", HashMap::new());

            tr.commit_and_refresh().unwrap();

            tr.run_unchecked(
                "MERGE (:DBInfo {pvm_version: 2, source: $src})",
                hashmap!("src" => Value::from(format!("libPVM-{}", crate::VERSION))),
            );

            tr.commit_and_refresh().unwrap();

            for evt in stream {
                match *evt {
                    DBTr::CreateNode(ref node) => {
                        let (id, labs, props) = node.to_db();
                        nodes.add(
                            id,
                            hashmap!("labels" => labs.into(), "props"  => props.into()),
                        );
                        ups += 1;
                    }
                    DBTr::CreateRel(ref rel) => {
                        let (id, data) = rel.to_db();
                        edges.add(id, data);
                        ups += 1;
                    }
                    DBTr::UpdateNode(ref node) => {
                        let (id, _, props) = node.to_db();
                        if let Some(props) = nodes.update(id, props.into()) {
                            if up_node.add(id, props) {
                                ups += 1;
                            }
                        }
                    }
                    DBTr::UpdateRel(ref rel) => {
                        rel_up_base += 1;
                        let (id, data) = rel.to_db();
                        if let Some(data) = edges.update(id, data) {
                            rel_up_node += 1;
                            if up_rel.add(id, data) {
                                ups += 1;
                                rel_up_rel += 1;
                            }
                        }
                    }
                }
                if ups > (btc + 1) * BATCH_SIZE {
                    nodes.execute(&mut tr);
                    edges.execute(&mut tr);
                    up_node.execute(&mut tr);
                    up_rel.execute(&mut tr);
                    btc += 1;
                }
                if ups > (trs + 1) * TR_SIZE {
                    tr.commit_and_refresh().unwrap();
                    trs += 1;
                }
            }
            nodes.execute(&mut tr);
            edges.execute(&mut tr);
            up_node.execute(&mut tr);
            up_rel.execute(&mut tr);
            println!("Final Commit");
            tr.commit().unwrap();
            trs += 1;
            println!("Neo4J Updates Issued: {}", ups);
            println!("Neo4J Batches Issued: {}", btc * 4);
            println!("Neo4J Transactions Issued: {}", trs);
            println!("Rel Updates: {}, Absorbed into Nodes: {}, Absorbed into other updates: {}, Finally executed: {}", rel_up_base, rel_up_base - rel_up_node, rel_up_node - rel_up_rel, rel_up_rel);
        }).unwrap();
        ViewInst {
            id,
            vtype: self.id,
            params,
            handle: thr,
        }
    }
}

struct CreateNodes {
    nodes: HashMap<ID, HashMap<&'static str, Value>>,
}

impl CreateNodes {
    fn new() -> Self {
        CreateNodes {
            nodes: HashMap::new(),
        }
    }
    fn execute(&mut self, db: &mut impl Neo4jOperations) {
        let nodes: Value = self.nodes.drain().map(|(_k, v)| v).collect();
        self._execute(db, nodes);
    }
    fn _execute(&mut self, db: &mut impl Neo4jOperations, nodes: Value) {
        db.run_unchecked(
            "UNWIND $nodes AS n
             CALL apoc.create.node(n.labels, n.props) YIELD node
             RETURN 0",
            hashmap!("nodes" => nodes),
        );
    }
    fn add(&mut self, id: ID, data: HashMap<&'static str, Value>) {
        self.nodes.insert(id, data);
    }
    fn update(&mut self, id: ID, props: Value) -> Option<Value> {
        match self.nodes.entry(id) {
            Entry::Occupied(mut ent) => {
                ent.get_mut().insert("props", props);
                None
            }
            Entry::Vacant(_) => Some(props),
        }
    }
}

struct CreateRels {
    rels: HashMap<ID, Value>,
}

impl CreateRels {
    fn new() -> Self {
        CreateRels {
            rels: HashMap::new(),
        }
    }
    fn execute(&mut self, db: &mut impl Neo4jOperations) {
        let rels: Value = self.rels.drain().map(|(_k, v)| v).collect();
        db.run_unchecked(
            "UNWIND $rels AS r
             MATCH (s:Node {db_id: r.src}),
                   (d:Node {db_id: r.dst})
             CALL apoc.create.relationship(s, r.type, r.props, d) YIELD rel
             RETURN 0",
            hashmap!("rels" => rels),
        );
    }
    fn add(&mut self, id: ID, data: Value) {
        self.rels.insert(id, data);
    }
    fn update(&mut self, id: ID, data: Value) -> Option<Value> {
        if self.rels.contains_key(&id) {
            self.rels.insert(id, data);
            None
        } else {
            Some(data)
        }
    }
}

struct UpdateNodes {
    props: HashMap<ID, Value>,
}

impl UpdateNodes {
    fn new() -> Self {
        UpdateNodes {
            props: HashMap::new(),
        }
    }
    fn execute(&mut self, db: &mut impl Neo4jOperations) {
        let nodes: Value = self.props.drain().map(|(_k, v)| v).collect();
        db.run_unchecked(
            "UNWIND $upds AS props
             MATCH (p:Node {db_id: props.db_id})
             SET p += props",
            hashmap!("upds" => nodes),
        );
    }
    fn add(&mut self, id: ID, value: Value) -> bool {
        self.props.insert(id, value).is_none()
    }
}

struct UpdateRels {
    props: HashMap<ID, Value>,
}

impl UpdateRels {
    fn new() -> Self {
        UpdateRels {
            props: HashMap::new(),
        }
    }
    fn execute(&mut self, db: &mut impl Neo4jOperations) {
        let rels: Value = self.props.drain().map(|(_k, v)| v).collect();
        db.run_unchecked(
            "UNWIND $upds AS up
             MATCH (s:Node {db_id: up.src})-[r {db_id: up.id}]->()
             SET r += up.props",
            hashmap!("upds" => rels),
        );
    }
    fn add(&mut self, id: ID, value: Value) -> bool {
        self.props.insert(id, value).is_none()
    }
}
