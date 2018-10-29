use std::{borrow::Cow, collections::HashMap};

use super::{node_types::ConcreteType, ID};

type MetaEntry = (String, ID);

type MetaType = (bool, Vec<MetaEntry>);

#[derive(Clone, Deserialize, Debug, Default, Serialize)]
pub struct MetaStore {
    entries: HashMap<Cow<'static, str>, MetaType>,
}

impl MetaStore {
    pub fn new() -> Self {
        MetaStore {
            entries: HashMap::new(),
        }
    }

    pub fn from_map(
        src: HashMap<&'static str, String>,
        ctx: ID,
        ty: &'static ConcreteType,
    ) -> Self {
        MetaStore {
            entries: src
                .into_iter()
                .map(|(k, v)| (k.into(), (ty.props[k], vec![(v, ctx)])))
                .collect(),
        }
    }

    pub fn snapshot(&self, ctx: ID) -> Self {
        let entries: HashMap<Cow<'static, str>, MetaType> = self
            .entries
            .iter()
            .filter_map(|(n, (h, v))| {
                if *h {
                    let (v_last, _) = v.last().unwrap();
                    Some((n.clone(), (true, vec![(v_last.clone(), ctx)])))
                } else {
                    None
                }
            })
            .collect();
        MetaStore { entries }
    }

    pub fn merge(&mut self, other: &MetaStore) {
        for (key, val, ctx, heritable) in other.iter() {
            self.update(key.to_string(), val, ctx, heritable);
        }
    }

    pub fn update<K: Into<Cow<'static, str>>, T: ToString + ?Sized>(
        &mut self,
        key: K,
        val: &T,
        ctx: ID,
        heritable: bool,
    ) {
        let cow_key = key.into();
        let str_val = val.to_string();
        if let Some(v) = self.cur(&cow_key) {
            if v == str_val {
                return;
            }
        }
        let entry = (str_val, ctx);
        self.entries
            .entry(cow_key)
            .or_insert((heritable, Vec::new()))
            .1
            .push(entry);
    }

    pub fn cur(&self, key: &str) -> Option<&str> {
        self.entries
            .get(key)
            .map(|(_h, v)| &v[v.len() - 1])
            .map(|(v, _t)| &v[..])
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str, ID, bool)> {
        self.entries
            .iter()
            .flat_map(move |(k, (h, v))| v.iter().map(move |(s, ctx)| (&k[..], &s[..], *ctx, *h)))
    }

    pub fn iter_latest(&self) -> impl Iterator<Item = (&str, &str, ID, bool)> {
        self.entries.iter().map(move |(k, (h, v))| {
            let (s, ctx) = v.last().unwrap();
            (&k[..], &s[..], *ctx, *h)
        })
    }
}
