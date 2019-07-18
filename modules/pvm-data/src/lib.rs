mod id;
mod meta_store;
pub mod node_types;
pub mod rel_types;

mod built_info {
    #![allow(dead_code)]
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

pub const fn version() -> &'static str {
    built_info::PKG_VERSION
}

pub use self::{id::ID, meta_store::MetaStore};

pub trait Enumerable {
    type Target;
    fn enumerate(self) -> Self::Target;
}

pub trait Denumerate: Enumerable {
    fn denumerate(val: &<Self as Enumerable>::Target) -> &Self;
    fn denumerate_mut(val: &mut <Self as Enumerable>::Target) -> &mut Self;
}

pub trait HasID {
    fn get_db_id(&self) -> ID;
}

pub trait HasSrc {
    fn get_src(&self) -> ID;
}

pub trait HasDst {
    fn get_dst(&self) -> ID;
}

pub trait RelGenerable: HasID + HasSrc + HasDst + Sized {
    type Init;

    fn new(id: ID, src: ID, dst: ID, init: Self::Init) -> Self;
}

impl HasID for ID {
    fn get_db_id(&self) -> ID {
        *self
    }
}

impl<T: HasID> HasID for &T {
    fn get_db_id(&self) -> ID {
        (*self).get_db_id()
    }
}

impl<'a, T> Enumerable for &'a T
where
    T: Enumerable + Clone,
{
    type Target = <T as Enumerable>::Target;
    fn enumerate(self) -> Self::Target {
        <T as Enumerable>::enumerate((*self).clone())
    }
}

impl<'a, T> Enumerable for &'a mut T
where
    T: Enumerable + Clone,
{
    type Target = <T as Enumerable>::Target;
    fn enumerate(self) -> Self::Target {
        <T as Enumerable>::enumerate((*self).clone())
    }
}
