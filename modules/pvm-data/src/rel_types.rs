use crate::{Denumerate, Enumerable, HasDst, HasID, HasSrc, RelGenerable, ID};

#[derive(Clone, Copy, Debug)]
pub enum PVMOps {
    Source,
    Sink,
    Connect,
    Version,
    Unknown,
}

#[derive(Clone, Debug)]
pub struct Inf {
    id: ID,
    src: ID,
    dst: ID,
    pub pvm_op: PVMOps,
    pub ctx: ID,
    pub byte_count: i64,
}

#[derive(Debug)]
pub struct InfInit {
    pub pvm_op: PVMOps,
    pub ctx: ID,
    pub byte_count: i64,
}

impl HasID for Inf {
    fn get_db_id(&self) -> ID {
        self.id
    }
}

impl HasSrc for Inf {
    fn get_src(&self) -> ID {
        self.src
    }
}

impl HasDst for Inf {
    fn get_dst(&self) -> ID {
        self.dst
    }
}

impl RelGenerable for Inf {
    type Init = InfInit;

    fn new(id: ID, src: ID, dst: ID, init: Self::Init) -> Self {
        Inf {
            id,
            src,
            dst,
            pvm_op: init.pvm_op,
            ctx: init.ctx,
            byte_count: init.byte_count,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Named {
    id: ID,
    src: ID,
    dst: ID,
    pub start: ID,
    pub end: ID,
}

#[derive(Debug)]
pub struct NamedInit {
    pub start: ID,
    pub end: ID,
}

impl HasID for Named {
    fn get_db_id(&self) -> ID {
        self.id
    }
}

impl HasSrc for Named {
    fn get_src(&self) -> ID {
        self.src
    }
}

impl HasDst for Named {
    fn get_dst(&self) -> ID {
        self.dst
    }
}

impl RelGenerable for Named {
    type Init = NamedInit;

    fn new(id: ID, src: ID, dst: ID, init: Self::Init) -> Self {
        Named {
            id,
            src,
            dst,
            start: init.start,
            end: init.end,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Rel {
    Inf(Inf),
    Named(Named),
}

impl Enumerable for Rel {
    type Target = Rel;
    fn enumerate(self) -> Rel {
        self
    }
}

macro_rules! rel_trait {
    ($TR: ty,
     $($F:ident() -> $T: ty),*) => {
        impl $TR for Rel {
            $(fn $F(&self) -> $T {
                match self {
                    Rel::Inf(i) => i.$F(),
                    Rel::Named(n) => n.$F(),
                }
            })*
        }
    }
}

rel_trait!(HasID, get_db_id() -> ID);
rel_trait!(HasSrc, get_src() -> ID);
rel_trait!(HasDst, get_dst() -> ID);

macro_rules! enum_denum {
    ($V:path, $T:ty) => {
        impl Enumerable for $T {
            type Target = Rel;
            fn enumerate(self) -> Self::Target {
                $V(self)
            }
        }

        impl Denumerate for $T {
            fn denumerate(val: &Rel) -> &Self {
                if let $V(ref ed) = *val {
                    ed
                } else {
                    panic!("{:?} is not an {}", val, stringify!($T))
                }
            }
            fn denumerate_mut(val: &mut Rel) -> &mut Self {
                if let $V(ref mut ed) = *val {
                    ed
                } else {
                    panic!("{:?} is not an {}", val, stringify!($T))
                }
            }
        }
    };
}

enum_denum!(Rel::Inf, Inf);
enum_denum!(Rel::Named, Named);
