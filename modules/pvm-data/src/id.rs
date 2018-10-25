#[derive(Copy, Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct ID(u64);

impl ID {
    pub fn new(val: u64) -> ID {
        ID(val)
    }
    pub fn inner(self) -> u64 {
        self.0
    }
}
