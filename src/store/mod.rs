use std::collections::HashMap;

// mod rocksdb;
// pub use self::rocksdb::RocksdbStore;
mod sled;
pub use self::sled::SledStore;

#[derive(Debug)]
pub enum Error {
    DBError(String),
}

impl ToString for Error {
    fn to_string(&self) -> String {
        match self {
            Error::DBError(err) => err.to_owned(),
        }
    }
}

pub type IteratorItem = (Vec<u8>, Vec<u8>);

pub enum IteratorDirection {
    Forward,
    Reverse,
}

pub trait Store {
    type Batch: Batch;
    fn new(path: &str) -> Self;
    fn get<K: AsRef<[u8]>>(&self, key: K) -> Result<Option<Vec<u8>>, Error>;
    fn exists<K: AsRef<[u8]>>(&self, key: K) -> Result<bool, Error>;
    fn iter<K: AsRef<[u8]>>(
        &self,
        from_key: K,
        direction: IteratorDirection,
    ) -> Result<Box<dyn Iterator<Item = IteratorItem>>, Error>;
    fn batch(&self) -> Result<Self::Batch, Error>;

    // returns key_prefix => (entries, totol size, total value size)
    fn statistics(&self) -> Result<HashMap<u8, (usize, usize, usize)>, Error> {
        let iter = self.iter(&[], IteratorDirection::Forward)?;
        let mut statistics: HashMap<u8, (usize, usize, usize)> = HashMap::new();
        for (key, value) in iter {
            let s = statistics.entry(*key.first().unwrap()).or_default();
            s.0 += 1;
            s.1 += key.len();
            s.2 += value.len();
        }
        Ok(statistics)
    }
}

pub trait Batch {
    fn put_kv<K: Into<Vec<u8>>, V: Into<Vec<u8>>>(
        &mut self,
        key: K,
        value: V,
    ) -> Result<(), Error> {
        self.put(&Into::<Vec<u8>>::into(key), &Into::<Vec<u8>>::into(value))
    }

    fn put<K: AsRef<[u8]>, V: AsRef<[u8]>>(&mut self, key: K, value: V) -> Result<(), Error>;
    fn delete<K: AsRef<[u8]>>(&mut self, key: K) -> Result<(), Error>;
    fn commit(self) -> Result<(), Error>;
}
