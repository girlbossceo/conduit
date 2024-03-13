use std::{future::Future, pin::Pin, sync::Arc};

use super::Config;
use crate::Result;

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "rocksdb")]
pub(crate) mod rocksdb;

#[cfg(any(feature = "sqlite", feature = "rocksdb"))]
pub(crate) mod watchers;

pub(crate) trait KeyValueDatabaseEngine: Send + Sync {
	fn open(config: &Config) -> Result<Self>
	where
		Self: Sized;
	fn open_tree(&self, name: &'static str) -> Result<Arc<dyn KvTree>>;
	fn flush(&self) -> Result<()>;
	fn cleanup(&self) -> Result<()> { Ok(()) }
	fn memory_usage(&self) -> Result<String> {
		Ok("Current database engine does not support memory usage reporting.".to_owned())
	}

	#[allow(dead_code)]
	fn clear_caches(&self) {}
}

pub(crate) trait KvTree: Send + Sync {
	fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

	#[cfg(feature = "rocksdb")]
	fn multi_get(
		&self, iter: Vec<(&Arc<rust_rocksdb::BoundColumnFamily<'_>>, Vec<u8>)>,
	) -> Vec<std::result::Result<Option<Vec<u8>>, rust_rocksdb::Error>>;

	fn insert(&self, key: &[u8], value: &[u8]) -> Result<()>;
	fn insert_batch(&self, iter: &mut dyn Iterator<Item = (Vec<u8>, Vec<u8>)>) -> Result<()>;

	fn remove(&self, key: &[u8]) -> Result<()>;

	fn iter<'a>(&'a self) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>;

	fn iter_from<'a>(&'a self, from: &[u8], backwards: bool) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>;

	fn increment(&self, key: &[u8]) -> Result<Vec<u8>>;
	fn increment_batch(&self, iter: &mut dyn Iterator<Item = Vec<u8>>) -> Result<()>;

	fn scan_prefix<'a>(&'a self, prefix: Vec<u8>) -> Box<dyn Iterator<Item = (Vec<u8>, Vec<u8>)> + 'a>;

	fn watch_prefix<'a>(&'a self, prefix: &[u8]) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>;

	fn clear(&self) -> Result<()> {
		for (key, _) in self.iter() {
			self.remove(&key)?;
		}

		Ok(())
	}
}
