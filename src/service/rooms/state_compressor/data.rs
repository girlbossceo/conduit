use std::{collections::HashSet, mem::size_of, sync::Arc};

use conduit::{checked, utils, Error, Result};
use database::{Database, Map};

use super::CompressedStateEvent;

pub(super) struct StateDiff {
	pub(super) parent: Option<u64>,
	pub(super) added: Arc<HashSet<CompressedStateEvent>>,
	pub(super) removed: Arc<HashSet<CompressedStateEvent>>,
}

pub(super) struct Data {
	shortstatehash_statediff: Arc<Map>,
}

impl Data {
	pub(super) fn new(db: &Arc<Database>) -> Self {
		Self {
			shortstatehash_statediff: db["shortstatehash_statediff"].clone(),
		}
	}

	pub(super) fn get_statediff(&self, shortstatehash: u64) -> Result<StateDiff> {
		let value = self
			.shortstatehash_statediff
			.get(&shortstatehash.to_be_bytes())?
			.ok_or_else(|| Error::bad_database("State hash does not exist"))?;
		let parent = utils::u64_from_bytes(&value[0..size_of::<u64>()]).expect("bytes have right length");
		let parent = if parent != 0 {
			Some(parent)
		} else {
			None
		};

		let mut add_mode = true;
		let mut added = HashSet::new();
		let mut removed = HashSet::new();

		let stride = size_of::<u64>();
		let mut i = stride;
		while let Some(v) = value.get(i..checked!(i + 2 * stride)?) {
			if add_mode && v.starts_with(&0_u64.to_be_bytes()) {
				add_mode = false;
				i = checked!(i + stride)?;
				continue;
			}
			if add_mode {
				added.insert(v.try_into().expect("we checked the size above"));
			} else {
				removed.insert(v.try_into().expect("we checked the size above"));
			}
			i = checked!(i + 2 * stride)?;
		}

		Ok(StateDiff {
			parent,
			added: Arc::new(added),
			removed: Arc::new(removed),
		})
	}

	pub(super) fn save_statediff(&self, shortstatehash: u64, diff: &StateDiff) -> Result<()> {
		let mut value = diff.parent.unwrap_or(0).to_be_bytes().to_vec();
		for new in diff.added.iter() {
			value.extend_from_slice(&new[..]);
		}

		if !diff.removed.is_empty() {
			value.extend_from_slice(&0_u64.to_be_bytes());
			for removed in diff.removed.iter() {
				value.extend_from_slice(&removed[..]);
			}
		}

		self.shortstatehash_statediff
			.insert(&shortstatehash.to_be_bytes(), &value)
	}
}
