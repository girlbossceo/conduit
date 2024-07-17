use conduit::{err, Result};

#[inline]
pub(crate) fn result<T>(r: std::result::Result<T, rocksdb::Error>) -> Result<T, conduit::Error> {
	r.map_or_else(or_else, and_then)
}

#[inline(always)]
pub(crate) fn and_then<T>(t: T) -> Result<T, conduit::Error> { Ok(t) }

pub(crate) fn or_else<T>(e: rocksdb::Error) -> Result<T, conduit::Error> { Err(map_err(e)) }

pub(crate) fn map_err(e: rocksdb::Error) -> conduit::Error {
	let string = e.into_string();
	err!(Database(error!("{string}")))
}
