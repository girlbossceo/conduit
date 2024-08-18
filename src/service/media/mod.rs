mod data;
mod preview;
mod remote;
mod tests;
mod thumbnail;

use std::{path::PathBuf, sync::Arc, time::SystemTime};

use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use conduit::{debug, debug_error, err, error, trace, utils, utils::MutexMap, Err, Result, Server};
use data::{Data, Metadata};
use ruma::{http_headers::ContentDisposition, Mxc, OwnedMxcUri, UserId};
use tokio::{
	fs,
	io::{AsyncReadExt, AsyncWriteExt, BufReader},
};

use crate::{client, globals, sending, Dep};

#[derive(Debug)]
pub struct FileMeta {
	pub content: Option<Vec<u8>>,
	pub content_type: Option<String>,
	pub content_disposition: Option<ContentDisposition>,
}

pub struct Service {
	url_preview_mutex: MutexMap<String, ()>,
	pub(crate) db: Data,
	services: Services,
}

struct Services {
	server: Arc<Server>,
	client: Dep<client::Service>,
	globals: Dep<globals::Service>,
	sending: Dep<sending::Service>,
}

/// generated MXC ID (`media-id`) length
pub const MXC_LENGTH: usize = 32;

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			url_preview_mutex: MutexMap::new(),
			db: Data::new(args.db),
			services: Services {
				server: args.server.clone(),
				client: args.depend::<client::Service>("client"),
				globals: args.depend::<globals::Service>("globals"),
				sending: args.depend::<sending::Service>("sending"),
			},
		}))
	}

	async fn worker(self: Arc<Self>) -> Result<()> {
		self.create_media_dir().await?;

		Ok(())
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Uploads a file.
	pub async fn create(
		&self, mxc: &Mxc<'_>, user: Option<&UserId>, content_disposition: Option<&ContentDisposition>,
		content_type: Option<&str>, file: &[u8],
	) -> Result<()> {
		// Width, Height = 0 if it's not a thumbnail
		let key = self
			.db
			.create_file_metadata(mxc, user, 0, 0, content_disposition, content_type)?;

		//TODO: Dangling metadata in database if creation fails
		let mut f = self.create_media_file(&key).await?;
		f.write_all(file).await?;

		Ok(())
	}

	/// Deletes a file in the database and from the media directory via an MXC
	pub async fn delete(&self, mxc: &Mxc<'_>) -> Result<()> {
		if let Ok(keys) = self.db.search_mxc_metadata_prefix(mxc) {
			for key in keys {
				trace!(?mxc, ?key, "Deleting from filesystem");
				if let Err(e) = self.remove_media_file(&key).await {
					error!(?mxc, ?key, "Failed to remove media file: {e}");
				}

				trace!(?mxc, ?key, "Deleting from database");
				if let Err(e) = self.db.delete_file_mxc(mxc) {
					error!(?mxc, ?key, "Failed to remove media from database: {e}");
				}
			}

			Ok(())
		} else {
			Err!(Database(error!(
				"Failed to find any media keys for MXC {mxc:?} in our database."
			)))
		}
	}

	/// Downloads a file.
	pub async fn get(&self, mxc: &Mxc<'_>) -> Result<Option<FileMeta>> {
		if let Ok(Metadata {
			content_disposition,
			content_type,
			key,
		}) = self.db.search_file_metadata(mxc, 0, 0)
		{
			let mut content = Vec::new();
			let path = self.get_media_file(&key);
			BufReader::new(fs::File::open(path).await?)
				.read_to_end(&mut content)
				.await?;

			Ok(Some(FileMeta {
				content: Some(content),
				content_type,
				content_disposition,
			}))
		} else {
			Ok(None)
		}
	}

	/// Deletes all remote only media files in the given at or after
	/// time/duration. Returns a u32 with the amount of media files deleted.
	pub async fn delete_all_remote_media_at_after_time(&self, time: String, force: bool) -> Result<usize> {
		let all_keys = self.db.get_all_media_keys();

		let user_duration: SystemTime = match cyborgtime::parse_duration(&time) {
			Err(e) => return Err!(Database(error!("Failed to parse specified time duration: {e}"))),
			Ok(duration) => SystemTime::now()
				.checked_sub(duration)
				.ok_or(err!(Arithmetic("Duration {duration:?} is too large")))?,
		};

		let mut remote_mxcs: Vec<String> = vec![];

		for key in all_keys {
			debug!("Full MXC key from database: {key:?}");

			// we need to get the MXC URL from the first part of the key (the first 0xff /
			// 255 push). this is all necessary because of conduit using magic keys for
			// media
			let mut parts = key.split(|&b| b == 0xFF);
			let mxc = parts
				.next()
				.map(|bytes| {
					utils::string_from_bytes(bytes)
						.map_err(|e| err!(Database(error!("Failed to parse MXC unicode bytes from our database: {e}"))))
				})
				.transpose()?;

			let Some(mxc_s) = mxc else {
				return Err!(Database("Parsed MXC URL unicode bytes from database but still is None"));
			};

			debug!("Parsed MXC key to URL: {mxc_s}");
			let mxc = OwnedMxcUri::from(mxc_s);
			if mxc.server_name() == Ok(self.services.globals.server_name()) {
				debug!("Ignoring local media MXC: {mxc}");
				// ignore our own MXC URLs as this would be local media.
				continue;
			}

			let path = self.get_media_file(&key);
			debug!("MXC path: {path:?}");

			let file_metadata = fs::metadata(path.clone()).await?;
			debug!("File metadata: {file_metadata:?}");

			let file_created_at = match file_metadata.created() {
				Ok(value) => value,
				Err(err) if err.kind() == std::io::ErrorKind::Unsupported => {
					debug!("btime is unsupported, using mtime instead");
					file_metadata.modified()?
				},
				Err(err) => {
					if force {
						error!("Could not delete MXC path {path:?}: {err:?}. Skipping...");
						continue;
					}
					return Err(err.into());
				},
			};

			debug!("File created at: {file_created_at:?}");
			if file_created_at <= user_duration {
				debug!("File is within user duration, pushing to list of file paths and keys to delete.");
				remote_mxcs.push(mxc.to_string());
			}
		}

		debug!(
			"Finished going through all our media in database for eligible keys to delete, checking if these are empty"
		);
		if remote_mxcs.is_empty() {
			return Err!(Database("Did not found any eligible MXCs to delete."));
		}

		debug!("Deleting media now in the past {user_duration:?}.");
		let mut deletion_count: usize = 0;
		for mxc in remote_mxcs {
			let mxc: Mxc<'_> = mxc.as_str().try_into()?;
			debug!("Deleting MXC {mxc} from database and filesystem");
			self.delete(&mxc).await?;
			deletion_count = deletion_count.saturating_add(1);
		}

		Ok(deletion_count)
	}

	pub async fn create_media_dir(&self) -> Result<()> {
		let dir = self.get_media_dir();
		Ok(fs::create_dir_all(dir).await?)
	}

	async fn remove_media_file(&self, key: &[u8]) -> Result<()> {
		let path = self.get_media_file(key);
		let legacy = self.get_media_file_b64(key);
		debug!(?key, ?path, ?legacy, "Removing media file");

		let file_rm = fs::remove_file(&path);
		let legacy_rm = fs::remove_file(&legacy);
		let (file_rm, legacy_rm) = tokio::join!(file_rm, legacy_rm);
		if let Err(e) = legacy_rm {
			if self.services.server.config.media_compat_file_link {
				debug_error!(?key, ?legacy, "Failed to remove legacy media symlink: {e}");
			}
		}

		Ok(file_rm?)
	}

	async fn create_media_file(&self, key: &[u8]) -> Result<fs::File> {
		let path = self.get_media_file(key);
		debug!(?key, ?path, "Creating media file");

		let file = fs::File::create(&path).await?;
		if self.services.server.config.media_compat_file_link {
			let legacy = self.get_media_file_b64(key);
			if let Err(e) = fs::symlink(&path, &legacy).await {
				debug_error!(
					key = ?encode_key(key), ?path, ?legacy,
					"Failed to create legacy media symlink: {e}"
				);
			}
		}

		Ok(file)
	}

	#[inline]
	#[must_use]
	pub fn get_media_file(&self, key: &[u8]) -> PathBuf { self.get_media_file_sha256(key) }

	/// new SHA256 file name media function. requires database migrated. uses
	/// SHA256 hash of the base64 key as the file name
	#[must_use]
	pub fn get_media_file_sha256(&self, key: &[u8]) -> PathBuf {
		let mut r = self.get_media_dir();
		// Using the hash of the base64 key as the filename
		// This is to prevent the total length of the path from exceeding the maximum
		// length in most filesystems
		let digest = <sha2::Sha256 as sha2::Digest>::digest(key);
		let encoded = encode_key(&digest);
		r.push(encoded);
		r
	}

	/// old base64 file name media function
	/// This is the old version of `get_media_file` that uses the full base64
	/// key as the filename.
	#[must_use]
	pub fn get_media_file_b64(&self, key: &[u8]) -> PathBuf {
		let mut r = self.get_media_dir();
		let encoded = encode_key(key);
		r.push(encoded);
		r
	}

	#[must_use]
	pub fn get_media_dir(&self) -> PathBuf {
		let mut r = PathBuf::new();
		r.push(self.services.server.config.database_path.clone());
		r.push("media");
		r
	}
}

#[inline]
#[must_use]
pub fn encode_key(key: &[u8]) -> String { general_purpose::URL_SAFE_NO_PAD.encode(key) }
