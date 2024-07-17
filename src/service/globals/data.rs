use std::{
	collections::BTreeMap,
	sync::{Arc, RwLock},
};

use conduit::{trace, utils, Error, Result};
use database::{Database, Map};
use futures_util::{stream::FuturesUnordered, StreamExt};
use ruma::{
	api::federation::discovery::{ServerSigningKeys, VerifyKey},
	signatures::Ed25519KeyPair,
	DeviceId, MilliSecondsSinceUnixEpoch, OwnedServerSigningKeyId, ServerName, UserId,
};

use crate::services;

pub struct Data {
	global: Arc<Map>,
	todeviceid_events: Arc<Map>,
	userroomid_joined: Arc<Map>,
	userroomid_invitestate: Arc<Map>,
	userroomid_leftstate: Arc<Map>,
	userroomid_notificationcount: Arc<Map>,
	userroomid_highlightcount: Arc<Map>,
	pduid_pdu: Arc<Map>,
	keychangeid_userid: Arc<Map>,
	roomusertype_roomuserdataid: Arc<Map>,
	server_signingkeys: Arc<Map>,
	readreceiptid_readreceipt: Arc<Map>,
	userid_lastonetimekeyupdate: Arc<Map>,
	pub(super) db: Arc<Database>,
	counter: RwLock<u64>,
}

const COUNTER: &[u8] = b"c";

impl Data {
	pub(super) fn new(db: &Arc<Database>) -> Self {
		Self {
			global: db["global"].clone(),
			todeviceid_events: db["todeviceid_events"].clone(),
			userroomid_joined: db["userroomid_joined"].clone(),
			userroomid_invitestate: db["userroomid_invitestate"].clone(),
			userroomid_leftstate: db["userroomid_leftstate"].clone(),
			userroomid_notificationcount: db["userroomid_notificationcount"].clone(),
			userroomid_highlightcount: db["userroomid_highlightcount"].clone(),
			pduid_pdu: db["pduid_pdu"].clone(),
			keychangeid_userid: db["keychangeid_userid"].clone(),
			roomusertype_roomuserdataid: db["roomusertype_roomuserdataid"].clone(),
			server_signingkeys: db["server_signingkeys"].clone(),
			readreceiptid_readreceipt: db["readreceiptid_readreceipt"].clone(),
			userid_lastonetimekeyupdate: db["userid_lastonetimekeyupdate"].clone(),
			db: db.clone(),
			counter: RwLock::new(Self::stored_count(&db["global"]).expect("initialized global counter")),
		}
	}

	pub fn next_count(&self) -> Result<u64> {
		let _cork = self.db.cork();
		let mut lock = self.counter.write().expect("locked");
		let counter: &mut u64 = &mut lock;
		debug_assert!(
			*counter == Self::stored_count(&self.global).expect("database failure"),
			"counter mismatch"
		);

		*counter = counter
			.checked_add(1)
			.expect("counter must not overflow u64");

		self.global.insert(COUNTER, &counter.to_be_bytes())?;

		Ok(*counter)
	}

	#[inline]
	pub fn current_count(&self) -> u64 {
		let lock = self.counter.read().expect("locked");
		let counter: &u64 = &lock;
		debug_assert!(
			*counter == Self::stored_count(&self.global).expect("database failure"),
			"counter mismatch"
		);

		*counter
	}

	fn stored_count(global: &Arc<Map>) -> Result<u64> {
		global
			.get(COUNTER)?
			.as_deref()
			.map_or(Ok(0_u64), utils::u64_from_bytes)
	}

	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn watch(&self, user_id: &UserId, device_id: &DeviceId) -> Result<()> {
		let userid_bytes = user_id.as_bytes().to_vec();
		let mut userid_prefix = userid_bytes.clone();
		userid_prefix.push(0xFF);

		let mut userdeviceid_prefix = userid_prefix.clone();
		userdeviceid_prefix.extend_from_slice(device_id.as_bytes());
		userdeviceid_prefix.push(0xFF);

		let mut futures = FuturesUnordered::new();

		// Return when *any* user changed their key
		// TODO: only send for user they share a room with
		futures.push(self.todeviceid_events.watch_prefix(&userdeviceid_prefix));

		futures.push(self.userroomid_joined.watch_prefix(&userid_prefix));
		futures.push(self.userroomid_invitestate.watch_prefix(&userid_prefix));
		futures.push(self.userroomid_leftstate.watch_prefix(&userid_prefix));
		futures.push(
			self.userroomid_notificationcount
				.watch_prefix(&userid_prefix),
		);
		futures.push(self.userroomid_highlightcount.watch_prefix(&userid_prefix));

		// Events for rooms we are in
		for room_id in services()
			.rooms
			.state_cache
			.rooms_joined(user_id)
			.filter_map(Result::ok)
		{
			let short_roomid = services()
				.rooms
				.short
				.get_shortroomid(&room_id)
				.ok()
				.flatten()
				.expect("room exists")
				.to_be_bytes()
				.to_vec();

			let roomid_bytes = room_id.as_bytes().to_vec();
			let mut roomid_prefix = roomid_bytes.clone();
			roomid_prefix.push(0xFF);

			// PDUs
			futures.push(self.pduid_pdu.watch_prefix(&short_roomid));

			// EDUs
			futures.push(Box::pin(async move {
				let _result = services().rooms.typing.wait_for_update(&room_id).await;
			}));

			futures.push(self.readreceiptid_readreceipt.watch_prefix(&roomid_prefix));

			// Key changes
			futures.push(self.keychangeid_userid.watch_prefix(&roomid_prefix));

			// Room account data
			let mut roomuser_prefix = roomid_prefix.clone();
			roomuser_prefix.extend_from_slice(&userid_prefix);

			futures.push(
				self.roomusertype_roomuserdataid
					.watch_prefix(&roomuser_prefix),
			);
		}

		let mut globaluserdata_prefix = vec![0xFF];
		globaluserdata_prefix.extend_from_slice(&userid_prefix);

		futures.push(
			self.roomusertype_roomuserdataid
				.watch_prefix(&globaluserdata_prefix),
		);

		// More key changes (used when user is not joined to any rooms)
		futures.push(self.keychangeid_userid.watch_prefix(&userid_prefix));

		// One time keys
		futures.push(self.userid_lastonetimekeyupdate.watch_prefix(&userid_bytes));

		futures.push(Box::pin(async move {
			while services().server.running() {
				let _result = services().server.signal.subscribe().recv().await;
			}
		}));

		if !services().server.running() {
			return Ok(());
		}

		// Wait until one of them finds something
		trace!(futures = futures.len(), "watch started");
		futures.next().await;
		trace!(futures = futures.len(), "watch finished");

		Ok(())
	}

	pub fn load_keypair(&self) -> Result<Ed25519KeyPair> {
		let keypair_bytes = self.global.get(b"keypair")?.map_or_else(
			|| {
				let keypair = utils::generate_keypair();
				self.global.insert(b"keypair", &keypair)?;
				Ok::<_, Error>(keypair)
			},
			|val| Ok(val.to_vec()),
		)?;

		let mut parts = keypair_bytes.splitn(2, |&b| b == 0xFF);

		utils::string_from_bytes(
			// 1. version
			parts
				.next()
				.expect("splitn always returns at least one element"),
		)
		.map_err(|_| Error::bad_database("Invalid version bytes in keypair."))
		.and_then(|version| {
			// 2. key
			parts
				.next()
				.ok_or_else(|| Error::bad_database("Invalid keypair format in database."))
				.map(|key| (version, key))
		})
		.and_then(|(version, key)| {
			Ed25519KeyPair::from_der(key, version)
				.map_err(|_| Error::bad_database("Private or public keys are invalid."))
		})
	}

	#[inline]
	pub fn remove_keypair(&self) -> Result<()> { self.global.remove(b"keypair") }

	/// TODO: the key valid until timestamp (`valid_until_ts`) is only honored
	/// in room version > 4
	///
	/// Remove the outdated keys and insert the new ones.
	///
	/// This doesn't actually check that the keys provided are newer than the
	/// old set.
	pub fn add_signing_key(
		&self, origin: &ServerName, new_keys: ServerSigningKeys,
	) -> Result<BTreeMap<OwnedServerSigningKeyId, VerifyKey>> {
		// Not atomic, but this is not critical
		let signingkeys = self.server_signingkeys.get(origin.as_bytes())?;

		let mut keys = signingkeys
			.and_then(|keys| serde_json::from_slice(&keys).ok())
			.unwrap_or_else(|| {
				// Just insert "now", it doesn't matter
				ServerSigningKeys::new(origin.to_owned(), MilliSecondsSinceUnixEpoch::now())
			});

		let ServerSigningKeys {
			verify_keys,
			old_verify_keys,
			..
		} = new_keys;

		keys.verify_keys.extend(verify_keys);
		keys.old_verify_keys.extend(old_verify_keys);

		self.server_signingkeys.insert(
			origin.as_bytes(),
			&serde_json::to_vec(&keys).expect("serversigningkeys can be serialized"),
		)?;

		let mut tree = keys.verify_keys;
		tree.extend(
			keys.old_verify_keys
				.into_iter()
				.map(|old| (old.0, VerifyKey::new(old.1.key))),
		);

		Ok(tree)
	}

	/// This returns an empty `Ok(BTreeMap<..>)` when there are no keys found
	/// for the server.
	pub fn signing_keys_for(&self, origin: &ServerName) -> Result<BTreeMap<OwnedServerSigningKeyId, VerifyKey>> {
		let signingkeys = self
			.server_signingkeys
			.get(origin.as_bytes())?
			.and_then(|bytes| serde_json::from_slice(&bytes).ok())
			.map_or_else(BTreeMap::new, |keys: ServerSigningKeys| {
				let mut tree = keys.verify_keys;
				tree.extend(
					keys.old_verify_keys
						.into_iter()
						.map(|old| (old.0, VerifyKey::new(old.1.key))),
				);
				tree
			});

		Ok(signingkeys)
	}

	pub fn database_version(&self) -> Result<u64> {
		self.global.get(b"version")?.map_or(Ok(0), |version| {
			utils::u64_from_bytes(&version).map_err(|_| Error::bad_database("Database version id is invalid."))
		})
	}

	#[inline]
	pub fn bump_database_version(&self, new_version: u64) -> Result<()> {
		self.global.insert(b"version", &new_version.to_be_bytes())?;
		Ok(())
	}

	#[inline]
	pub fn backup(&self) -> Result<(), Box<dyn std::error::Error>> { self.db.db.backup() }

	#[inline]
	pub fn backup_list(&self) -> Result<String> { self.db.db.backup_list() }

	#[inline]
	pub fn file_list(&self) -> Result<String> { self.db.db.file_list() }
}
