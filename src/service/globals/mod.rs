mod data;

use std::{
	collections::HashMap,
	fmt::Write,
	sync::{Arc, RwLock},
	time::Instant,
};

use conduit::{err, error, trace, Config, Result};
use data::Data;
use ipaddress::IPAddress;
use regex::RegexSet;
use ruma::{
	OwnedEventId, OwnedRoomAliasId, OwnedServerName, OwnedUserId, RoomAliasId, RoomVersionId, ServerName, UserId,
};
use tokio::sync::Mutex;

use crate::service;

pub struct Service {
	pub db: Data,

	pub config: Config,
	pub cidr_range_denylist: Vec<IPAddress>,
	jwt_decoding_key: Option<jsonwebtoken::DecodingKey>,
	pub stable_room_versions: Vec<RoomVersionId>,
	pub unstable_room_versions: Vec<RoomVersionId>,
	pub bad_event_ratelimiter: Arc<RwLock<HashMap<OwnedEventId, RateLimitState>>>,
	pub bad_query_ratelimiter: Arc<RwLock<HashMap<OwnedServerName, RateLimitState>>>,
	pub stateres_mutex: Arc<Mutex<()>>,
	pub server_user: OwnedUserId,
	pub admin_alias: OwnedRoomAliasId,
	pub turn_secret: String,
	pub registration_token: Option<String>,
}

type RateLimitState = (Instant, u32); // Time if last failed try, number of failed tries

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let db = Data::new(&args);
		let config = &args.server.config;

		let jwt_decoding_key = config
			.jwt_secret
			.as_ref()
			.map(|secret| jsonwebtoken::DecodingKey::from_secret(secret.as_bytes()));

		// Supported and stable room versions
		let stable_room_versions = vec![
			RoomVersionId::V6,
			RoomVersionId::V7,
			RoomVersionId::V8,
			RoomVersionId::V9,
			RoomVersionId::V10,
			RoomVersionId::V11,
		];
		// Experimental, partially supported room versions
		let unstable_room_versions = vec![RoomVersionId::V2, RoomVersionId::V3, RoomVersionId::V4, RoomVersionId::V5];

		let cidr_range_denylist: Vec<_> = config
			.ip_range_denylist
			.iter()
			.map(IPAddress::parse)
			.inspect(|cidr| trace!("Denied CIDR range: {cidr:?}"))
			.collect::<Result<_, String>>()
			.map_err(|e| err!(Config("ip_range_denylist", e)))?;

		let turn_secret = config
			.turn_secret_file
			.as_ref()
			.map_or(config.turn_secret.clone(), |path| {
				std::fs::read_to_string(path).unwrap_or_else(|e| {
					error!("Failed to read the TURN secret file: {e}");

					config.turn_secret.clone()
				})
			});

		let registration_token =
			config
				.registration_token_file
				.as_ref()
				.map_or(config.registration_token.clone(), |path| {
					let Ok(token) = std::fs::read_to_string(path).inspect_err(|e| {
						error!("Failed to read the registration token file: {e}");
					}) else {
						return config.registration_token.clone();
					};

					Some(token)
				});

		let mut s = Self {
			db,
			config: config.clone(),
			cidr_range_denylist,
			jwt_decoding_key,
			stable_room_versions,
			unstable_room_versions,
			bad_event_ratelimiter: Arc::new(RwLock::new(HashMap::new())),
			bad_query_ratelimiter: Arc::new(RwLock::new(HashMap::new())),
			stateres_mutex: Arc::new(Mutex::new(())),
			admin_alias: RoomAliasId::parse(format!("#admins:{}", &config.server_name))
				.expect("#admins:server_name is valid alias name"),
			server_user: UserId::parse_with_server_name(String::from("conduit"), &config.server_name)
				.expect("@conduit:server_name is valid"),
			turn_secret,
			registration_token,
		};

		if !s
			.supported_room_versions()
			.contains(&s.config.default_room_version)
		{
			error!(config=?s.config.default_room_version, fallback=?conduit::config::default_default_room_version(), "Room version in config isn't supported, falling back to default version");
			s.config.default_room_version = conduit::config::default_default_room_version();
		};

		Ok(Arc::new(s))
	}

	fn memory_usage(&self, out: &mut dyn Write) -> Result<()> {
		let bad_event_ratelimiter = self
			.bad_event_ratelimiter
			.read()
			.expect("locked for reading")
			.len();
		writeln!(out, "bad_event_ratelimiter: {bad_event_ratelimiter}")?;

		let bad_query_ratelimiter = self
			.bad_query_ratelimiter
			.read()
			.expect("locked for reading")
			.len();
		writeln!(out, "bad_query_ratelimiter: {bad_query_ratelimiter}")?;

		Ok(())
	}

	fn clear_cache(&self) {
		self.bad_event_ratelimiter
			.write()
			.expect("locked for writing")
			.clear();

		self.bad_query_ratelimiter
			.write()
			.expect("locked for writing")
			.clear();
	}

	fn name(&self) -> &str { service::make_name(std::module_path!()) }
}

impl Service {
	#[inline]
	pub fn next_count(&self) -> Result<u64> { self.db.next_count() }

	#[inline]
	pub fn current_count(&self) -> Result<u64> { Ok(self.db.current_count()) }

	#[inline]
	pub fn server_name(&self) -> &ServerName { self.config.server_name.as_ref() }

	pub fn allow_registration(&self) -> bool { self.config.allow_registration }

	pub fn allow_guest_registration(&self) -> bool { self.config.allow_guest_registration }

	pub fn allow_guests_auto_join_rooms(&self) -> bool { self.config.allow_guests_auto_join_rooms }

	pub fn log_guest_registrations(&self) -> bool { self.config.log_guest_registrations }

	pub fn allow_encryption(&self) -> bool { self.config.allow_encryption }

	pub fn allow_federation(&self) -> bool { self.config.allow_federation }

	pub fn allow_public_room_directory_over_federation(&self) -> bool {
		self.config.allow_public_room_directory_over_federation
	}

	pub fn allow_device_name_federation(&self) -> bool { self.config.allow_device_name_federation }

	pub fn allow_room_creation(&self) -> bool { self.config.allow_room_creation }

	pub fn allow_unstable_room_versions(&self) -> bool { self.config.allow_unstable_room_versions }

	#[inline]
	pub fn default_room_version(&self) -> RoomVersionId { self.config.default_room_version.clone() }

	pub fn new_user_displayname_suffix(&self) -> &String { &self.config.new_user_displayname_suffix }

	pub fn allow_check_for_updates(&self) -> bool { self.config.allow_check_for_updates }

	pub fn trusted_servers(&self) -> &[OwnedServerName] { &self.config.trusted_servers }

	pub fn jwt_decoding_key(&self) -> Option<&jsonwebtoken::DecodingKey> { self.jwt_decoding_key.as_ref() }

	pub fn turn_password(&self) -> &String { &self.config.turn_password }

	pub fn turn_ttl(&self) -> u64 { self.config.turn_ttl }

	pub fn turn_uris(&self) -> &[String] { &self.config.turn_uris }

	pub fn turn_username(&self) -> &String { &self.config.turn_username }

	pub fn notification_push_path(&self) -> &String { &self.config.notification_push_path }

	pub fn emergency_password(&self) -> &Option<String> { &self.config.emergency_password }

	pub fn url_preview_domain_contains_allowlist(&self) -> &Vec<String> {
		&self.config.url_preview_domain_contains_allowlist
	}

	pub fn url_preview_domain_explicit_allowlist(&self) -> &Vec<String> {
		&self.config.url_preview_domain_explicit_allowlist
	}

	pub fn url_preview_domain_explicit_denylist(&self) -> &Vec<String> {
		&self.config.url_preview_domain_explicit_denylist
	}

	pub fn url_preview_url_contains_allowlist(&self) -> &Vec<String> { &self.config.url_preview_url_contains_allowlist }

	pub fn url_preview_max_spider_size(&self) -> usize { self.config.url_preview_max_spider_size }

	pub fn url_preview_check_root_domain(&self) -> bool { self.config.url_preview_check_root_domain }

	pub fn forbidden_alias_names(&self) -> &RegexSet { &self.config.forbidden_alias_names }

	pub fn forbidden_usernames(&self) -> &RegexSet { &self.config.forbidden_usernames }

	pub fn allow_local_presence(&self) -> bool { self.config.allow_local_presence }

	pub fn allow_incoming_presence(&self) -> bool { self.config.allow_incoming_presence }

	pub fn allow_outgoing_presence(&self) -> bool { self.config.allow_outgoing_presence }

	pub fn allow_incoming_read_receipts(&self) -> bool { self.config.allow_incoming_read_receipts }

	pub fn allow_outgoing_read_receipts(&self) -> bool { self.config.allow_outgoing_read_receipts }

	pub fn block_non_admin_invites(&self) -> bool { self.config.block_non_admin_invites }

	pub fn supported_room_versions(&self) -> Vec<RoomVersionId> {
		if self.config.allow_unstable_room_versions {
			self.stable_room_versions
				.clone()
				.into_iter()
				.chain(self.unstable_room_versions.clone())
				.collect()
		} else {
			self.stable_room_versions.clone()
		}
	}

	#[inline]
	pub fn valid_cidr_range(&self, ip: &IPAddress) -> bool {
		for cidr in &self.cidr_range_denylist {
			if cidr.includes(ip) {
				return false;
			}
		}

		true
	}

	/// checks if `user_id` is local to us via server_name comparison
	#[inline]
	pub fn user_is_local(&self, user_id: &UserId) -> bool { self.server_is_ours(user_id.server_name()) }

	#[inline]
	pub fn server_is_ours(&self, server_name: &ServerName) -> bool { server_name == self.config.server_name }

	#[inline]
	pub fn is_read_only(&self) -> bool { self.db.db.is_read_only() }
}
