mod data;

use std::{
	collections::HashMap,
	fmt::Write,
	sync::{Arc, Mutex as StdMutex, Mutex},
};

use conduit::{err, error, pdu::PduBuilder, utils::math::usize_from_f64, warn, Error, PduEvent, Result};
use data::Data;
use lru_cache::LruCache;
use ruma::{
	events::{
		room::{
			avatar::RoomAvatarEventContent,
			canonical_alias::RoomCanonicalAliasEventContent,
			create::RoomCreateEventContent,
			encryption::RoomEncryptionEventContent,
			guest_access::{GuestAccess, RoomGuestAccessEventContent},
			history_visibility::{HistoryVisibility, RoomHistoryVisibilityEventContent},
			join_rules::{AllowRule, JoinRule, RoomJoinRulesEventContent, RoomMembership},
			member::{MembershipState, RoomMemberEventContent},
			name::RoomNameEventContent,
			power_levels::{RoomPowerLevels, RoomPowerLevelsEventContent},
			topic::RoomTopicEventContent,
		},
		StateEventType,
	},
	room::RoomType,
	space::SpaceRoomJoinRule,
	EventEncryptionAlgorithm, EventId, OwnedRoomAliasId, OwnedRoomId, OwnedServerName, OwnedUserId, RoomId, ServerName,
	UserId,
};
use serde_json::value::to_raw_value;

use crate::{rooms, rooms::state::RoomMutexGuard, Dep};

pub struct Service {
	services: Services,
	db: Data,
	pub server_visibility_cache: Mutex<LruCache<(OwnedServerName, u64), bool>>,
	pub user_visibility_cache: Mutex<LruCache<(OwnedUserId, u64), bool>>,
}

struct Services {
	state_cache: Dep<rooms::state_cache::Service>,
	timeline: Dep<rooms::timeline::Service>,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let config = &args.server.config;
		let server_visibility_cache_capacity =
			f64::from(config.server_visibility_cache_capacity) * config.cache_capacity_modifier;
		let user_visibility_cache_capacity =
			f64::from(config.user_visibility_cache_capacity) * config.cache_capacity_modifier;

		Ok(Arc::new(Self {
			services: Services {
				state_cache: args.depend::<rooms::state_cache::Service>("rooms::state_cache"),
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
			},
			db: Data::new(&args),
			server_visibility_cache: StdMutex::new(LruCache::new(usize_from_f64(server_visibility_cache_capacity)?)),
			user_visibility_cache: StdMutex::new(LruCache::new(usize_from_f64(user_visibility_cache_capacity)?)),
		}))
	}

	fn memory_usage(&self, out: &mut dyn Write) -> Result<()> {
		let server_visibility_cache = self.server_visibility_cache.lock().expect("locked").len();
		writeln!(out, "server_visibility_cache: {server_visibility_cache}")?;

		let user_visibility_cache = self.user_visibility_cache.lock().expect("locked").len();
		writeln!(out, "user_visibility_cache: {user_visibility_cache}")?;

		Ok(())
	}

	fn clear_cache(&self) {
		self.server_visibility_cache.lock().expect("locked").clear();
		self.user_visibility_cache.lock().expect("locked").clear();
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Builds a StateMap by iterating over all keys that start
	/// with state_hash, this gives the full state for the given state_hash.
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn state_full_ids(&self, shortstatehash: u64) -> Result<HashMap<u64, Arc<EventId>>> {
		self.db.state_full_ids(shortstatehash).await
	}

	pub async fn state_full(&self, shortstatehash: u64) -> Result<HashMap<(StateEventType, String), Arc<PduEvent>>> {
		self.db.state_full(shortstatehash).await
	}

	/// Returns a single PDU from `room_id` with key (`event_type`,
	/// `state_key`).
	#[tracing::instrument(skip(self), level = "debug")]
	pub fn state_get_id(
		&self, shortstatehash: u64, event_type: &StateEventType, state_key: &str,
	) -> Result<Option<Arc<EventId>>> {
		self.db.state_get_id(shortstatehash, event_type, state_key)
	}

	/// Returns a single PDU from `room_id` with key (`event_type`,
	/// `state_key`).
	#[inline]
	pub fn state_get(
		&self, shortstatehash: u64, event_type: &StateEventType, state_key: &str,
	) -> Result<Option<Arc<PduEvent>>> {
		self.db.state_get(shortstatehash, event_type, state_key)
	}

	/// Get membership for given user in state
	fn user_membership(&self, shortstatehash: u64, user_id: &UserId) -> Result<MembershipState> {
		self.state_get(shortstatehash, &StateEventType::RoomMember, user_id.as_str())?
			.map_or(Ok(MembershipState::Leave), |s| {
				serde_json::from_str(s.content.get())
					.map(|c: RoomMemberEventContent| c.membership)
					.map_err(|_| Error::bad_database("Invalid room membership event in database."))
			})
	}

	/// The user was a joined member at this state (potentially in the past)
	#[inline]
	fn user_was_joined(&self, shortstatehash: u64, user_id: &UserId) -> bool {
		self.user_membership(shortstatehash, user_id)
			.is_ok_and(|s| s == MembershipState::Join)
		// Return sensible default, i.e.
		// false
	}

	/// The user was an invited or joined room member at this state (potentially
	/// in the past)
	#[inline]
	fn user_was_invited(&self, shortstatehash: u64, user_id: &UserId) -> bool {
		self.user_membership(shortstatehash, user_id)
			.is_ok_and(|s| s == MembershipState::Join || s == MembershipState::Invite)
		// Return sensible default, i.e. false
	}

	/// Whether a server is allowed to see an event through federation, based on
	/// the room's history_visibility at that event's state.
	#[tracing::instrument(skip(self, origin, room_id, event_id))]
	pub fn server_can_see_event(&self, origin: &ServerName, room_id: &RoomId, event_id: &EventId) -> Result<bool> {
		let Some(shortstatehash) = self.pdu_shortstatehash(event_id)? else {
			return Ok(true);
		};

		if let Some(visibility) = self
			.server_visibility_cache
			.lock()
			.unwrap()
			.get_mut(&(origin.to_owned(), shortstatehash))
		{
			return Ok(*visibility);
		}

		let history_visibility = self
			.state_get(shortstatehash, &StateEventType::RoomHistoryVisibility, "")?
			.map_or(Ok(HistoryVisibility::Shared), |s| {
				serde_json::from_str(s.content.get())
					.map(|c: RoomHistoryVisibilityEventContent| c.history_visibility)
					.map_err(|e| {
						error!(
							"Invalid history visibility event in database for room {room_id}, assuming is \"shared\": \
							 {e}"
						);
						Error::bad_database("Invalid history visibility event in database.")
					})
			})
			.unwrap_or(HistoryVisibility::Shared);

		let mut current_server_members = self
			.services
			.state_cache
			.room_members(room_id)
			.filter_map(Result::ok)
			.filter(|member| member.server_name() == origin);

		let visibility = match history_visibility {
			HistoryVisibility::WorldReadable | HistoryVisibility::Shared => true,
			HistoryVisibility::Invited => {
				// Allow if any member on requesting server was AT LEAST invited, else deny
				current_server_members.any(|member| self.user_was_invited(shortstatehash, &member))
			},
			HistoryVisibility::Joined => {
				// Allow if any member on requested server was joined, else deny
				current_server_members.any(|member| self.user_was_joined(shortstatehash, &member))
			},
			_ => {
				error!("Unknown history visibility {history_visibility}");
				false
			},
		};

		self.server_visibility_cache
			.lock()
			.unwrap()
			.insert((origin.to_owned(), shortstatehash), visibility);

		Ok(visibility)
	}

	/// Whether a user is allowed to see an event, based on
	/// the room's history_visibility at that event's state.
	#[tracing::instrument(skip(self, user_id, room_id, event_id))]
	pub fn user_can_see_event(&self, user_id: &UserId, room_id: &RoomId, event_id: &EventId) -> Result<bool> {
		let Some(shortstatehash) = self.pdu_shortstatehash(event_id)? else {
			return Ok(true);
		};

		if let Some(visibility) = self
			.user_visibility_cache
			.lock()
			.unwrap()
			.get_mut(&(user_id.to_owned(), shortstatehash))
		{
			return Ok(*visibility);
		}

		let currently_member = self.services.state_cache.is_joined(user_id, room_id)?;

		let history_visibility = self
			.state_get(shortstatehash, &StateEventType::RoomHistoryVisibility, "")?
			.map_or(Ok(HistoryVisibility::Shared), |s| {
				serde_json::from_str(s.content.get())
					.map(|c: RoomHistoryVisibilityEventContent| c.history_visibility)
					.map_err(|e| {
						error!(
							"Invalid history visibility event in database for room {room_id}, assuming is \"shared\": \
							 {e}"
						);
						Error::bad_database("Invalid history visibility event in database.")
					})
			})
			.unwrap_or(HistoryVisibility::Shared);

		let visibility = match history_visibility {
			HistoryVisibility::WorldReadable => true,
			HistoryVisibility::Shared => currently_member,
			HistoryVisibility::Invited => {
				// Allow if any member on requesting server was AT LEAST invited, else deny
				self.user_was_invited(shortstatehash, user_id)
			},
			HistoryVisibility::Joined => {
				// Allow if any member on requested server was joined, else deny
				self.user_was_joined(shortstatehash, user_id)
			},
			_ => {
				error!("Unknown history visibility {history_visibility}");
				false
			},
		};

		self.user_visibility_cache
			.lock()
			.unwrap()
			.insert((user_id.to_owned(), shortstatehash), visibility);

		Ok(visibility)
	}

	/// Whether a user is allowed to see an event, based on
	/// the room's history_visibility at that event's state.
	#[tracing::instrument(skip(self, user_id, room_id))]
	pub fn user_can_see_state_events(&self, user_id: &UserId, room_id: &RoomId) -> Result<bool> {
		let currently_member = self.services.state_cache.is_joined(user_id, room_id)?;

		let history_visibility = self
			.room_state_get(room_id, &StateEventType::RoomHistoryVisibility, "")?
			.map_or(Ok(HistoryVisibility::Shared), |s| {
				serde_json::from_str(s.content.get())
					.map(|c: RoomHistoryVisibilityEventContent| c.history_visibility)
					.map_err(|e| {
						error!(
							"Invalid history visibility event in database for room {room_id}, assuming is \"shared\": \
							 {e}"
						);
						Error::bad_database("Invalid history visibility event in database.")
					})
			})
			.unwrap_or(HistoryVisibility::Shared);

		Ok(currently_member || history_visibility == HistoryVisibility::WorldReadable)
	}

	/// Returns the state hash for this pdu.
	pub fn pdu_shortstatehash(&self, event_id: &EventId) -> Result<Option<u64>> { self.db.pdu_shortstatehash(event_id) }

	/// Returns the full room state.
	#[tracing::instrument(skip(self), level = "debug")]
	pub async fn room_state_full(&self, room_id: &RoomId) -> Result<HashMap<(StateEventType, String), Arc<PduEvent>>> {
		self.db.room_state_full(room_id).await
	}

	/// Returns a single PDU from `room_id` with key (`event_type`,
	/// `state_key`).
	#[tracing::instrument(skip(self), level = "debug")]
	pub fn room_state_get_id(
		&self, room_id: &RoomId, event_type: &StateEventType, state_key: &str,
	) -> Result<Option<Arc<EventId>>> {
		self.db.room_state_get_id(room_id, event_type, state_key)
	}

	/// Returns a single PDU from `room_id` with key (`event_type`,
	/// `state_key`).
	#[tracing::instrument(skip(self), level = "debug")]
	pub fn room_state_get(
		&self, room_id: &RoomId, event_type: &StateEventType, state_key: &str,
	) -> Result<Option<Arc<PduEvent>>> {
		self.db.room_state_get(room_id, event_type, state_key)
	}

	pub fn get_name(&self, room_id: &RoomId) -> Result<Option<String>> {
		self.room_state_get(room_id, &StateEventType::RoomName, "")?
			.map_or(Ok(None), |s| {
				Ok(serde_json::from_str(s.content.get()).map_or_else(|_| None, |c: RoomNameEventContent| Some(c.name)))
			})
	}

	pub fn get_avatar(&self, room_id: &RoomId) -> Result<ruma::JsOption<RoomAvatarEventContent>> {
		self.room_state_get(room_id, &StateEventType::RoomAvatar, "")?
			.map_or(Ok(ruma::JsOption::Undefined), |s| {
				serde_json::from_str(s.content.get())
					.map_err(|_| Error::bad_database("Invalid room avatar event in database."))
			})
	}

	pub fn get_member(&self, room_id: &RoomId, user_id: &UserId) -> Result<Option<RoomMemberEventContent>> {
		self.room_state_get(room_id, &StateEventType::RoomMember, user_id.as_str())?
			.map_or(Ok(None), |s| {
				serde_json::from_str(s.content.get())
					.map_err(|_| Error::bad_database("Invalid room member event in database."))
			})
	}

	pub fn user_can_invite(
		&self, room_id: &RoomId, sender: &UserId, target_user: &UserId, state_lock: &RoomMutexGuard,
	) -> Result<bool> {
		let content = to_raw_value(&RoomMemberEventContent::new(MembershipState::Invite))
			.expect("Event content always serializes");

		let new_event = PduBuilder {
			event_type: ruma::events::TimelineEventType::RoomMember,
			content,
			unsigned: None,
			state_key: Some(target_user.into()),
			redacts: None,
			timestamp: None,
		};

		Ok(self
			.services
			.timeline
			.create_hash_and_sign_event(new_event, sender, room_id, state_lock)
			.is_ok())
	}

	/// Checks if guests are able to view room content without joining
	pub fn is_world_readable(&self, room_id: &RoomId) -> Result<bool, Error> {
		self.room_state_get(room_id, &StateEventType::RoomHistoryVisibility, "")?
			.map_or(Ok(false), |s| {
				serde_json::from_str(s.content.get())
					.map(|c: RoomHistoryVisibilityEventContent| {
						c.history_visibility == HistoryVisibility::WorldReadable
					})
					.map_err(|e| {
						error!(
							"Invalid room history visibility event in database for room {room_id}, assuming not world \
							 readable: {e} "
						);
						Error::bad_database("Invalid room history visibility event in database.")
					})
			})
	}

	/// Checks if guests are able to join a given room
	pub fn guest_can_join(&self, room_id: &RoomId) -> Result<bool, Error> {
		self.room_state_get(room_id, &StateEventType::RoomGuestAccess, "")?
			.map_or(Ok(false), |s| {
				serde_json::from_str(s.content.get())
					.map(|c: RoomGuestAccessEventContent| c.guest_access == GuestAccess::CanJoin)
					.map_err(|_| Error::bad_database("Invalid room guest access event in database."))
			})
	}

	/// Gets the primary alias from canonical alias event
	pub fn get_canonical_alias(&self, room_id: &RoomId) -> Result<Option<OwnedRoomAliasId>, Error> {
		self.room_state_get(room_id, &StateEventType::RoomCanonicalAlias, "")?
			.map_or(Ok(None), |s| {
				serde_json::from_str(s.content.get())
					.map(|c: RoomCanonicalAliasEventContent| c.alias)
					.map_err(|_| Error::bad_database("Invalid canonical alias event in database."))
			})
	}

	/// Gets the room topic
	pub fn get_room_topic(&self, room_id: &RoomId) -> Result<Option<String>, Error> {
		self.room_state_get(room_id, &StateEventType::RoomTopic, "")?
			.map_or(Ok(None), |s| {
				serde_json::from_str(s.content.get())
					.map(|c: RoomTopicEventContent| Some(c.topic))
					.map_err(|e| {
						error!("Invalid room topic event in database for room {room_id}: {e}");
						Error::bad_database("Invalid room topic event in database.")
					})
			})
	}

	/// Checks if a given user can redact a given event
	///
	/// If federation is true, it allows redaction events from any user of the
	/// same server as the original event sender
	pub fn user_can_redact(
		&self, redacts: &EventId, sender: &UserId, room_id: &RoomId, federation: bool,
	) -> Result<bool> {
		self.room_state_get(room_id, &StateEventType::RoomPowerLevels, "")?
			.map_or_else(
				|| {
					// Falling back on m.room.create to judge power level
					if let Some(pdu) = self.room_state_get(room_id, &StateEventType::RoomCreate, "")? {
						Ok(pdu.sender == sender
							|| if let Ok(Some(pdu)) = self.services.timeline.get_pdu(redacts) {
								pdu.sender == sender
							} else {
								false
							})
					} else {
						Err(Error::bad_database(
							"No m.room.power_levels or m.room.create events in database for room",
						))
					}
				},
				|event| {
					serde_json::from_str(event.content.get())
						.map(|content: RoomPowerLevelsEventContent| content.into())
						.map(|event: RoomPowerLevels| {
							event.user_can_redact_event_of_other(sender)
								|| event.user_can_redact_own_event(sender)
									&& if let Ok(Some(pdu)) = self.services.timeline.get_pdu(redacts) {
										if federation {
											pdu.sender.server_name() == sender.server_name()
										} else {
											pdu.sender == sender
										}
									} else {
										false
									}
						})
						.map_err(|_| Error::bad_database("Invalid m.room.power_levels event in database"))
				},
			)
	}

	/// Returns the join rule (`SpaceRoomJoinRule`) for a given room
	pub fn get_join_rule(&self, room_id: &RoomId) -> Result<(SpaceRoomJoinRule, Vec<OwnedRoomId>), Error> {
		Ok(self
			.room_state_get(room_id, &StateEventType::RoomJoinRules, "")?
			.map(|s| {
				serde_json::from_str(s.content.get())
					.map(|c: RoomJoinRulesEventContent| {
						(c.join_rule.clone().into(), self.allowed_room_ids(c.join_rule))
					})
					.map_err(|e| err!(Database(error!("Invalid room join rule event in database: {e}"))))
			})
			.transpose()?
			.unwrap_or((SpaceRoomJoinRule::Invite, vec![])))
	}

	/// Returns an empty vec if not a restricted room
	pub fn allowed_room_ids(&self, join_rule: JoinRule) -> Vec<OwnedRoomId> {
		let mut room_ids = vec![];
		if let JoinRule::Restricted(r) | JoinRule::KnockRestricted(r) = join_rule {
			for rule in r.allow {
				if let AllowRule::RoomMembership(RoomMembership {
					room_id: membership,
				}) = rule
				{
					room_ids.push(membership.clone());
				}
			}
		}
		room_ids
	}

	pub fn get_room_type(&self, room_id: &RoomId) -> Result<Option<RoomType>> {
		Ok(self
			.room_state_get(room_id, &StateEventType::RoomCreate, "")?
			.map(|s| {
				serde_json::from_str::<RoomCreateEventContent>(s.content.get())
					.map_err(|e| err!(Database(error!("Invalid room create event in database: {e}"))))
			})
			.transpose()?
			.and_then(|e| e.room_type))
	}

	/// Gets the room's encryption algorithm if `m.room.encryption` state event
	/// is found
	pub fn get_room_encryption(&self, room_id: &RoomId) -> Result<Option<EventEncryptionAlgorithm>> {
		self.room_state_get(room_id, &StateEventType::RoomEncryption, "")?
			.map_or(Ok(None), |s| {
				serde_json::from_str::<RoomEncryptionEventContent>(s.content.get())
					.map(|content| Some(content.algorithm))
					.map_err(|e| err!(Database(error!("Invalid room encryption event in database: {e}"))))
			})
	}
}
