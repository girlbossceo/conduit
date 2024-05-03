//! Helper tools for implementing filtering in the `/client/v3/sync` and
//! `/client/v3/rooms/:roomId/messages` endpoints.
//!
//! The default strategy for filtering is to generate all events, check them
//! against the filter, and drop events that were rejected. When significant
//! fraction of events are rejected, this results in a large amount of wasted
//! work computing events that will be dropped. In most cases, the structure of
//! our database doesn't allow for anything fancier, with only a few exceptions.
//!
//! The first exception is room filters (`room`/`not_room` pairs in
//! `filter.rooms` and `filter.rooms.{account_data,timeline,ephemeral,state}`).
//! In `/messages`, if the room is rejected by the filter, we can skip the
//! entire request.

use std::{collections::HashSet, hash::Hash};

use ruma::{
	api::client::filter::{RoomEventFilter, UrlFilter},
	RoomId, UserId,
};

use crate::{Error, PduEvent};

/// Structure for testing against an allowlist and a denylist with a single
/// `HashSet` lookup.
///
/// The denylist takes precedence (an item included in both the allowlist and
/// the denylist is denied).
pub(crate) enum AllowDenyList<'a, T: ?Sized> {
	/// TODO: fast-paths for allow-all and deny-all?
	Allow(HashSet<&'a T>),
	Deny(HashSet<&'a T>),
}

impl<'a, T: ?Sized + Hash + PartialEq + Eq> AllowDenyList<'a, T> {
	fn new<A, D>(allow: Option<A>, deny: D) -> AllowDenyList<'a, T>
	where
		A: Iterator<Item = &'a T>,
		D: Iterator<Item = &'a T>,
	{
		let deny_set = deny.collect::<HashSet<_>>();
		if let Some(allow) = allow {
			AllowDenyList::Allow(allow.filter(|x| !deny_set.contains(x)).collect())
		} else {
			AllowDenyList::Deny(deny_set)
		}
	}

	fn from_slices<O: AsRef<T>>(allow: Option<&'a [O]>, deny: &'a [O]) -> AllowDenyList<'a, T> {
		AllowDenyList::new(
			allow.map(|allow| allow.iter().map(AsRef::as_ref)),
			deny.iter().map(AsRef::as_ref),
		)
	}

	pub(crate) fn allowed(&self, value: &T) -> bool {
		match self {
			AllowDenyList::Allow(allow) => allow.contains(value),
			AllowDenyList::Deny(deny) => !deny.contains(value),
		}
	}
}

pub(crate) struct CompiledRoomEventFilter<'a> {
	rooms: AllowDenyList<'a, RoomId>,
	senders: AllowDenyList<'a, UserId>,
	url_filter: Option<UrlFilter>,
}

impl<'a> TryFrom<&'a RoomEventFilter> for CompiledRoomEventFilter<'a> {
	type Error = Error;

	fn try_from(source: &'a RoomEventFilter) -> Result<CompiledRoomEventFilter<'a>, Error> {
		Ok(CompiledRoomEventFilter {
			rooms: AllowDenyList::from_slices(source.rooms.as_deref(), &source.not_rooms),
			senders: AllowDenyList::from_slices(source.senders.as_deref(), &source.not_senders),
			url_filter: source.url_filter,
		})
	}
}

impl CompiledRoomEventFilter<'_> {
	/// Returns `true` if a room is allowed by the `rooms` and `not_rooms`
	/// fields.
	///
	/// This does *not* test the room against the top-level `rooms` filter.
	/// It is expected that callers have already filtered rooms that are
	/// rejected by the top-level filter using
	/// [`CompiledRoomFilter::room_allowed`], if applicable.
	pub(crate) fn room_allowed(&self, room_id: &RoomId) -> bool { self.rooms.allowed(room_id) }

	/// Returns `true` if a PDU event is allowed by the filter.
	///
	/// This tests against the `senders`, `not_senders`, and `url_filter`
	/// fields.
	///
	/// This does *not* check whether the event's room is allowed. It is
	/// expected that callers have already filtered out rejected rooms using
	/// [`CompiledRoomEventFilter::room_allowed`] and
	/// [`CompiledRoomFilter::room_allowed`].
	pub(crate) fn pdu_event_allowed(&self, pdu: &PduEvent) -> bool {
		self.senders.allowed(&pdu.sender) && self.allowed_by_url_filter(pdu)
	}

	fn allowed_by_url_filter(&self, pdu: &PduEvent) -> bool {
		let Some(filter) = self.url_filter else {
			return true;
		};
		// TODO: is this unwrap okay?
		let content: serde_json::Value = serde_json::from_str(pdu.content.get()).unwrap();
		match filter {
			UrlFilter::EventsWithoutUrl => !content["url"].is_string(),
			UrlFilter::EventsWithUrl => content["url"].is_string(),
		}
	}
}
