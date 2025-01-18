use std::collections::HashSet;

use axum::extract::State;
use conduwuit::{
	at,
	utils::{
		result::{FlatOk, LogErr},
		stream::{BroadbandExt, TryIgnore, WidebandExt},
		IterStream, ReadyExt,
	},
	Event, PduCount, Result,
};
use futures::{FutureExt, StreamExt};
use ruma::{
	api::{
		client::{filter::RoomEventFilter, message::get_message_events},
		Direction,
	},
	events::{AnyStateEvent, StateEventType, TimelineEventType, TimelineEventType::*},
	serde::Raw,
	DeviceId, OwnedUserId, RoomId, UserId,
};
use service::{rooms::timeline::PdusIterItem, Services};

use crate::Ruma;

pub(crate) type LazySet = HashSet<OwnedUserId>;

/// list of safe and common non-state events to ignore if the user is ignored
const IGNORED_MESSAGE_TYPES: &[TimelineEventType; 17] = &[
	Audio,
	CallInvite,
	Emote,
	File,
	Image,
	KeyVerificationStart,
	Location,
	PollStart,
	UnstablePollStart,
	Beacon,
	Reaction,
	RoomEncrypted,
	RoomMessage,
	Sticker,
	Video,
	Voice,
	CallNotify,
];

const LIMIT_MAX: usize = 100;
const LIMIT_DEFAULT: usize = 10;

/// # `GET /_matrix/client/r0/rooms/{roomId}/messages`
///
/// Allows paginating through room history.
///
/// - Only works if the user is joined (TODO: always allow, but only show events
///   where the user was joined, depending on `history_visibility`)
pub(crate) async fn get_message_events_route(
	State(services): State<crate::State>,
	body: Ruma<get_message_events::v3::Request>,
) -> Result<get_message_events::v3::Response> {
	debug_assert!(IGNORED_MESSAGE_TYPES.is_sorted(), "IGNORED_MESSAGE_TYPES is not sorted");
	let sender = body.sender();
	let (sender_user, sender_device) = sender;
	let room_id = &body.room_id;
	let filter = &body.filter;

	let from: PduCount = body
		.from
		.as_deref()
		.map(str::parse)
		.transpose()?
		.unwrap_or_else(|| match body.dir {
			| Direction::Forward => PduCount::min(),
			| Direction::Backward => PduCount::max(),
		});

	let to: Option<PduCount> = body.to.as_deref().map(str::parse).flat_ok();

	let limit: usize = body
		.limit
		.try_into()
		.unwrap_or(LIMIT_DEFAULT)
		.min(LIMIT_MAX);

	services.rooms.lazy_loading.lazy_load_confirm_delivery(
		sender_user,
		sender_device,
		room_id,
		from,
	);

	if matches!(body.dir, Direction::Backward) {
		services
			.rooms
			.timeline
			.backfill_if_required(room_id, from)
			.boxed()
			.await
			.log_err()
			.ok();
	}

	let it = match body.dir {
		| Direction::Forward => services
			.rooms
			.timeline
			.pdus(Some(sender_user), room_id, Some(from))
			.ignore_err()
			.boxed(),

		| Direction::Backward => services
			.rooms
			.timeline
			.pdus_rev(Some(sender_user), room_id, Some(from))
			.ignore_err()
			.boxed(),
	};

	let events: Vec<_> = it
		.ready_take_while(|(count, _)| Some(*count) != to)
		.ready_filter_map(|item| event_filter(item, filter))
		.wide_filter_map(|item| ignored_filter(&services, item, sender_user))
		.wide_filter_map(|item| visibility_filter(&services, item, sender_user))
		.take(limit)
		.collect()
		.await;

	let lazy = events
		.iter()
		.stream()
		.fold(LazySet::new(), |lazy, item| {
			update_lazy(&services, room_id, sender, lazy, item, false)
		})
		.await;

	let state = lazy
		.iter()
		.stream()
		.broad_filter_map(|user_id| get_member_event(&services, room_id, user_id))
		.collect()
		.await;

	let next_token = events.last().map(at!(0));

	if !cfg!(feature = "element_hacks") {
		if let Some(next_token) = next_token {
			services.rooms.lazy_loading.lazy_load_mark_sent(
				sender_user,
				sender_device,
				room_id,
				lazy,
				next_token,
			);
		}
	}

	let chunk = events
		.into_iter()
		.map(at!(1))
		.map(|pdu| pdu.to_room_event())
		.collect();

	Ok(get_message_events::v3::Response {
		start: from.to_string(),
		end: next_token.as_ref().map(ToString::to_string),
		chunk,
		state,
	})
}

async fn get_member_event(
	services: &Services,
	room_id: &RoomId,
	user_id: &UserId,
) -> Option<Raw<AnyStateEvent>> {
	services
		.rooms
		.state_accessor
		.room_state_get(room_id, &StateEventType::RoomMember, user_id.as_str())
		.await
		.map(|member_event| member_event.to_state_event())
		.ok()
}

pub(crate) async fn update_lazy(
	services: &Services,
	room_id: &RoomId,
	sender: (&UserId, &DeviceId),
	mut lazy: LazySet,
	item: &PdusIterItem,
	force: bool,
) -> LazySet {
	let (_, event) = &item;
	let (sender_user, sender_device) = sender;

	/* TODO: Remove the "element_hacks" check when these are resolved:
	 * https://github.com/vector-im/element-android/issues/3417
	 * https://github.com/vector-im/element-web/issues/21034
	 */
	if force || cfg!(features = "element_hacks") {
		lazy.insert(event.sender().into());
		return lazy;
	}

	if lazy.contains(event.sender()) {
		return lazy;
	}

	if !services
		.rooms
		.lazy_loading
		.lazy_load_was_sent_before(sender_user, sender_device, room_id, event.sender())
		.await
	{
		lazy.insert(event.sender().into());
	}

	lazy
}

pub(crate) async fn ignored_filter(
	services: &Services,
	item: PdusIterItem,
	user_id: &UserId,
) -> Option<PdusIterItem> {
	let (_, pdu) = &item;

	// exclude Synapse's dummy events from bloating up response bodies. clients
	// don't need to see this.
	if pdu.kind.to_cow_str() == "org.matrix.dummy_event" {
		return None;
	}

	if IGNORED_MESSAGE_TYPES.binary_search(&pdu.kind).is_ok()
		&& (services.users.user_is_ignored(&pdu.sender, user_id).await
			|| services
				.moderation
				.is_remote_server_forbidden(pdu.sender().server_name()))
	{
		return None;
	}

	Some(item)
}

pub(crate) async fn visibility_filter(
	services: &Services,
	item: PdusIterItem,
	user_id: &UserId,
) -> Option<PdusIterItem> {
	let (_, pdu) = &item;

	services
		.rooms
		.state_accessor
		.user_can_see_event(user_id, &pdu.room_id, &pdu.event_id)
		.await
		.then_some(item)
}

pub(crate) fn event_filter(item: PdusIterItem, filter: &RoomEventFilter) -> Option<PdusIterItem> {
	let (_, pdu) = &item;
	pdu.matches(filter).then_some(item)
}
