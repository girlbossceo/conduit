use std::collections::{BTreeMap, HashSet};

use conduit::PduCount;
use ruma::{
	api::client::{
		error::ErrorKind,
		filter::{RoomEventFilter, UrlFilter},
		message::{get_message_events, send_message_event},
	},
	events::{MessageLikeEventType, StateEventType},
	RoomId, UserId,
};
use serde_json::{from_str, Value};

use crate::{service::pdu::PduBuilder, services, utils, Error, PduEvent, Result, Ruma};

/// # `PUT /_matrix/client/v3/rooms/{roomId}/send/{eventType}/{txnId}`
///
/// Send a message event into the room.
///
/// - Is a NOOP if the txn id was already used before and returns the same event
///   id again
/// - The only requirement for the content is that it has to be valid json
/// - Tries to send the event into the room, auth rules will determine if it is
///   allowed
pub(crate) async fn send_message_event_route(
	body: Ruma<send_message_event::v3::Request>,
) -> Result<send_message_event::v3::Response> {
	let sender_user = body.sender_user.as_ref().expect("user is authenticated");
	let sender_device = body.sender_device.as_deref();

	let state_lock = services().rooms.state.mutex.lock(&body.room_id).await;

	// Forbid m.room.encrypted if encryption is disabled
	if MessageLikeEventType::RoomEncrypted == body.event_type && !services().globals.allow_encryption() {
		return Err(Error::BadRequest(ErrorKind::forbidden(), "Encryption has been disabled"));
	}

	if body.event_type == MessageLikeEventType::CallInvite
		&& services().rooms.directory.is_public_room(&body.room_id)?
	{
		return Err(Error::BadRequest(
			ErrorKind::forbidden(),
			"Room call invites are not allowed in public rooms",
		));
	}

	// Check if this is a new transaction id
	if let Some(response) = services()
		.transaction_ids
		.existing_txnid(sender_user, sender_device, &body.txn_id)?
	{
		// The client might have sent a txnid of the /sendToDevice endpoint
		// This txnid has no response associated with it
		if response.is_empty() {
			return Err(Error::BadRequest(
				ErrorKind::InvalidParam,
				"Tried to use txn id already used for an incompatible endpoint.",
			));
		}

		let event_id = utils::string_from_bytes(&response)
			.map_err(|_| Error::bad_database("Invalid txnid bytes in database."))?
			.try_into()
			.map_err(|_| Error::bad_database("Invalid event id in txnid data."))?;
		return Ok(send_message_event::v3::Response {
			event_id,
		});
	}

	let mut unsigned = BTreeMap::new();
	unsigned.insert("transaction_id".to_owned(), body.txn_id.to_string().into());

	let event_id = services()
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				event_type: body.event_type.to_string().into(),
				content: from_str(body.body.body.json().get())
					.map_err(|_| Error::BadRequest(ErrorKind::BadJson, "Invalid JSON body."))?,
				unsigned: Some(unsigned),
				state_key: None,
				redacts: None,
			},
			sender_user,
			&body.room_id,
			&state_lock,
		)
		.await?;

	services()
		.transaction_ids
		.add_txnid(sender_user, sender_device, &body.txn_id, event_id.as_bytes())?;

	drop(state_lock);

	Ok(send_message_event::v3::Response::new((*event_id).to_owned()))
}

/// # `GET /_matrix/client/r0/rooms/{roomId}/messages`
///
/// Allows paginating through room history.
///
/// - Only works if the user is joined (TODO: always allow, but only show events
///   where the user was joined, depending on `history_visibility`)
pub(crate) async fn get_message_events_route(
	body: Ruma<get_message_events::v3::Request>,
) -> Result<get_message_events::v3::Response> {
	let sender_user = body.sender_user.as_ref().expect("user is authenticated");
	let sender_device = body.sender_device.as_ref().expect("user is authenticated");

	let from = match body.from.clone() {
		Some(from) => PduCount::try_from_string(&from)?,
		None => match body.dir {
			ruma::api::Direction::Forward => PduCount::min(),
			ruma::api::Direction::Backward => PduCount::max(),
		},
	};

	let to = body
		.to
		.as_ref()
		.and_then(|t| PduCount::try_from_string(t).ok());

	services()
		.rooms
		.lazy_loading
		.lazy_load_confirm_delivery(sender_user, sender_device, &body.room_id, from)
		.await?;

	let limit = usize::try_from(body.limit).unwrap_or(10).min(100);

	let next_token;

	let mut resp = get_message_events::v3::Response::new();

	let mut lazy_loaded = HashSet::new();

	match body.dir {
		ruma::api::Direction::Forward => {
			let events_after: Vec<_> = services()
				.rooms
				.timeline
				.pdus_after(sender_user, &body.room_id, from)?
				.filter_map(Result::ok) // Filter out buggy events
				.filter(|(_, pdu)| { contains_url_filter(pdu, &body.filter) && visibility_filter(pdu, sender_user, &body.room_id)

				})
				.take_while(|&(k, _)| Some(k) != to) // Stop at `to`
				.take(limit)
				.collect();

			for (_, event) in &events_after {
				/* TODO: Remove the not "element_hacks" check when these are resolved:
				 * https://github.com/vector-im/element-android/issues/3417
				 * https://github.com/vector-im/element-web/issues/21034
				 */
				if !cfg!(feature = "element_hacks")
					&& !services().rooms.lazy_loading.lazy_load_was_sent_before(
						sender_user,
						sender_device,
						&body.room_id,
						&event.sender,
					)? {
					lazy_loaded.insert(event.sender.clone());
				}

				lazy_loaded.insert(event.sender.clone());
			}

			next_token = events_after.last().map(|(count, _)| count).copied();

			let events_after: Vec<_> = events_after
				.into_iter()
				.map(|(_, pdu)| pdu.to_room_event())
				.collect();

			resp.start = from.stringify();
			resp.end = next_token.map(|count| count.stringify());
			resp.chunk = events_after;
		},
		ruma::api::Direction::Backward => {
			services()
				.rooms
				.timeline
				.backfill_if_required(&body.room_id, from)
				.await?;
			let events_before: Vec<_> = services()
				.rooms
				.timeline
				.pdus_until(sender_user, &body.room_id, from)?
				.filter_map(Result::ok) // Filter out buggy events
				.filter(|(_, pdu)| {contains_url_filter(pdu, &body.filter) && visibility_filter(pdu, sender_user, &body.room_id)})
				.take_while(|&(k, _)| Some(k) != to) // Stop at `to`
				.take(limit)
				.collect();

			for (_, event) in &events_before {
				/* TODO: Remove the not "element_hacks" check when these are resolved:
				 * https://github.com/vector-im/element-android/issues/3417
				 * https://github.com/vector-im/element-web/issues/21034
				 */
				if !cfg!(feature = "element_hacks")
					&& !services().rooms.lazy_loading.lazy_load_was_sent_before(
						sender_user,
						sender_device,
						&body.room_id,
						&event.sender,
					)? {
					lazy_loaded.insert(event.sender.clone());
				}

				lazy_loaded.insert(event.sender.clone());
			}

			next_token = events_before.last().map(|(count, _)| count).copied();

			let events_before: Vec<_> = events_before
				.into_iter()
				.map(|(_, pdu)| pdu.to_room_event())
				.collect();

			resp.start = from.stringify();
			resp.end = next_token.map(|count| count.stringify());
			resp.chunk = events_before;
		},
	}

	resp.state = Vec::new();
	for ll_id in &lazy_loaded {
		if let Some(member_event) = services().rooms.state_accessor.room_state_get(
			&body.room_id,
			&StateEventType::RoomMember,
			ll_id.as_str(),
		)? {
			resp.state.push(member_event.to_state_event());
		}
	}

	// remove the feature check when we are sure clients like element can handle it
	if !cfg!(feature = "element_hacks") {
		if let Some(next_token) = next_token {
			services()
				.rooms
				.lazy_loading
				.lazy_load_mark_sent(sender_user, sender_device, &body.room_id, lazy_loaded, next_token)
				.await;
		}
	}

	Ok(resp)
}

fn visibility_filter(pdu: &PduEvent, user_id: &UserId, room_id: &RoomId) -> bool {
	services()
		.rooms
		.state_accessor
		.user_can_see_event(user_id, room_id, &pdu.event_id)
		.unwrap_or(false)
}

fn contains_url_filter(pdu: &PduEvent, filter: &RoomEventFilter) -> bool {
	if filter.url_filter.is_none() {
		return true;
	}

	let content: Value = from_str(pdu.content.get()).unwrap();
	match filter.url_filter {
		Some(UrlFilter::EventsWithoutUrl) => !content["url"].is_string(),
		Some(UrlFilter::EventsWithUrl) => content["url"].is_string(),
		None => true,
	}
}
