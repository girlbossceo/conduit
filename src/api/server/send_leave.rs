#![allow(deprecated)]

use std::collections::BTreeMap;

use ruma::{
	api::{client::error::ErrorKind, federation::membership::create_leave_event},
	events::{
		room::member::{MembershipState, RoomMemberEventContent},
		StateEventType,
	},
	OwnedServerName, OwnedUserId, RoomId, ServerName,
};
use serde_json::value::RawValue as RawJsonValue;
use tokio::sync::RwLock;

use crate::{
	service::{pdu::gen_event_id_canonical_json, server_is_ours},
	services, Error, Result, Ruma,
};

/// # `PUT /_matrix/federation/v1/send_leave/{roomId}/{eventId}`
///
/// Submits a signed leave event.
pub(crate) async fn create_leave_event_v1_route(
	body: Ruma<create_leave_event::v1::Request>,
) -> Result<create_leave_event::v1::Response> {
	let origin = body.origin.as_ref().expect("server is authenticated");

	create_leave_event(origin, &body.room_id, &body.pdu).await?;

	Ok(create_leave_event::v1::Response::new())
}

/// # `PUT /_matrix/federation/v2/send_leave/{roomId}/{eventId}`
///
/// Submits a signed leave event.
pub(crate) async fn create_leave_event_v2_route(
	body: Ruma<create_leave_event::v2::Request>,
) -> Result<create_leave_event::v2::Response> {
	let origin = body.origin.as_ref().expect("server is authenticated");

	create_leave_event(origin, &body.room_id, &body.pdu).await?;

	Ok(create_leave_event::v2::Response::new())
}

async fn create_leave_event(origin: &ServerName, room_id: &RoomId, pdu: &RawJsonValue) -> Result<()> {
	if !services().rooms.metadata.exists(room_id)? {
		return Err(Error::BadRequest(ErrorKind::NotFound, "Room is unknown to this server."));
	}

	// ACL check origin
	services().rooms.event_handler.acl_check(origin, room_id)?;

	let pub_key_map = RwLock::new(BTreeMap::new());

	// We do not add the event_id field to the pdu here because of signature and
	// hashes checks
	let room_version_id = services().rooms.state.get_room_version(room_id)?;
	let Ok((event_id, value)) = gen_event_id_canonical_json(pdu, &room_version_id) else {
		// Event could not be converted to canonical json
		return Err(Error::BadRequest(
			ErrorKind::InvalidParam,
			"Could not convert event to canonical json.",
		));
	};

	let content: RoomMemberEventContent = serde_json::from_value(
		value
			.get("content")
			.ok_or_else(|| Error::BadRequest(ErrorKind::InvalidParam, "Event missing content property"))?
			.clone()
			.into(),
	)
	.map_err(|_| Error::BadRequest(ErrorKind::InvalidParam, "Event content is empty or invalid"))?;

	if content.membership != MembershipState::Leave {
		return Err(Error::BadRequest(
			ErrorKind::InvalidParam,
			"Not allowed to send a non-leave membership event to leave endpoint.",
		));
	}

	let event_type: StateEventType = serde_json::from_value(
		value
			.get("type")
			.ok_or_else(|| Error::BadRequest(ErrorKind::InvalidParam, "Event missing type property."))?
			.clone()
			.into(),
	)
	.map_err(|_| Error::BadRequest(ErrorKind::BadJson, "Event does not have a valid state event type."))?;

	if event_type != StateEventType::RoomMember {
		return Err(Error::BadRequest(
			ErrorKind::InvalidParam,
			"Not allowed to send non-membership state event to leave endpoint.",
		));
	}

	// ACL check sender server name
	let sender: OwnedUserId = serde_json::from_value(
		value
			.get("sender")
			.ok_or_else(|| Error::BadRequest(ErrorKind::InvalidParam, "Event missing sender property."))?
			.clone()
			.into(),
	)
	.map_err(|_| Error::BadRequest(ErrorKind::BadJson, "User ID in sender is invalid."))?;

	services()
		.rooms
		.event_handler
		.acl_check(sender.server_name(), room_id)?;

	if sender.server_name() != origin {
		return Err(Error::BadRequest(
			ErrorKind::InvalidParam,
			"Not allowed to leave on behalf of another server.",
		));
	}

	let state_key: OwnedUserId = serde_json::from_value(
		value
			.get("state_key")
			.ok_or_else(|| Error::BadRequest(ErrorKind::InvalidParam, "Event missing state_key property."))?
			.clone()
			.into(),
	)
	.map_err(|_| Error::BadRequest(ErrorKind::BadJson, "state_key is invalid or not a user ID"))?;

	if state_key != sender {
		return Err(Error::BadRequest(
			ErrorKind::InvalidParam,
			"state_key does not match sender user.",
		));
	}

	let origin: OwnedServerName = serde_json::from_value(
		serde_json::to_value(
			value
				.get("origin")
				.ok_or_else(|| Error::BadRequest(ErrorKind::InvalidParam, "Event missing origin property."))?,
		)
		.expect("CanonicalJson is valid json value"),
	)
	.map_err(|_| Error::BadRequest(ErrorKind::InvalidParam, "origin is not a server name."))?;

	services()
		.rooms
		.event_handler
		.fetch_required_signing_keys([&value], &pub_key_map)
		.await?;

	let mutex_lock = services()
		.rooms
		.event_handler
		.mutex_federation
		.lock(room_id)
		.await;
	let pdu_id: Vec<u8> = services()
		.rooms
		.event_handler
		.handle_incoming_pdu(&origin, room_id, &event_id, value, true, &pub_key_map)
		.await?
		.ok_or_else(|| Error::BadRequest(ErrorKind::InvalidParam, "Could not accept as timeline event."))?;

	drop(mutex_lock);

	let servers = services()
		.rooms
		.state_cache
		.room_servers(room_id)
		.filter_map(Result::ok)
		.filter(|server| !server_is_ours(server));

	services().sending.send_pdu_servers(servers, &pdu_id)?;

	Ok(())
}
