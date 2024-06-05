use std::sync::Arc;

use ruma::{
	api::{client::error::ErrorKind, federation::membership::prepare_leave_event},
	events::{
		room::member::{MembershipState, RoomMemberEventContent},
		TimelineEventType,
	},
	RoomVersionId,
};
use serde_json::value::to_raw_value;

use crate::{service::pdu::PduBuilder, services, Error, Result, Ruma};

/// # `PUT /_matrix/federation/v1/make_leave/{roomId}/{eventId}`
///
/// Creates a leave template.
pub(crate) async fn create_leave_event_template_route(
	body: Ruma<prepare_leave_event::v1::Request>,
) -> Result<prepare_leave_event::v1::Response> {
	if !services().rooms.metadata.exists(&body.room_id)? {
		return Err(Error::BadRequest(ErrorKind::NotFound, "Room is unknown to this server."));
	}

	let origin = body.origin.as_ref().expect("server is authenticated");
	if body.user_id.server_name() != origin {
		return Err(Error::BadRequest(
			ErrorKind::InvalidParam,
			"Not allowed to leave on behalf of another server/user",
		));
	}

	// ACL check origin
	services()
		.rooms
		.event_handler
		.acl_check(origin, &body.room_id)?;

	let room_version_id = services().rooms.state.get_room_version(&body.room_id)?;

	let mutex_state = Arc::clone(
		services()
			.globals
			.roomid_mutex_state
			.write()
			.await
			.entry(body.room_id.clone())
			.or_default(),
	);
	let state_lock = mutex_state.lock().await;

	let content = to_raw_value(&RoomMemberEventContent {
		avatar_url: None,
		blurhash: None,
		displayname: None,
		is_direct: None,
		membership: MembershipState::Leave,
		third_party_invite: None,
		reason: None,
		join_authorized_via_users_server: None,
	})
	.expect("member event is valid value");

	let (_pdu, mut pdu_json) = services().rooms.timeline.create_hash_and_sign_event(
		PduBuilder {
			event_type: TimelineEventType::RoomMember,
			content,
			unsigned: None,
			state_key: Some(body.user_id.to_string()),
			redacts: None,
		},
		&body.user_id,
		&body.room_id,
		&state_lock,
	)?;

	drop(state_lock);

	// room v3 and above removed the "event_id" field from remote PDU format
	match room_version_id {
		RoomVersionId::V1 | RoomVersionId::V2 => {},
		RoomVersionId::V3
		| RoomVersionId::V4
		| RoomVersionId::V5
		| RoomVersionId::V6
		| RoomVersionId::V7
		| RoomVersionId::V8
		| RoomVersionId::V9
		| RoomVersionId::V10
		| RoomVersionId::V11 => {
			pdu_json.remove("event_id");
		},
		_ => {
			return Err(Error::BadRequest(
				ErrorKind::BadJson,
				"Unexpected or unsupported room version found",
			));
		},
	};

	Ok(prepare_leave_event::v1::Response {
		room_version: Some(room_version_id),
		event: to_raw_value(&pdu_json).expect("CanonicalJson can be serialized to JSON"),
	})
}
