use axum::extract::State;
use ruma::{
	api::client::redact::redact_event,
	events::{room::redaction::RoomRedactionEventContent, TimelineEventType},
};
use serde_json::value::to_raw_value;

use crate::{service::pdu::PduBuilder, Result, Ruma};

/// # `PUT /_matrix/client/r0/rooms/{roomId}/redact/{eventId}/{txnId}`
///
/// Tries to send a redaction event into the room.
///
/// - TODO: Handle txn id
pub(crate) async fn redact_event_route(
	State(services): State<crate::State>, body: Ruma<redact_event::v3::Request>,
) -> Result<redact_event::v3::Response> {
	let sender_user = body.sender_user.as_ref().expect("user is authenticated");
	let body = body.body;

	let state_lock = services.rooms.state.mutex.lock(&body.room_id).await;

	let event_id = services
		.rooms
		.timeline
		.build_and_append_pdu(
			PduBuilder {
				event_type: TimelineEventType::RoomRedaction,
				content: to_raw_value(&RoomRedactionEventContent {
					redacts: Some(body.event_id.clone()),
					reason: body.reason.clone(),
				})
				.expect("event is valid, we just created it"),
				unsigned: None,
				state_key: None,
				redacts: Some(body.event_id.into()),
				timestamp: None,
			},
			sender_user,
			&body.room_id,
			&state_lock,
		)
		.await?;

	drop(state_lock);

	let event_id = (*event_id).to_owned();
	Ok(redact_event::v3::Response {
		event_id,
	})
}
