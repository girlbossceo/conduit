use std::{borrow::Borrow, iter::once};

use axum::extract::State;
use conduit::{Error, Result};
use futures::StreamExt;
use ruma::{
	api::{client::error::ErrorKind, federation::authorization::get_event_authorization},
	RoomId,
};

use super::AccessCheck;
use crate::Ruma;

/// # `GET /_matrix/federation/v1/event_auth/{roomId}/{eventId}`
///
/// Retrieves the auth chain for a given event.
///
/// - This does not include the event itself
pub(crate) async fn get_event_authorization_route(
	State(services): State<crate::State>, body: Ruma<get_event_authorization::v1::Request>,
) -> Result<get_event_authorization::v1::Response> {
	AccessCheck {
		services: &services,
		origin: body.origin(),
		room_id: &body.room_id,
		event_id: None,
	}
	.check()
	.await?;

	let event = services
		.rooms
		.timeline
		.get_pdu_json(&body.event_id)
		.await
		.map_err(|_| Error::BadRequest(ErrorKind::NotFound, "Event not found."))?;

	let room_id_str = event
		.get("room_id")
		.and_then(|val| val.as_str())
		.ok_or_else(|| Error::bad_database("Invalid event in database."))?;

	let room_id =
		<&RoomId>::try_from(room_id_str).map_err(|_| Error::bad_database("Invalid room_id in event in database."))?;

	let auth_chain = services
		.rooms
		.auth_chain
		.event_ids_iter(room_id, once(body.event_id.borrow()))
		.await?
		.filter_map(|id| async move { services.rooms.timeline.get_pdu_json(&id).await.ok() })
		.then(|pdu| services.sending.convert_to_outgoing_federation_event(pdu))
		.collect()
		.await;

	Ok(get_event_authorization::v1::Response {
		auth_chain,
	})
}
