use std::{borrow::Borrow, iter::once};

use axum::extract::State;
use conduit::{err, result::LogErr, utils::IterStream, Result};
use futures::{FutureExt, StreamExt, TryStreamExt};
use ruma::api::federation::event::get_room_state;

use super::AccessCheck;
use crate::Ruma;

/// # `GET /_matrix/federation/v1/state/{roomId}`
///
/// Retrieves a snapshot of a room's state at a given event.
pub(crate) async fn get_room_state_route(
	State(services): State<crate::State>, body: Ruma<get_room_state::v1::Request>,
) -> Result<get_room_state::v1::Response> {
	AccessCheck {
		services: &services,
		origin: body.origin(),
		room_id: &body.room_id,
		event_id: None,
	}
	.check()
	.await?;

	let shortstatehash = services
		.rooms
		.state_accessor
		.pdu_shortstatehash(&body.event_id)
		.await
		.map_err(|_| err!(Request(NotFound("PDU state not found."))))?;

	let pdus = services
		.rooms
		.state_accessor
		.state_full_ids(shortstatehash)
		.await
		.log_err()
		.map_err(|_| err!(Request(NotFound("PDU state IDs not found."))))?
		.values()
		.try_stream()
		.and_then(|id| services.rooms.timeline.get_pdu_json(id))
		.and_then(|pdu| {
			services
				.sending
				.convert_to_outgoing_federation_event(pdu)
				.map(Ok)
		})
		.try_collect()
		.await?;

	let auth_chain = services
		.rooms
		.auth_chain
		.event_ids_iter(&body.room_id, once(body.event_id.borrow()))
		.await?
		.map(Ok)
		.and_then(|id| async move { services.rooms.timeline.get_pdu_json(&id).await })
		.and_then(|pdu| {
			services
				.sending
				.convert_to_outgoing_federation_event(pdu)
				.map(Ok)
		})
		.try_collect()
		.await?;

	Ok(get_room_state::v1::Response {
		auth_chain,
		pdus,
	})
}
