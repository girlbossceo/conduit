use axum::extract::State;
use conduit::{at, utils::BoolExt, Err, Result};
use futures::StreamExt;
use ruma::api::client::room::initial_sync::v3::{PaginationChunk, Request, Response};

use crate::Ruma;

const LIMIT_MAX: usize = 100;

pub(crate) async fn room_initial_sync_route(
	State(services): State<crate::State>, body: Ruma<Request>,
) -> Result<Response> {
	let room_id = &body.room_id;

	if !services
		.rooms
		.state_accessor
		.user_can_see_state_events(body.sender_user(), room_id)
		.await
	{
		return Err!(Request(Forbidden("No room preview available.")));
	}

	let limit = LIMIT_MAX;
	let events: Vec<_> = services
		.rooms
		.timeline
		.pdus_rev(None, room_id, None)
		.await?
		.take(limit)
		.collect()
		.await;

	let state: Vec<_> = services
		.rooms
		.state_accessor
		.room_state_full_pdus(room_id)
		.await?
		.into_iter()
		.map(|pdu| pdu.to_state_event())
		.collect();

	let messages = PaginationChunk {
		start: events.last().map(at!(0)).as_ref().map(ToString::to_string),

		end: events
			.first()
			.map(at!(0))
			.as_ref()
			.map(ToString::to_string)
			.unwrap_or_default(),

		chunk: events
			.into_iter()
			.map(at!(1))
			.map(|pdu| pdu.to_room_event())
			.collect(),
	};

	Ok(Response {
		room_id: room_id.to_owned(),
		account_data: None,
		state: state.into(),
		messages: messages.chunk.is_empty().or_some(messages),
		visibility: services.rooms.directory.visibility(room_id).await.into(),
		membership: services
			.rooms
			.state_cache
			.user_membership(body.sender_user(), room_id)
			.await,
	})
}
