mod v3;
mod v4;

use conduit::{utils::ReadyExt, PduCount};
use futures::StreamExt;
use ruma::{RoomId, UserId};

pub(crate) use self::{v3::sync_events_route, v4::sync_events_v4_route};
use crate::{service::Services, Error, PduEvent, Result};

async fn load_timeline(
	services: &Services, sender_user: &UserId, room_id: &RoomId, roomsincecount: PduCount,
	next_batch: Option<PduCount>, limit: usize,
) -> Result<(Vec<(PduCount, PduEvent)>, bool), Error> {
	let last_timeline_count = services
		.rooms
		.timeline
		.last_timeline_count(Some(sender_user), room_id)
		.await?;

	if last_timeline_count <= roomsincecount {
		return Ok((Vec::new(), false));
	}

	let mut non_timeline_pdus = services
		.rooms
		.timeline
		.pdus_rev(Some(sender_user), room_id, None)
		.await?
		.ready_skip_while(|&(pducount, _)| pducount > next_batch.unwrap_or_else(PduCount::max))
		.ready_take_while(|&(pducount, _)| pducount > roomsincecount);

	// Take the last events for the timeline
	let timeline_pdus: Vec<_> = non_timeline_pdus
		.by_ref()
		.take(limit)
		.collect::<Vec<_>>()
		.await
		.into_iter()
		.rev()
		.collect();

	// They /sync response doesn't always return all messages, so we say the output
	// is limited unless there are events in non_timeline_pdus
	let limited = non_timeline_pdus.next().await.is_some();

	Ok((timeline_pdus, limited))
}

async fn share_encrypted_room(
	services: &Services, sender_user: &UserId, user_id: &UserId, ignore_room: Option<&RoomId>,
) -> bool {
	services
		.rooms
		.state_cache
		.get_shared_rooms(sender_user, user_id)
		.ready_filter(|&room_id| Some(room_id) != ignore_room)
		.any(|other_room_id| {
			services
				.rooms
				.state_accessor
				.is_encrypted_room(other_room_id)
		})
		.await
}
