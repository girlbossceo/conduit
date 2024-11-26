use std::{collections::HashMap, sync::Arc};

use conduit::{
	at, err,
	utils::stream::{IterStream, ReadyExt},
	PduEvent, Result,
};
use database::{Deserialized, Map};
use futures::{StreamExt, TryFutureExt};
use ruma::{events::StateEventType, EventId, RoomId};

use crate::{
	rooms,
	rooms::{
		short::{ShortEventId, ShortStateHash, ShortStateKey},
		state_compressor::parse_compressed_state_event,
	},
	Dep,
};

pub(super) struct Data {
	eventid_shorteventid: Arc<Map>,
	shorteventid_shortstatehash: Arc<Map>,
	services: Services,
}

struct Services {
	short: Dep<rooms::short::Service>,
	state: Dep<rooms::state::Service>,
	state_compressor: Dep<rooms::state_compressor::Service>,
	timeline: Dep<rooms::timeline::Service>,
}

impl Data {
	pub(super) fn new(args: &crate::Args<'_>) -> Self {
		let db = &args.db;
		Self {
			eventid_shorteventid: db["eventid_shorteventid"].clone(),
			shorteventid_shortstatehash: db["shorteventid_shortstatehash"].clone(),
			services: Services {
				short: args.depend::<rooms::short::Service>("rooms::short"),
				state: args.depend::<rooms::state::Service>("rooms::state"),
				state_compressor: args.depend::<rooms::state_compressor::Service>("rooms::state_compressor"),
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
			},
		}
	}

	pub(super) async fn state_full(
		&self, shortstatehash: ShortStateHash,
	) -> Result<HashMap<(StateEventType, String), Arc<PduEvent>>> {
		let state = self
			.state_full_pdus(shortstatehash)
			.await?
			.into_iter()
			.filter_map(|pdu| Some(((pdu.kind.to_string().into(), pdu.state_key.clone()?), pdu)))
			.collect();

		Ok(state)
	}

	pub(super) async fn state_full_pdus(&self, shortstatehash: ShortStateHash) -> Result<Vec<Arc<PduEvent>>> {
		let short_ids = self
			.state_full_shortids(shortstatehash)
			.await?
			.into_iter()
			.map(at!(1));

		let event_ids = self
			.services
			.short
			.multi_get_eventid_from_short(short_ids)
			.await;

		let full_pdus = event_ids
			.into_iter()
			.stream()
			.ready_filter_map(Result::ok)
			.filter_map(|event_id| async move { self.services.timeline.get_pdu(&event_id).await.ok() })
			.collect()
			.await;

		Ok(full_pdus)
	}

	pub(super) async fn state_full_ids(&self, shortstatehash: ShortStateHash) -> Result<HashMap<u64, Arc<EventId>>> {
		let short_ids = self.state_full_shortids(shortstatehash).await?;

		let event_ids = self
			.services
			.short
			.multi_get_eventid_from_short(short_ids.iter().map(at!(1)))
			.await;

		let full_ids = short_ids
			.into_iter()
			.map(at!(0))
			.zip(event_ids.into_iter())
			.filter_map(|(shortstatekey, event_id)| Some((shortstatekey, event_id.ok()?)))
			.collect();

		Ok(full_ids)
	}

	pub(super) async fn state_full_shortids(
		&self, shortstatehash: ShortStateHash,
	) -> Result<Vec<(ShortStateKey, ShortEventId)>> {
		let shortids = self
			.services
			.state_compressor
			.load_shortstatehash_info(shortstatehash)
			.await
			.map_err(|e| err!(Database("Missing state IDs: {e}")))?
			.pop()
			.expect("there is always one layer")
			.full_state
			.iter()
			.copied()
			.map(parse_compressed_state_event)
			.collect();

		Ok(shortids)
	}

	/// Returns a single PDU from `room_id` with key (`event_type`,`state_key`).
	#[allow(clippy::unused_self)]
	pub(super) async fn state_get_id(
		&self, shortstatehash: ShortStateHash, event_type: &StateEventType, state_key: &str,
	) -> Result<Arc<EventId>> {
		let shortstatekey = self
			.services
			.short
			.get_shortstatekey(event_type, state_key)
			.await?;

		let full_state = self
			.services
			.state_compressor
			.load_shortstatehash_info(shortstatehash)
			.await
			.map_err(|e| err!(Database(error!(?event_type, ?state_key, "Missing state: {e:?}"))))?
			.pop()
			.expect("there is always one layer")
			.full_state;

		let compressed = full_state
			.iter()
			.find(|bytes| bytes.starts_with(&shortstatekey.to_be_bytes()))
			.ok_or(err!(Database("No shortstatekey in compressed state")))?;

		let (_, shorteventid) = parse_compressed_state_event(*compressed);

		self.services
			.short
			.get_eventid_from_short(shorteventid)
			.await
	}

	/// Returns a single PDU from `room_id` with key (`event_type`,`state_key`).
	pub(super) async fn state_get(
		&self, shortstatehash: ShortStateHash, event_type: &StateEventType, state_key: &str,
	) -> Result<Arc<PduEvent>> {
		self.state_get_id(shortstatehash, event_type, state_key)
			.and_then(|event_id| async move { self.services.timeline.get_pdu(&event_id).await })
			.await
	}

	/// Returns the state hash for this pdu.
	pub(super) async fn pdu_shortstatehash(&self, event_id: &EventId) -> Result<ShortStateHash> {
		self.eventid_shorteventid
			.get(event_id)
			.and_then(|shorteventid| self.shorteventid_shortstatehash.get(&shorteventid))
			.await
			.deserialized()
	}

	/// Returns the full room state.
	#[allow(unused_qualifications)] // async traits
	pub(super) async fn room_state_full(
		&self, room_id: &RoomId,
	) -> Result<HashMap<(StateEventType, String), Arc<PduEvent>>> {
		self.services
			.state
			.get_room_shortstatehash(room_id)
			.and_then(|shortstatehash| self.state_full(shortstatehash))
			.map_err(|e| err!(Database("Missing state for {room_id:?}: {e:?}")))
			.await
	}

	/// Returns the full room state's pdus.
	#[allow(unused_qualifications)] // async traits
	pub(super) async fn room_state_full_pdus(&self, room_id: &RoomId) -> Result<Vec<Arc<PduEvent>>> {
		self.services
			.state
			.get_room_shortstatehash(room_id)
			.and_then(|shortstatehash| self.state_full_pdus(shortstatehash))
			.map_err(|e| err!(Database("Missing state pdus for {room_id:?}: {e:?}")))
			.await
	}

	/// Returns a single PDU from `room_id` with key (`event_type`,`state_key`).
	pub(super) async fn room_state_get_id(
		&self, room_id: &RoomId, event_type: &StateEventType, state_key: &str,
	) -> Result<Arc<EventId>> {
		self.services
			.state
			.get_room_shortstatehash(room_id)
			.and_then(|shortstatehash| self.state_get_id(shortstatehash, event_type, state_key))
			.await
	}

	/// Returns a single PDU from `room_id` with key (`event_type`,`state_key`).
	pub(super) async fn room_state_get(
		&self, room_id: &RoomId, event_type: &StateEventType, state_key: &str,
	) -> Result<Arc<PduEvent>> {
		self.services
			.state
			.get_room_shortstatehash(room_id)
			.and_then(|shortstatehash| self.state_get(shortstatehash, event_type, state_key))
			.await
	}
}
