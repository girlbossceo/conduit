mod appservice;
mod data;
mod resolve;
mod send;
mod sender;

use std::fmt::Debug;

use conduit::{err, Result};
pub use resolve::{resolve_actual_dest, CachedDest, CachedOverride, FedDest};
use ruma::{
	api::{appservice::Registration, OutgoingRequest},
	OwnedServerName, OwnedUserId, RoomId, ServerName, UserId,
};
pub use sender::convert_to_outgoing_federation_event;
use tokio::sync::Mutex;
use tracing::warn;

use crate::{server_is_ours, services};

pub struct Service {
	pub db: data::Data,

	/// The state for a given state hash.
	sender: loole::Sender<Msg>,
	receiver: Mutex<loole::Receiver<Msg>>,
	startup_netburst: bool,
	startup_netburst_keep: i64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Msg {
	dest: Destination,
	event: SendingEvent,
	queue_id: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Destination {
	Appservice(String),
	Push(OwnedUserId, String), // user and pushkey
	Normal(OwnedServerName),
}

#[allow(clippy::module_name_repetitions)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum SendingEvent {
	Pdu(Vec<u8>), // pduid
	Edu(Vec<u8>), // pdu json
	Flush,        // none
}

impl Service {
	#[tracing::instrument(skip(self, pdu_id, user, pushkey), level = "debug")]
	pub fn send_pdu_push(&self, pdu_id: &[u8], user: &UserId, pushkey: String) -> Result<()> {
		let dest = Destination::Push(user.to_owned(), pushkey);
		let event = SendingEvent::Pdu(pdu_id.to_owned());
		let _cork = services().db.cork();
		let keys = self.db.queue_requests(&[(&dest, event.clone())])?;
		self.dispatch(Msg {
			dest,
			event,
			queue_id: keys.into_iter().next().expect("request queue key"),
		})
	}

	#[tracing::instrument(skip(self), level = "debug")]
	pub fn send_pdu_appservice(&self, appservice_id: String, pdu_id: Vec<u8>) -> Result<()> {
		let dest = Destination::Appservice(appservice_id);
		let event = SendingEvent::Pdu(pdu_id);
		let _cork = services().db.cork();
		let keys = self.db.queue_requests(&[(&dest, event.clone())])?;
		self.dispatch(Msg {
			dest,
			event,
			queue_id: keys.into_iter().next().expect("request queue key"),
		})
	}

	#[tracing::instrument(skip(self, room_id, pdu_id), level = "debug")]
	pub fn send_pdu_room(&self, room_id: &RoomId, pdu_id: &[u8]) -> Result<()> {
		let servers = services()
			.rooms
			.state_cache
			.room_servers(room_id)
			.filter_map(Result::ok)
			.filter(|server_name| !server_is_ours(server_name));

		self.send_pdu_servers(servers, pdu_id)
	}

	#[tracing::instrument(skip(self, servers, pdu_id), level = "debug")]
	pub fn send_pdu_servers<I: Iterator<Item = OwnedServerName>>(&self, servers: I, pdu_id: &[u8]) -> Result<()> {
		let requests = servers
			.into_iter()
			.map(|server| (Destination::Normal(server), SendingEvent::Pdu(pdu_id.to_owned())))
			.collect::<Vec<_>>();
		let _cork = services().db.cork();
		let keys = self.db.queue_requests(
			&requests
				.iter()
				.map(|(o, e)| (o, e.clone()))
				.collect::<Vec<_>>(),
		)?;
		for ((dest, event), queue_id) in requests.into_iter().zip(keys) {
			self.dispatch(Msg {
				dest,
				event,
				queue_id,
			})?;
		}

		Ok(())
	}

	#[tracing::instrument(skip(self, server, serialized), level = "debug")]
	pub fn send_edu_server(&self, server: &ServerName, serialized: Vec<u8>) -> Result<()> {
		let dest = Destination::Normal(server.to_owned());
		let event = SendingEvent::Edu(serialized);
		let _cork = services().db.cork();
		let keys = self.db.queue_requests(&[(&dest, event.clone())])?;
		self.dispatch(Msg {
			dest,
			event,
			queue_id: keys.into_iter().next().expect("request queue key"),
		})
	}

	#[tracing::instrument(skip(self, room_id, serialized), level = "debug")]
	pub fn send_edu_room(&self, room_id: &RoomId, serialized: Vec<u8>) -> Result<()> {
		let servers = services()
			.rooms
			.state_cache
			.room_servers(room_id)
			.filter_map(Result::ok)
			.filter(|server_name| !server_is_ours(server_name));

		self.send_edu_servers(servers, serialized)
	}

	#[tracing::instrument(skip(self, servers, serialized), level = "debug")]
	pub fn send_edu_servers<I: Iterator<Item = OwnedServerName>>(&self, servers: I, serialized: Vec<u8>) -> Result<()> {
		let requests = servers
			.into_iter()
			.map(|server| (Destination::Normal(server), SendingEvent::Edu(serialized.clone())))
			.collect::<Vec<_>>();
		let _cork = services().db.cork();
		let keys = self.db.queue_requests(
			&requests
				.iter()
				.map(|(o, e)| (o, e.clone()))
				.collect::<Vec<_>>(),
		)?;

		for ((dest, event), queue_id) in requests.into_iter().zip(keys) {
			self.dispatch(Msg {
				dest,
				event,
				queue_id,
			})?;
		}

		Ok(())
	}

	#[tracing::instrument(skip(self, room_id), level = "debug")]
	pub fn flush_room(&self, room_id: &RoomId) -> Result<()> {
		let servers = services()
			.rooms
			.state_cache
			.room_servers(room_id)
			.filter_map(Result::ok)
			.filter(|server_name| !server_is_ours(server_name));

		self.flush_servers(servers)
	}

	#[tracing::instrument(skip(self, servers), level = "debug")]
	pub fn flush_servers<I: Iterator<Item = OwnedServerName>>(&self, servers: I) -> Result<()> {
		let requests = servers.into_iter().map(Destination::Normal);
		for dest in requests {
			self.dispatch(Msg {
				dest,
				event: SendingEvent::Flush,
				queue_id: Vec::<u8>::new(),
			})?;
		}

		Ok(())
	}

	#[tracing::instrument(skip(self, request), name = "request")]
	pub async fn send_federation_request<T>(&self, dest: &ServerName, request: T) -> Result<T::IncomingResponse>
	where
		T: OutgoingRequest + Debug + Send,
	{
		let client = &services().globals.client.federation;
		send::send(client, dest, request).await
	}

	/// Sends a request to an appservice
	///
	/// Only returns None if there is no url specified in the appservice
	/// registration file
	pub async fn send_appservice_request<T>(
		&self, registration: Registration, request: T,
	) -> Result<Option<T::IncomingResponse>>
	where
		T: OutgoingRequest + Debug + Send,
	{
		appservice::send_request(registration, request).await
	}

	/// Cleanup event data
	/// Used for instance after we remove an appservice registration
	#[tracing::instrument(skip(self), level = "debug")]
	pub fn cleanup_events(&self, appservice_id: String) -> Result<()> {
		self.db
			.delete_all_requests_for(&Destination::Appservice(appservice_id))?;

		Ok(())
	}

	fn dispatch(&self, msg: Msg) -> Result<()> {
		debug_assert!(!self.sender.is_full(), "channel full");
		debug_assert!(!self.sender.is_closed(), "channel closed");
		self.sender.send(msg).map_err(|e| err!("{e}"))
	}
}

impl Destination {
	#[must_use]
	pub fn get_prefix(&self) -> Vec<u8> {
		match self {
			Self::Normal(server) => {
				let len = server.as_bytes().len().saturating_add(1);

				let mut p = Vec::with_capacity(len);
				p.extend_from_slice(server.as_bytes());
				p.push(0xFF);
				p
			},
			Self::Appservice(server) => {
				let sigil = b"+";
				let len = sigil
					.len()
					.saturating_add(server.as_bytes().len())
					.saturating_add(1);

				let mut p = Vec::with_capacity(len);
				p.extend_from_slice(sigil);
				p.extend_from_slice(server.as_bytes());
				p.push(0xFF);
				p
			},
			Self::Push(user, pushkey) => {
				let sigil = b"$";
				let len = sigil
					.len()
					.saturating_add(user.as_bytes().len())
					.saturating_add(1)
					.saturating_add(pushkey.as_bytes().len())
					.saturating_add(1);

				let mut p = Vec::with_capacity(len);
				p.extend_from_slice(sigil);
				p.extend_from_slice(user.as_bytes());
				p.push(0xFF);
				p.extend_from_slice(pushkey.as_bytes());
				p.push(0xFF);
				p
			},
		}
	}
}
