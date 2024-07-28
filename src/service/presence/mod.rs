mod data;
mod presence;

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use conduit::{checked, debug, error, Error, Result, Server};
use futures_util::{stream::FuturesUnordered, StreamExt};
use ruma::{events::presence::PresenceEvent, presence::PresenceState, OwnedUserId, UInt, UserId};
use tokio::{sync::Mutex, time::sleep};

use self::{data::Data, presence::Presence};
use crate::{globals, users, Dep};

pub struct Service {
	timer_sender: loole::Sender<TimerType>,
	timer_receiver: Mutex<loole::Receiver<TimerType>>,
	timeout_remote_users: bool,
	idle_timeout: u64,
	offline_timeout: u64,
	pub db: Data,
	services: Services,
}

struct Services {
	server: Arc<Server>,
	globals: Dep<globals::Service>,
	users: Dep<users::Service>,
}

type TimerType = (OwnedUserId, Duration);

#[async_trait]
impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		let config = &args.server.config;
		let idle_timeout_s = config.presence_idle_timeout_s;
		let offline_timeout_s = config.presence_offline_timeout_s;
		let (timer_sender, timer_receiver) = loole::unbounded();
		Ok(Arc::new(Self {
			timer_sender,
			timer_receiver: Mutex::new(timer_receiver),
			timeout_remote_users: config.presence_timeout_remote_users,
			idle_timeout: checked!(idle_timeout_s * 1_000)?,
			offline_timeout: checked!(offline_timeout_s * 1_000)?,
			db: Data::new(&args),
			services: Services {
				server: args.server.clone(),
				globals: args.depend::<globals::Service>("globals"),
				users: args.depend::<users::Service>("users"),
			},
		}))
	}

	async fn worker(self: Arc<Self>) -> Result<()> {
		let mut presence_timers = FuturesUnordered::new();
		let receiver = self.timer_receiver.lock().await;
		loop {
			debug_assert!(!receiver.is_closed(), "channel error");
			tokio::select! {
				Some(user_id) = presence_timers.next() => self.process_presence_timer(&user_id)?,
				event = receiver.recv_async() => match event {
					Err(_e) => return Ok(()),
					Ok((user_id, timeout)) => {
						debug!("Adding timer {}: {user_id} timeout:{timeout:?}", presence_timers.len());
						presence_timers.push(presence_timer(user_id, timeout));
					},
				},
			}
		}
	}

	fn interrupt(&self) {
		if !self.timer_sender.is_closed() {
			self.timer_sender.close();
		}
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

impl Service {
	/// Returns the latest presence event for the given user.
	#[inline]
	pub fn get_presence(&self, user_id: &UserId) -> Result<Option<PresenceEvent>> {
		if let Some((_, presence)) = self.db.get_presence(user_id)? {
			Ok(Some(presence))
		} else {
			Ok(None)
		}
	}

	/// Pings the presence of the given user in the given room, setting the
	/// specified state.
	pub fn ping_presence(&self, user_id: &UserId, new_state: &PresenceState) -> Result<()> {
		const REFRESH_TIMEOUT: u64 = 60 * 25 * 1000;

		let last_presence = self.db.get_presence(user_id)?;
		let state_changed = match last_presence {
			None => true,
			Some((_, ref presence)) => presence.content.presence != *new_state,
		};

		let last_last_active_ago = match last_presence {
			None => 0_u64,
			Some((_, ref presence)) => presence.content.last_active_ago.unwrap_or_default().into(),
		};

		if !state_changed && last_last_active_ago < REFRESH_TIMEOUT {
			return Ok(());
		}

		let status_msg = match last_presence {
			Some((_, ref presence)) => presence.content.status_msg.clone(),
			None => Some(String::new()),
		};

		let last_active_ago = UInt::new(0);
		let currently_active = *new_state == PresenceState::Online;
		self.set_presence(user_id, new_state, Some(currently_active), last_active_ago, status_msg)
	}

	/// Adds a presence event which will be saved until a new event replaces it.
	pub fn set_presence(
		&self, user_id: &UserId, state: &PresenceState, currently_active: Option<bool>, last_active_ago: Option<UInt>,
		status_msg: Option<String>,
	) -> Result<()> {
		let presence_state = match state.as_str() {
			"" => &PresenceState::Offline, // default an empty string to 'offline'
			&_ => state,
		};

		self.db
			.set_presence(user_id, presence_state, currently_active, last_active_ago, status_msg)?;

		if self.timeout_remote_users || self.services.globals.user_is_local(user_id) {
			let timeout = match presence_state {
				PresenceState::Online => self.services.server.config.presence_idle_timeout_s,
				_ => self.services.server.config.presence_offline_timeout_s,
			};

			self.timer_sender
				.send((user_id.to_owned(), Duration::from_secs(timeout)))
				.map_err(|e| {
					error!("Failed to add presence timer: {}", e);
					Error::bad_database("Failed to add presence timer")
				})?;
		}

		Ok(())
	}

	/// Removes the presence record for the given user from the database.
	///
	/// TODO: Why is this not used?
	#[allow(dead_code)]
	pub fn remove_presence(&self, user_id: &UserId) -> Result<()> { self.db.remove_presence(user_id) }

	/// Returns the most recent presence updates that happened after the event
	/// with id `since`.
	#[inline]
	pub fn presence_since(&self, since: u64) -> Box<dyn Iterator<Item = (OwnedUserId, u64, Vec<u8>)> + '_> {
		self.db.presence_since(since)
	}

	pub fn from_json_bytes_to_event(&self, bytes: &[u8], user_id: &UserId) -> Result<PresenceEvent> {
		let presence = Presence::from_json_bytes(bytes)?;
		presence.to_presence_event(user_id, &self.services.users)
	}

	fn process_presence_timer(&self, user_id: &OwnedUserId) -> Result<()> {
		let mut presence_state = PresenceState::Offline;
		let mut last_active_ago = None;
		let mut status_msg = None;

		let presence_event = self.get_presence(user_id)?;

		if let Some(presence_event) = presence_event {
			presence_state = presence_event.content.presence;
			last_active_ago = presence_event.content.last_active_ago;
			status_msg = presence_event.content.status_msg;
		}

		let new_state = match (&presence_state, last_active_ago.map(u64::from)) {
			(PresenceState::Online, Some(ago)) if ago >= self.idle_timeout => Some(PresenceState::Unavailable),
			(PresenceState::Unavailable, Some(ago)) if ago >= self.offline_timeout => Some(PresenceState::Offline),
			_ => None,
		};

		debug!(
			"Processed presence timer for user '{user_id}': Old state = {presence_state}, New state = {new_state:?}"
		);

		if let Some(new_state) = new_state {
			self.set_presence(user_id, &new_state, Some(false), last_active_ago, status_msg)?;
		}

		Ok(())
	}
}

async fn presence_timer(user_id: OwnedUserId, timeout: Duration) -> OwnedUserId {
	sleep(timeout).await;

	user_id
}
