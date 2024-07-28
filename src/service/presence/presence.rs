use std::sync::Arc;

use conduit::{utils, Error, Result};
use ruma::{
	events::presence::{PresenceEvent, PresenceEventContent},
	presence::PresenceState,
	UInt, UserId,
};
use serde::{Deserialize, Serialize};

use crate::users;

/// Represents data required to be kept in order to implement the presence
/// specification.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub(super) struct Presence {
	state: PresenceState,
	currently_active: bool,
	last_active_ts: u64,
	status_msg: Option<String>,
}

impl Presence {
	#[must_use]
	pub(super) fn new(
		state: PresenceState, currently_active: bool, last_active_ts: u64, status_msg: Option<String>,
	) -> Self {
		Self {
			state,
			currently_active,
			last_active_ts,
			status_msg,
		}
	}

	pub(super) fn from_json_bytes(bytes: &[u8]) -> Result<Self> {
		serde_json::from_slice(bytes).map_err(|_| Error::bad_database("Invalid presence data in database"))
	}

	pub(super) fn to_json_bytes(&self) -> Result<Vec<u8>> {
		serde_json::to_vec(self).map_err(|_| Error::bad_database("Could not serialize Presence to JSON"))
	}

	/// Creates a PresenceEvent from available data.
	pub(super) fn to_presence_event(&self, user_id: &UserId, users: &Arc<users::Service>) -> Result<PresenceEvent> {
		let now = utils::millis_since_unix_epoch();
		let last_active_ago = if self.currently_active {
			None
		} else {
			Some(UInt::new_saturating(now.saturating_sub(self.last_active_ts)))
		};

		Ok(PresenceEvent {
			sender: user_id.to_owned(),
			content: PresenceEventContent {
				presence: self.state.clone(),
				status_msg: self.status_msg.clone(),
				currently_active: Some(self.currently_active),
				last_active_ago,
				displayname: users.displayname(user_id)?,
				avatar_url: users.avatar_url(user_id)?,
			},
		})
	}
}
