use api::client::leave_room;
use clap::Subcommand;
use conduit::{debug, error, info, warn, Result};
use ruma::{events::room::message::RoomMessageEventContent, OwnedRoomId, RoomAliasId, RoomId, RoomOrAliasId};

use crate::{admin_command, admin_command_dispatch, get_room_info};

#[admin_command_dispatch]
#[derive(Debug, Subcommand)]
pub(crate) enum RoomModerationCommand {
	/// - Bans a room from local users joining and evicts all our local users
	///   from the room. Also blocks any invites (local and remote) for the
	///   banned room.
	///
	/// Server admins (users in the conduwuit admin room) will not be evicted
	/// and server admins can still join the room. To evict admins too, use
	/// --force (also ignores errors) To disable incoming federation of the
	/// room, use --disable-federation
	BanRoom {
		#[arg(short, long)]
		/// Evicts admins out of the room and ignores any potential errors when
		/// making our local users leave the room
		force: bool,

		#[arg(long)]
		/// Disables incoming federation of the room after banning and evicting
		/// users
		disable_federation: bool,

		/// The room in the format of `!roomid:example.com` or a room alias in
		/// the format of `#roomalias:example.com`
		room: Box<RoomOrAliasId>,
	},

	/// - Bans a list of rooms (room IDs and room aliases) from a newline
	///   delimited codeblock similar to `user deactivate-all`
	BanListOfRooms {
		#[arg(short, long)]
		/// Evicts admins out of the room and ignores any potential errors when
		/// making our local users leave the room
		force: bool,

		#[arg(long)]
		/// Disables incoming federation of the room after banning and evicting
		/// users
		disable_federation: bool,
	},

	/// - Unbans a room to allow local users to join again
	///
	/// To re-enable incoming federation of the room, use --enable-federation
	UnbanRoom {
		#[arg(long)]
		/// Enables incoming federation of the room after unbanning
		enable_federation: bool,

		/// The room in the format of `!roomid:example.com` or a room alias in
		/// the format of `#roomalias:example.com`
		room: Box<RoomOrAliasId>,
	},

	/// - List of all rooms we have banned
	ListBannedRooms,
}

#[admin_command]
async fn ban_room(
	&self, force: bool, disable_federation: bool, room: Box<RoomOrAliasId>,
) -> Result<RoomMessageEventContent> {
	debug!("Got room alias or ID: {}", room);

	let admin_room_alias = &self.services.globals.admin_alias;

	if let Some(admin_room_id) = self.services.admin.get_admin_room()? {
		if room.to_string().eq(&admin_room_id) || room.to_string().eq(admin_room_alias) {
			return Ok(RoomMessageEventContent::text_plain("Not allowed to ban the admin room."));
		}
	}

	let room_id = if room.is_room_id() {
		let room_id = match RoomId::parse(&room) {
			Ok(room_id) => room_id,
			Err(e) => {
				return Ok(RoomMessageEventContent::text_plain(format!(
					"Failed to parse room ID {room}. Please note that this requires a full room ID \
					 (`!awIh6gGInaS5wLQJwa:example.com`) or a room alias (`#roomalias:example.com`): {e}"
				)))
			},
		};

		debug!("Room specified is a room ID, banning room ID");

		self.services.rooms.metadata.ban_room(&room_id, true)?;

		room_id
	} else if room.is_room_alias_id() {
		let room_alias = match RoomAliasId::parse(&room) {
			Ok(room_alias) => room_alias,
			Err(e) => {
				return Ok(RoomMessageEventContent::text_plain(format!(
					"Failed to parse room ID {room}. Please note that this requires a full room ID \
					 (`!awIh6gGInaS5wLQJwa:example.com`) or a room alias (`#roomalias:example.com`): {e}"
				)))
			},
		};

		debug!(
			"Room specified is not a room ID, attempting to resolve room alias to a room ID locally, if not using \
			 get_alias_helper to fetch room ID remotely"
		);

		let room_id = if let Some(room_id) = self.services.rooms.alias.resolve_local_alias(&room_alias)? {
			room_id
		} else {
			debug!("We don't have this room alias to a room ID locally, attempting to fetch room ID over federation");

			match self
				.services
				.rooms
				.alias
				.resolve_alias(&room_alias, None)
				.await
			{
				Ok((room_id, servers)) => {
					debug!(?room_id, ?servers, "Got federation response fetching room ID for {room}");
					room_id
				},
				Err(e) => {
					return Ok(RoomMessageEventContent::notice_plain(format!(
						"Failed to resolve room alias {room} to a room ID: {e}"
					)));
				},
			}
		};

		self.services.rooms.metadata.ban_room(&room_id, true)?;

		room_id
	} else {
		return Ok(RoomMessageEventContent::text_plain(
			"Room specified is not a room ID or room alias. Please note that this requires a full room ID \
			 (`!awIh6gGInaS5wLQJwa:example.com`) or a room alias (`#roomalias:example.com`)",
		));
	};

	debug!("Making all users leave the room {}", &room);
	if force {
		for local_user in self
			.services
			.rooms
			.state_cache
			.room_members(&room_id)
			.filter_map(|user| {
				user.ok().filter(|local_user| {
					self.services.globals.user_is_local(local_user)
								// additional wrapped check here is to avoid adding remote users
								// who are in the admin room to the list of local users (would
								// fail auth check)
								&& (self.services.globals.user_is_local(local_user)
									// since this is a force operation, assume user is an admin
									// if somehow this fails
									&& self.services
										.users
										.is_admin(local_user)
										.unwrap_or(true))
				})
			}) {
			debug!(
				"Attempting leave for user {} in room {} (forced, ignoring all errors, evicting admins too)",
				&local_user, &room_id
			);

			if let Err(e) = leave_room(self.services, &local_user, &room_id, None).await {
				warn!(%e, "Failed to leave room");
			}
		}
	} else {
		for local_user in self
			.services
			.rooms
			.state_cache
			.room_members(&room_id)
			.filter_map(|user| {
				user.ok().filter(|local_user| {
					local_user.server_name() == self.services.globals.server_name()
								// additional wrapped check here is to avoid adding remote users
								// who are in the admin room to the list of local users (would fail auth check)
								&& (local_user.server_name()
									== self.services.globals.server_name()
									&& !self.services
										.users
										.is_admin(local_user)
										.unwrap_or(false))
				})
			}) {
			debug!("Attempting leave for user {} in room {}", &local_user, &room_id);
			if let Err(e) = leave_room(self.services, &local_user, &room_id, None).await {
				error!(
					"Error attempting to make local user {} leave room {} during room banning: {}",
					&local_user, &room_id, e
				);
				return Ok(RoomMessageEventContent::text_plain(format!(
					"Error attempting to make local user {} leave room {} during room banning (room is still banned \
					 but not removing any more users): {}\nIf you would like to ignore errors, use --force",
					&local_user, &room_id, e
				)));
			}
		}
	}

	// remove any local aliases, ignore errors
	for ref local_alias in self
		.services
		.rooms
		.alias
		.local_aliases_for_room(&room_id)
		.filter_map(Result::ok)
	{
		_ = self
			.services
			.rooms
			.alias
			.remove_alias(local_alias, &self.services.globals.server_user)
			.await;
	}

	// unpublish from room directory, ignore errors
	_ = self.services.rooms.directory.set_not_public(&room_id);

	if disable_federation {
		self.services.rooms.metadata.disable_room(&room_id, true)?;
		return Ok(RoomMessageEventContent::text_plain(
			"Room banned, removed all our local users, and disabled incoming federation with room.",
		));
	}

	Ok(RoomMessageEventContent::text_plain(
		"Room banned and removed all our local users, use `!admin federation disable-room` to stop receiving new \
		 inbound federation events as well if needed.",
	))
}

#[admin_command]
async fn ban_list_of_rooms(&self, force: bool, disable_federation: bool) -> Result<RoomMessageEventContent> {
	if self.body.len() < 2 || !self.body[0].trim().starts_with("```") || self.body.last().unwrap_or(&"").trim() != "```"
	{
		return Ok(RoomMessageEventContent::text_plain(
			"Expected code block in command body. Add --help for details.",
		));
	}

	let rooms_s = self
		.body
		.to_vec()
		.drain(1..self.body.len().saturating_sub(1))
		.collect::<Vec<_>>();

	let admin_room_alias = &self.services.globals.admin_alias;

	let mut room_ban_count: usize = 0;
	let mut room_ids: Vec<OwnedRoomId> = Vec::new();

	for &room in &rooms_s {
		match <&RoomOrAliasId>::try_from(room) {
			Ok(room_alias_or_id) => {
				if let Some(admin_room_id) = self.services.admin.get_admin_room()? {
					if room.to_owned().eq(&admin_room_id) || room.to_owned().eq(admin_room_alias) {
						info!("User specified admin room in bulk ban list, ignoring");
						continue;
					}
				}

				if room_alias_or_id.is_room_id() {
					let room_id = match RoomId::parse(room_alias_or_id) {
						Ok(room_id) => room_id,
						Err(e) => {
							if force {
								// ignore rooms we failed to parse if we're force banning
								warn!(
									"Error parsing room \"{room}\" during bulk room banning, ignoring error and \
									 logging here: {e}"
								);
								continue;
							}

							return Ok(RoomMessageEventContent::text_plain(format!(
								"{room} is not a valid room ID or room alias, please fix the list and try again: {e}"
							)));
						},
					};

					room_ids.push(room_id);
				}

				if room_alias_or_id.is_room_alias_id() {
					match RoomAliasId::parse(room_alias_or_id) {
						Ok(room_alias) => {
							let room_id =
								if let Some(room_id) = self.services.rooms.alias.resolve_local_alias(&room_alias)? {
									room_id
								} else {
									debug!(
										"We don't have this room alias to a room ID locally, attempting to fetch room \
										 ID over federation"
									);

									match self
										.services
										.rooms
										.alias
										.resolve_alias(&room_alias, None)
										.await
									{
										Ok((room_id, servers)) => {
											debug!(
												?room_id,
												?servers,
												"Got federation response fetching room ID for {room}",
											);
											room_id
										},
										Err(e) => {
											// don't fail if force blocking
											if force {
												warn!("Failed to resolve room alias {room} to a room ID: {e}");
												continue;
											}

											return Ok(RoomMessageEventContent::text_plain(format!(
												"Failed to resolve room alias {room} to a room ID: {e}"
											)));
										},
									}
								};

							room_ids.push(room_id);
						},
						Err(e) => {
							if force {
								// ignore rooms we failed to parse if we're force deleting
								error!(
									"Error parsing room \"{room}\" during bulk room banning, ignoring error and \
									 logging here: {e}"
								);
								continue;
							}

							return Ok(RoomMessageEventContent::text_plain(format!(
								"{room} is not a valid room ID or room alias, please fix the list and try again: {e}"
							)));
						},
					}
				}
			},
			Err(e) => {
				if force {
					// ignore rooms we failed to parse if we're force deleting
					error!(
						"Error parsing room \"{room}\" during bulk room banning, ignoring error and logging here: {e}"
					);
					continue;
				}

				return Ok(RoomMessageEventContent::text_plain(format!(
					"{room} is not a valid room ID or room alias, please fix the list and try again: {e}"
				)));
			},
		}
	}

	for room_id in room_ids {
		if self
			.services
			.rooms
			.metadata
			.ban_room(&room_id, true)
			.is_ok()
		{
			debug!("Banned {room_id} successfully");
			room_ban_count = room_ban_count.saturating_add(1);
		}

		debug!("Making all users leave the room {}", &room_id);
		if force {
			for local_user in self
				.services
				.rooms
				.state_cache
				.room_members(&room_id)
				.filter_map(|user| {
					user.ok().filter(|local_user| {
						local_user.server_name() == self.services.globals.server_name()
										// additional wrapped check here is to avoid adding remote
										// users who are in the admin room to the list of local
										// users (would fail auth check)
										&& (local_user.server_name()
											== self.services.globals.server_name()
											// since this is a force operation, assume user is an
											// admin if somehow this fails
											&& self.services
												.users
												.is_admin(local_user)
												.unwrap_or(true))
					})
				}) {
				debug!(
					"Attempting leave for user {} in room {} (forced, ignoring all errors, evicting admins too)",
					&local_user, room_id
				);
				if let Err(e) = leave_room(self.services, &local_user, &room_id, None).await {
					warn!(%e, "Failed to leave room");
				}
			}
		} else {
			for local_user in self
				.services
				.rooms
				.state_cache
				.room_members(&room_id)
				.filter_map(|user| {
					user.ok().filter(|local_user| {
						local_user.server_name() == self.services.globals.server_name()
										// additional wrapped check here is to avoid adding remote
										// users who are in the admin room to the list of local
										// users (would fail auth check)
										&& (local_user.server_name()
											== self.services.globals.server_name()
											&& !self.services
												.users
												.is_admin(local_user)
												.unwrap_or(false))
					})
				}) {
				debug!("Attempting leave for user {} in room {}", &local_user, &room_id);
				if let Err(e) = leave_room(self.services, &local_user, &room_id, None).await {
					error!(
						"Error attempting to make local user {} leave room {} during bulk room banning: {}",
						&local_user, &room_id, e
					);
					return Ok(RoomMessageEventContent::text_plain(format!(
						"Error attempting to make local user {} leave room {} during room banning (room is still \
						 banned but not removing any more users and not banning any more rooms): {}\nIf you would \
						 like to ignore errors, use --force",
						&local_user, &room_id, e
					)));
				}
			}
		}

		// remove any local aliases, ignore errors
		for ref local_alias in self
			.services
			.rooms
			.alias
			.local_aliases_for_room(&room_id)
			.filter_map(Result::ok)
		{
			_ = self
				.services
				.rooms
				.alias
				.remove_alias(local_alias, &self.services.globals.server_user)
				.await;
		}

		// unpublish from room directory, ignore errors
		_ = self.services.rooms.directory.set_not_public(&room_id);

		if disable_federation {
			self.services.rooms.metadata.disable_room(&room_id, true)?;
		}
	}

	if disable_federation {
		Ok(RoomMessageEventContent::text_plain(format!(
			"Finished bulk room ban, banned {room_ban_count} total rooms, evicted all users, and disabled incoming \
			 federation with the room."
		)))
	} else {
		Ok(RoomMessageEventContent::text_plain(format!(
			"Finished bulk room ban, banned {room_ban_count} total rooms and evicted all users."
		)))
	}
}

#[admin_command]
async fn unban_room(&self, enable_federation: bool, room: Box<RoomOrAliasId>) -> Result<RoomMessageEventContent> {
	let room_id = if room.is_room_id() {
		let room_id = match RoomId::parse(&room) {
			Ok(room_id) => room_id,
			Err(e) => {
				return Ok(RoomMessageEventContent::text_plain(format!(
					"Failed to parse room ID {room}. Please note that this requires a full room ID \
					 (`!awIh6gGInaS5wLQJwa:example.com`) or a room alias (`#roomalias:example.com`): {e}"
				)))
			},
		};

		debug!("Room specified is a room ID, unbanning room ID");

		self.services.rooms.metadata.ban_room(&room_id, false)?;

		room_id
	} else if room.is_room_alias_id() {
		let room_alias = match RoomAliasId::parse(&room) {
			Ok(room_alias) => room_alias,
			Err(e) => {
				return Ok(RoomMessageEventContent::text_plain(format!(
					"Failed to parse room ID {room}. Please note that this requires a full room ID \
					 (`!awIh6gGInaS5wLQJwa:example.com`) or a room alias (`#roomalias:example.com`): {e}"
				)))
			},
		};

		debug!(
			"Room specified is not a room ID, attempting to resolve room alias to a room ID locally, if not using \
			 get_alias_helper to fetch room ID remotely"
		);

		let room_id = if let Some(room_id) = self.services.rooms.alias.resolve_local_alias(&room_alias)? {
			room_id
		} else {
			debug!("We don't have this room alias to a room ID locally, attempting to fetch room ID over federation");

			match self
				.services
				.rooms
				.alias
				.resolve_alias(&room_alias, None)
				.await
			{
				Ok((room_id, servers)) => {
					debug!(?room_id, ?servers, "Got federation response fetching room ID for room {room}");
					room_id
				},
				Err(e) => {
					return Ok(RoomMessageEventContent::text_plain(format!(
						"Failed to resolve room alias {room} to a room ID: {e}"
					)));
				},
			}
		};

		self.services.rooms.metadata.ban_room(&room_id, false)?;

		room_id
	} else {
		return Ok(RoomMessageEventContent::text_plain(
			"Room specified is not a room ID or room alias. Please note that this requires a full room ID \
			 (`!awIh6gGInaS5wLQJwa:example.com`) or a room alias (`#roomalias:example.com`)",
		));
	};

	if enable_federation {
		self.services.rooms.metadata.disable_room(&room_id, false)?;
		return Ok(RoomMessageEventContent::text_plain("Room unbanned."));
	}

	Ok(RoomMessageEventContent::text_plain(
		"Room unbanned, you may need to re-enable federation with the room using enable-room if this is a remote room \
		 to make it fully functional.",
	))
}

#[admin_command]
async fn list_banned_rooms(&self) -> Result<RoomMessageEventContent> {
	let rooms = self
		.services
		.rooms
		.metadata
		.list_banned_rooms()
		.collect::<Result<Vec<_>, _>>();

	match rooms {
		Ok(room_ids) => {
			if room_ids.is_empty() {
				return Ok(RoomMessageEventContent::text_plain("No rooms are banned."));
			}

			let mut rooms = room_ids
				.into_iter()
				.map(|room_id| get_room_info(self.services, &room_id))
				.collect::<Vec<_>>();
			rooms.sort_by_key(|r| r.1);
			rooms.reverse();

			let output_plain = format!(
				"Rooms Banned ({}):\n```\n{}\n```",
				rooms.len(),
				rooms
					.iter()
					.map(|(id, members, name)| format!("{id}\tMembers: {members}\tName: {name}"))
					.collect::<Vec<_>>()
					.join("\n")
			);

			Ok(RoomMessageEventContent::notice_markdown(output_plain))
		},
		Err(e) => {
			error!("Failed to list banned rooms: {}", e);
			Ok(RoomMessageEventContent::text_plain(format!("Unable to list banned rooms: {e}")))
		},
	}
}
