mod room_alias_commands;
mod room_commands;
mod room_directory_commands;
mod room_info_commands;
mod room_moderation_commands;

use clap::Subcommand;
use conduit::Result;
use ruma::{events::room::message::RoomMessageEventContent, RoomId, RoomOrAliasId};

use self::room_commands::list;

#[cfg_attr(test, derive(Debug))]
#[derive(Subcommand)]
pub(super) enum RoomCommand {
	/// - List all rooms the server knows about
	List {
		page: Option<usize>,

		/// Excludes rooms that we have federation disabled with
		#[arg(long)]
		exclude_disabled: bool,

		/// Excludes rooms that we have banned
		#[arg(long)]
		exclude_banned: bool,
	},

	#[command(subcommand)]
	/// - View information about a room we know about
	Info(RoomInfoCommand),

	#[command(subcommand)]
	/// - Manage moderation of remote or local rooms
	Moderation(RoomModerationCommand),

	#[command(subcommand)]
	/// - Manage rooms' aliases
	Alias(RoomAliasCommand),

	#[command(subcommand)]
	/// - Manage the room directory
	Directory(RoomDirectoryCommand),
}

#[cfg_attr(test, derive(Debug))]
#[derive(Subcommand)]
pub(super) enum RoomInfoCommand {
	/// - List joined members in a room
	ListJoinedMembers {
		room_id: Box<RoomId>,
	},

	/// - Displays room topic
	///
	/// Room topics can be huge, so this is in its
	/// own separate command
	ViewRoomTopic {
		room_id: Box<RoomId>,
	},
}

#[cfg_attr(test, derive(Debug))]
#[derive(Subcommand)]
pub(super) enum RoomAliasCommand {
	/// - Make an alias point to a room.
	Set {
		#[arg(short, long)]
		/// Set the alias even if a room is already using it
		force: bool,

		/// The room id to set the alias on
		room_id: Box<RoomId>,

		/// The alias localpart to use (`alias`, not `#alias:servername.tld`)
		room_alias_localpart: String,
	},

	/// - Remove a local alias
	Remove {
		/// The alias localpart to remove (`alias`, not `#alias:servername.tld`)
		room_alias_localpart: String,
	},

	/// - Show which room is using an alias
	Which {
		/// The alias localpart to look up (`alias`, not
		/// `#alias:servername.tld`)
		room_alias_localpart: String,
	},

	/// - List aliases currently being used
	List {
		/// If set, only list the aliases for this room
		room_id: Option<Box<RoomId>>,
	},
}

#[cfg_attr(test, derive(Debug))]
#[derive(Subcommand)]
pub(super) enum RoomDirectoryCommand {
	/// - Publish a room to the room directory
	Publish {
		/// The room id of the room to publish
		room_id: Box<RoomId>,
	},

	/// - Unpublish a room to the room directory
	Unpublish {
		/// The room id of the room to unpublish
		room_id: Box<RoomId>,
	},

	/// - List rooms that are published
	List {
		page: Option<usize>,
	},
}

#[cfg_attr(test, derive(Debug))]
#[derive(Subcommand)]
pub(super) enum RoomModerationCommand {
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

pub(super) async fn process(command: RoomCommand, body: Vec<&str>) -> Result<RoomMessageEventContent> {
	Ok(match command {
		RoomCommand::Info(command) => room_info_commands::process(command, body).await?,

		RoomCommand::Alias(command) => room_alias_commands::process(command, body).await?,

		RoomCommand::Directory(command) => room_directory_commands::process(command, body).await?,

		RoomCommand::Moderation(command) => room_moderation_commands::process(command, body).await?,

		RoomCommand::List {
			page,
			exclude_disabled,
			exclude_banned,
		} => list(body, page, exclude_disabled, exclude_banned).await?,
	})
}
