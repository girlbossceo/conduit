use axum::extract::State;
use conduit::{pdu::PduBuilder, warn, Error, Result};
use ruma::{
	api::{
		client::{
			error::ErrorKind,
			profile::{get_avatar_url, get_display_name, get_profile, set_avatar_url, set_display_name},
		},
		federation,
	},
	events::{room::member::RoomMemberEventContent, StateEventType, TimelineEventType},
	presence::PresenceState,
	OwnedMxcUri, OwnedRoomId, OwnedUserId,
};
use serde_json::value::to_raw_value;
use service::Services;

use crate::Ruma;

/// # `PUT /_matrix/client/r0/profile/{userId}/displayname`
///
/// Updates the displayname.
///
/// - Also makes sure other users receive the update using presence EDUs
pub(crate) async fn set_displayname_route(
	State(services): State<crate::State>, body: Ruma<set_display_name::v3::Request>,
) -> Result<set_display_name::v3::Response> {
	let sender_user = body.sender_user.as_ref().expect("user is authenticated");
	let all_joined_rooms: Vec<OwnedRoomId> = services
		.rooms
		.state_cache
		.rooms_joined(sender_user)
		.filter_map(Result::ok)
		.collect();

	update_displayname(&services, sender_user.clone(), body.displayname.clone(), all_joined_rooms).await?;

	if services.globals.allow_local_presence() {
		// Presence update
		services
			.presence
			.ping_presence(sender_user, &PresenceState::Online)?;
	}

	Ok(set_display_name::v3::Response {})
}

/// # `GET /_matrix/client/v3/profile/{userId}/displayname`
///
/// Returns the displayname of the user.
///
/// - If user is on another server and we do not have a local copy already fetch
///   displayname over federation
pub(crate) async fn get_displayname_route(
	State(services): State<crate::State>, body: Ruma<get_display_name::v3::Request>,
) -> Result<get_display_name::v3::Response> {
	if !services.globals.user_is_local(&body.user_id) {
		// Create and update our local copy of the user
		if let Ok(response) = services
			.sending
			.send_federation_request(
				body.user_id.server_name(),
				federation::query::get_profile_information::v1::Request {
					user_id: body.user_id.clone(),
					field: None, // we want the full user's profile to update locally too
				},
			)
			.await
		{
			if !services.users.exists(&body.user_id)? {
				services.users.create(&body.user_id, None)?;
			}

			services
				.users
				.set_displayname(&body.user_id, response.displayname.clone())
				.await?;
			services
				.users
				.set_avatar_url(&body.user_id, response.avatar_url.clone())
				.await?;
			services
				.users
				.set_blurhash(&body.user_id, response.blurhash.clone())
				.await?;

			return Ok(get_display_name::v3::Response {
				displayname: response.displayname,
			});
		}
	}

	if !services.users.exists(&body.user_id)? {
		// Return 404 if this user doesn't exist and we couldn't fetch it over
		// federation
		return Err(Error::BadRequest(ErrorKind::NotFound, "Profile was not found."));
	}

	Ok(get_display_name::v3::Response {
		displayname: services.users.displayname(&body.user_id)?,
	})
}

/// # `PUT /_matrix/client/v3/profile/{userId}/avatar_url`
///
/// Updates the `avatar_url` and `blurhash`.
///
/// - Also makes sure other users receive the update using presence EDUs
pub(crate) async fn set_avatar_url_route(
	State(services): State<crate::State>, body: Ruma<set_avatar_url::v3::Request>,
) -> Result<set_avatar_url::v3::Response> {
	let sender_user = body.sender_user.as_ref().expect("user is authenticated");
	let all_joined_rooms: Vec<OwnedRoomId> = services
		.rooms
		.state_cache
		.rooms_joined(sender_user)
		.filter_map(Result::ok)
		.collect();

	update_avatar_url(
		&services,
		sender_user.clone(),
		body.avatar_url.clone(),
		body.blurhash.clone(),
		all_joined_rooms,
	)
	.await?;

	if services.globals.allow_local_presence() {
		// Presence update
		services
			.presence
			.ping_presence(sender_user, &PresenceState::Online)?;
	}

	Ok(set_avatar_url::v3::Response {})
}

/// # `GET /_matrix/client/v3/profile/{userId}/avatar_url`
///
/// Returns the `avatar_url` and `blurhash` of the user.
///
/// - If user is on another server and we do not have a local copy already fetch
///   `avatar_url` and blurhash over federation
pub(crate) async fn get_avatar_url_route(
	State(services): State<crate::State>, body: Ruma<get_avatar_url::v3::Request>,
) -> Result<get_avatar_url::v3::Response> {
	if !services.globals.user_is_local(&body.user_id) {
		// Create and update our local copy of the user
		if let Ok(response) = services
			.sending
			.send_federation_request(
				body.user_id.server_name(),
				federation::query::get_profile_information::v1::Request {
					user_id: body.user_id.clone(),
					field: None, // we want the full user's profile to update locally as well
				},
			)
			.await
		{
			if !services.users.exists(&body.user_id)? {
				services.users.create(&body.user_id, None)?;
			}

			services
				.users
				.set_displayname(&body.user_id, response.displayname.clone())
				.await?;
			services
				.users
				.set_avatar_url(&body.user_id, response.avatar_url.clone())
				.await?;
			services
				.users
				.set_blurhash(&body.user_id, response.blurhash.clone())
				.await?;

			return Ok(get_avatar_url::v3::Response {
				avatar_url: response.avatar_url,
				blurhash: response.blurhash,
			});
		}
	}

	if !services.users.exists(&body.user_id)? {
		// Return 404 if this user doesn't exist and we couldn't fetch it over
		// federation
		return Err(Error::BadRequest(ErrorKind::NotFound, "Profile was not found."));
	}

	Ok(get_avatar_url::v3::Response {
		avatar_url: services.users.avatar_url(&body.user_id)?,
		blurhash: services.users.blurhash(&body.user_id)?,
	})
}

/// # `GET /_matrix/client/v3/profile/{userId}`
///
/// Returns the displayname, avatar_url and blurhash of the user.
///
/// - If user is on another server and we do not have a local copy already,
///   fetch profile over federation.
pub(crate) async fn get_profile_route(
	State(services): State<crate::State>, body: Ruma<get_profile::v3::Request>,
) -> Result<get_profile::v3::Response> {
	if !services.globals.user_is_local(&body.user_id) {
		// Create and update our local copy of the user
		if let Ok(response) = services
			.sending
			.send_federation_request(
				body.user_id.server_name(),
				federation::query::get_profile_information::v1::Request {
					user_id: body.user_id.clone(),
					field: None,
				},
			)
			.await
		{
			if !services.users.exists(&body.user_id)? {
				services.users.create(&body.user_id, None)?;
			}

			services
				.users
				.set_displayname(&body.user_id, response.displayname.clone())
				.await?;
			services
				.users
				.set_avatar_url(&body.user_id, response.avatar_url.clone())
				.await?;
			services
				.users
				.set_blurhash(&body.user_id, response.blurhash.clone())
				.await?;

			return Ok(get_profile::v3::Response {
				displayname: response.displayname,
				avatar_url: response.avatar_url,
				blurhash: response.blurhash,
			});
		}
	}

	if !services.users.exists(&body.user_id)? {
		// Return 404 if this user doesn't exist and we couldn't fetch it over
		// federation
		return Err(Error::BadRequest(ErrorKind::NotFound, "Profile was not found."));
	}

	Ok(get_profile::v3::Response {
		avatar_url: services.users.avatar_url(&body.user_id)?,
		blurhash: services.users.blurhash(&body.user_id)?,
		displayname: services.users.displayname(&body.user_id)?,
	})
}

pub async fn update_displayname(
	services: &Services, user_id: OwnedUserId, displayname: Option<String>, all_joined_rooms: Vec<OwnedRoomId>,
) -> Result<()> {
	let current_display_name = services.users.displayname(&user_id).unwrap_or_default();

	if displayname == current_display_name {
		return Ok(());
	}

	services
		.users
		.set_displayname(&user_id, displayname.clone())
		.await?;

	// Send a new join membership event into all joined rooms
	let all_joined_rooms: Vec<_> = all_joined_rooms
		.iter()
		.map(|room_id| {
			Ok::<_, Error>((
				PduBuilder {
					event_type: TimelineEventType::RoomMember,
					content: to_raw_value(&RoomMemberEventContent {
						displayname: displayname.clone(),
						join_authorized_via_users_server: None,
						..serde_json::from_str(
							services
								.rooms
								.state_accessor
								.room_state_get(room_id, &StateEventType::RoomMember, user_id.as_str())?
								.ok_or_else(|| {
									Error::bad_database("Tried to send display name update for user not in the room.")
								})?
								.content
								.get(),
						)
						.map_err(|_| Error::bad_database("Database contains invalid PDU."))?
					})
					.expect("event is valid, we just created it"),
					unsigned: None,
					state_key: Some(user_id.to_string()),
					redacts: None,
					timestamp: None,
				},
				room_id,
			))
		})
		.filter_map(Result::ok)
		.collect();

	update_all_rooms(services, all_joined_rooms, user_id).await;

	Ok(())
}

pub async fn update_avatar_url(
	services: &Services, user_id: OwnedUserId, avatar_url: Option<OwnedMxcUri>, blurhash: Option<String>,
	all_joined_rooms: Vec<OwnedRoomId>,
) -> Result<()> {
	let current_avatar_url = services.users.avatar_url(&user_id).unwrap_or_default();
	let current_blurhash = services.users.blurhash(&user_id).unwrap_or_default();

	if current_avatar_url == avatar_url && current_blurhash == blurhash {
		return Ok(());
	}

	services
		.users
		.set_avatar_url(&user_id, avatar_url.clone())
		.await?;
	services
		.users
		.set_blurhash(&user_id, blurhash.clone())
		.await?;

	// Send a new join membership event into all joined rooms
	let all_joined_rooms: Vec<_> = all_joined_rooms
		.iter()
		.map(|room_id| {
			Ok::<_, Error>((
				PduBuilder {
					event_type: TimelineEventType::RoomMember,
					content: to_raw_value(&RoomMemberEventContent {
						avatar_url: avatar_url.clone(),
						blurhash: blurhash.clone(),
						join_authorized_via_users_server: None,
						..serde_json::from_str(
							services
								.rooms
								.state_accessor
								.room_state_get(room_id, &StateEventType::RoomMember, user_id.as_str())?
								.ok_or_else(|| {
									Error::bad_database("Tried to send avatar URL update for user not in the room.")
								})?
								.content
								.get(),
						)
						.map_err(|_| Error::bad_database("Database contains invalid PDU."))?
					})
					.expect("event is valid, we just created it"),
					unsigned: None,
					state_key: Some(user_id.to_string()),
					redacts: None,
					timestamp: None,
				},
				room_id,
			))
		})
		.filter_map(Result::ok)
		.collect();

	update_all_rooms(services, all_joined_rooms, user_id).await;

	Ok(())
}

pub async fn update_all_rooms(
	services: &Services, all_joined_rooms: Vec<(PduBuilder, &OwnedRoomId)>, user_id: OwnedUserId,
) {
	for (pdu_builder, room_id) in all_joined_rooms {
		let state_lock = services.rooms.state.mutex.lock(room_id).await;
		if let Err(e) = services
			.rooms
			.timeline
			.build_and_append_pdu(pdu_builder, &user_id, room_id, &state_lock)
			.await
		{
			warn!(%user_id, %room_id, %e, "Failed to update/send new profile join membership update in room");
		}
	}
}
