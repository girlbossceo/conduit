use std::{collections::BTreeMap, net::IpAddr, time::Instant};

use axum::extract::State;
use axum_client_ip::InsecureClientIp;
use conduit::{debug, debug_warn, err, trace, warn, Err};
use ruma::{
	api::{
		client::error::ErrorKind,
		federation::transactions::{
			edu::{
				DeviceListUpdateContent, DirectDeviceContent, Edu, PresenceContent, ReceiptContent,
				SigningKeyUpdateContent, TypingContent,
			},
			send_transaction_message,
		},
	},
	events::receipt::{ReceiptEvent, ReceiptEventContent, ReceiptType},
	to_device::DeviceIdOrAllDevices,
	OwnedEventId, ServerName,
};
use tokio::sync::RwLock;

use crate::{
	service::rooms::event_handler::parse_incoming_pdu,
	services::Services,
	utils::{self},
	Error, Result, Ruma,
};

type ResolvedMap = BTreeMap<OwnedEventId, Result<(), Error>>;

/// # `PUT /_matrix/federation/v1/send/{txnId}`
///
/// Push EDUs and PDUs to this server.
#[tracing::instrument(skip_all, fields(%client), name = "send")]
pub(crate) async fn send_transaction_message_route(
	State(services): State<&Services>, InsecureClientIp(client): InsecureClientIp,
	body: Ruma<send_transaction_message::v1::Request>,
) -> Result<send_transaction_message::v1::Response> {
	let origin = body.origin.as_ref().expect("server is authenticated");

	if *origin != body.body.origin {
		return Err!(Request(Forbidden(
			"Not allowed to send transactions on behalf of other servers"
		)));
	}

	if body.pdus.len() > 50_usize {
		return Err!(Request(Forbidden("Not allowed to send more than 50 PDUs in one transaction")));
	}

	if body.edus.len() > 100_usize {
		return Err!(Request(Forbidden("Not allowed to send more than 100 EDUs in one transaction")));
	}

	let txn_start_time = Instant::now();
	trace!(
		pdus = ?body.pdus.len(),
		edus = ?body.edus.len(),
		elapsed = ?txn_start_time.elapsed(),
		id = ?body.transaction_id,
		origin =?body.origin,
		"Starting txn",
	);

	let resolved_map = handle_pdus(services, &client, &body, origin, &txn_start_time).await?;
	handle_edus(services, &client, &body, origin).await?;

	debug!(
		pdus = ?body.pdus.len(),
		edus = ?body.edus.len(),
		elapsed = ?txn_start_time.elapsed(),
		id = ?body.transaction_id,
		origin =?body.origin,
		"Finished txn",
	);

	Ok(send_transaction_message::v1::Response {
		pdus: resolved_map
			.into_iter()
			.map(|(e, r)| (e, r.map_err(|e| e.sanitized_string())))
			.collect(),
	})
}

async fn handle_pdus(
	services: &Services, _client: &IpAddr, body: &Ruma<send_transaction_message::v1::Request>, origin: &ServerName,
	txn_start_time: &Instant,
) -> Result<ResolvedMap> {
	let mut parsed_pdus = Vec::with_capacity(body.pdus.len());
	for pdu in &body.pdus {
		parsed_pdus.push(match parse_incoming_pdu(pdu) {
			Ok(t) => t,
			Err(e) => {
				debug_warn!("Could not parse PDU: {e}");
				continue;
			},
		});

		// We do not add the event_id field to the pdu here because of signature
		// and hashes checks
	}

	// We go through all the signatures we see on the PDUs and fetch the
	// corresponding signing keys
	let pub_key_map = RwLock::new(BTreeMap::new());
	if !parsed_pdus.is_empty() {
		services
			.rooms
			.event_handler
			.fetch_required_signing_keys(parsed_pdus.iter().map(|(_event_id, event, _room_id)| event), &pub_key_map)
			.await
			.unwrap_or_else(|e| warn!("Could not fetch all signatures for PDUs from {origin}: {e:?}"));

		debug!(
			elapsed = ?txn_start_time.elapsed(),
			"Fetched signing keys"
		);
	}

	let mut resolved_map = BTreeMap::new();
	for (event_id, value, room_id) in parsed_pdus {
		let pdu_start_time = Instant::now();
		let mutex_lock = services
			.rooms
			.event_handler
			.mutex_federation
			.lock(&room_id)
			.await;
		resolved_map.insert(
			event_id.clone(),
			services
				.rooms
				.event_handler
				.handle_incoming_pdu(origin, &room_id, &event_id, value, true, &pub_key_map)
				.await
				.map(|_| ()),
		);
		drop(mutex_lock);

		debug!(
			pdu_elapsed = ?pdu_start_time.elapsed(),
			txn_elapsed = ?txn_start_time.elapsed(),
			"Finished PDU {event_id}",
		);
	}

	for pdu in &resolved_map {
		if let Err(e) = pdu.1 {
			if matches!(e, Error::BadRequest(ErrorKind::NotFound, _)) {
				warn!("Incoming PDU failed {pdu:?}");
			}
		}
	}

	Ok(resolved_map)
}

async fn handle_edus(
	services: &Services, client: &IpAddr, body: &Ruma<send_transaction_message::v1::Request>, origin: &ServerName,
) -> Result<()> {
	for edu in body
		.edus
		.iter()
		.filter_map(|edu| serde_json::from_str::<Edu>(edu.json().get()).ok())
	{
		match edu {
			Edu::Presence(presence) => handle_edu_presence(services, client, origin, presence).await?,
			Edu::Receipt(receipt) => handle_edu_receipt(services, client, origin, receipt).await?,
			Edu::Typing(typing) => handle_edu_typing(services, client, origin, typing).await?,
			Edu::DeviceListUpdate(content) => handle_edu_device_list_update(services, client, origin, content).await?,
			Edu::DirectToDevice(content) => handle_edu_direct_to_device(services, client, origin, content).await?,
			Edu::SigningKeyUpdate(content) => handle_edu_signing_key_update(services, client, origin, content).await?,
			Edu::_Custom(ref _custom) => {
				debug_warn!(?body.edus, "received custom/unknown EDU");
			},
		}
	}

	Ok(())
}

async fn handle_edu_presence(
	services: &Services, _client: &IpAddr, origin: &ServerName, presence: PresenceContent,
) -> Result<()> {
	if !services.globals.allow_incoming_presence() {
		return Ok(());
	}

	for update in presence.push {
		if update.user_id.server_name() != origin {
			debug_warn!(
				%update.user_id, %origin,
				"received presence EDU for user not belonging to origin"
			);
			continue;
		}

		services.presence.set_presence(
			&update.user_id,
			&update.presence,
			Some(update.currently_active),
			Some(update.last_active_ago),
			update.status_msg.clone(),
		)?;
	}

	Ok(())
}

async fn handle_edu_receipt(
	services: &Services, _client: &IpAddr, origin: &ServerName, receipt: ReceiptContent,
) -> Result<()> {
	if !services.globals.allow_incoming_read_receipts() {
		return Ok(());
	}

	for (room_id, room_updates) in receipt.receipts {
		if services
			.rooms
			.event_handler
			.acl_check(origin, &room_id)
			.is_err()
		{
			debug_warn!(
				%origin, %room_id,
				"received read receipt EDU from ACL'd server"
			);
			continue;
		}

		for (user_id, user_updates) in room_updates.read {
			if user_id.server_name() != origin {
				debug_warn!(
					%user_id, %origin,
					"received read receipt EDU for user not belonging to origin"
				);
				continue;
			}

			if services
				.rooms
				.state_cache
				.room_members(&room_id)
				.filter_map(Result::ok)
				.any(|member| member.server_name() == user_id.server_name())
			{
				for event_id in &user_updates.event_ids {
					let user_receipts = BTreeMap::from([(user_id.clone(), user_updates.data.clone())]);
					let receipts = BTreeMap::from([(ReceiptType::Read, user_receipts)]);
					let receipt_content = BTreeMap::from([(event_id.to_owned(), receipts)]);
					let event = ReceiptEvent {
						content: ReceiptEventContent(receipt_content),
						room_id: room_id.clone(),
					};

					services
						.rooms
						.read_receipt
						.readreceipt_update(&user_id, &room_id, &event)?;
				}
			} else {
				debug_warn!(
					%user_id, %room_id, %origin,
					"received read receipt EDU from server who does not have a member in the room",
				);
				continue;
			}
		}
	}

	Ok(())
}

async fn handle_edu_typing(
	services: &Services, _client: &IpAddr, origin: &ServerName, typing: TypingContent,
) -> Result<()> {
	if !services.globals.config.allow_incoming_typing {
		return Ok(());
	}

	if typing.user_id.server_name() != origin {
		debug_warn!(
			%typing.user_id, %origin,
			"received typing EDU for user not belonging to origin"
		);
		return Ok(());
	}

	if services
		.rooms
		.event_handler
		.acl_check(typing.user_id.server_name(), &typing.room_id)
		.is_err()
	{
		debug_warn!(
			%typing.user_id, %typing.room_id, %origin,
			"received typing EDU for ACL'd user's server"
		);
		return Ok(());
	}

	if services
		.rooms
		.state_cache
		.is_joined(&typing.user_id, &typing.room_id)?
	{
		if typing.typing {
			let timeout = utils::millis_since_unix_epoch().saturating_add(
				services
					.globals
					.config
					.typing_federation_timeout_s
					.saturating_mul(1000),
			);
			services
				.rooms
				.typing
				.typing_add(&typing.user_id, &typing.room_id, timeout)
				.await?;
		} else {
			services
				.rooms
				.typing
				.typing_remove(&typing.user_id, &typing.room_id)
				.await?;
		}
	} else {
		debug_warn!(
			%typing.user_id, %typing.room_id, %origin,
			"received typing EDU for user not in room"
		);
		return Ok(());
	}

	Ok(())
}

async fn handle_edu_device_list_update(
	services: &Services, _client: &IpAddr, origin: &ServerName, content: DeviceListUpdateContent,
) -> Result<()> {
	let DeviceListUpdateContent {
		user_id,
		..
	} = content;

	if user_id.server_name() != origin {
		debug_warn!(
			%user_id, %origin,
			"received device list update EDU for user not belonging to origin"
		);
		return Ok(());
	}

	services.users.mark_device_key_update(&user_id)?;

	Ok(())
}

async fn handle_edu_direct_to_device(
	services: &Services, _client: &IpAddr, origin: &ServerName, content: DirectDeviceContent,
) -> Result<()> {
	let DirectDeviceContent {
		sender,
		ev_type,
		message_id,
		messages,
	} = content;

	if sender.server_name() != origin {
		debug_warn!(
			%sender, %origin,
			"received direct to device EDU for user not belonging to origin"
		);
		return Ok(());
	}

	// Check if this is a new transaction id
	if services
		.transaction_ids
		.existing_txnid(&sender, None, &message_id)?
		.is_some()
	{
		return Ok(());
	}

	for (target_user_id, map) in &messages {
		for (target_device_id_maybe, event) in map {
			match target_device_id_maybe {
				DeviceIdOrAllDevices::DeviceId(target_device_id) => {
					services.users.add_to_device_event(
						&sender,
						target_user_id,
						target_device_id,
						&ev_type.to_string(),
						event
							.deserialize_as()
							.map_err(|e| err!(Request(InvalidParam(error!("To-Device event is invalid: {e}")))))?,
					)?;
				},

				DeviceIdOrAllDevices::AllDevices => {
					for target_device_id in services.users.all_device_ids(target_user_id) {
						services.users.add_to_device_event(
							&sender,
							target_user_id,
							&target_device_id?,
							&ev_type.to_string(),
							event
								.deserialize_as()
								.map_err(|e| err!(Request(InvalidParam("Event is invalid: {e}"))))?,
						)?;
					}
				},
			}
		}
	}

	// Save transaction id with empty data
	services
		.transaction_ids
		.add_txnid(&sender, None, &message_id, &[])?;

	Ok(())
}

async fn handle_edu_signing_key_update(
	services: &Services, _client: &IpAddr, origin: &ServerName, content: SigningKeyUpdateContent,
) -> Result<()> {
	let SigningKeyUpdateContent {
		user_id,
		master_key,
		self_signing_key,
	} = content;

	if user_id.server_name() != origin {
		debug_warn!(
			%user_id, %origin,
			"received signing key update EDU from server that does not belong to user's server"
		);
		return Ok(());
	}

	if let Some(master_key) = master_key {
		services
			.users
			.add_cross_signing_keys(&user_id, &master_key, &self_signing_key, &None, true)?;
	}

	Ok(())
}
