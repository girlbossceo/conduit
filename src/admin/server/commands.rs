use std::{fmt::Write, sync::Arc};

use conduit::{info, utils::time, warn, Err, Result};
use ruma::events::room::message::RoomMessageEventContent;

use crate::admin_command;

#[admin_command]
pub(super) async fn uptime(&self) -> Result<RoomMessageEventContent> {
	let elapsed = self
		.services
		.server
		.started
		.elapsed()
		.expect("standard duration");

	let result = time::pretty(elapsed);
	Ok(RoomMessageEventContent::notice_plain(format!("{result}.")))
}

#[admin_command]
pub(super) async fn show_config(&self) -> Result<RoomMessageEventContent> {
	// Construct and send the response
	Ok(RoomMessageEventContent::text_plain(format!("{}", self.services.globals.config)))
}

#[admin_command]
pub(super) async fn list_features(
	&self, available: bool, enabled: bool, comma: bool,
) -> Result<RoomMessageEventContent> {
	let delim = if comma {
		","
	} else {
		" "
	};
	if enabled && !available {
		let features = info::rustc::features().join(delim);
		let out = format!("`\n{features}\n`");
		return Ok(RoomMessageEventContent::text_markdown(out));
	}

	if available && !enabled {
		let features = info::cargo::features().join(delim);
		let out = format!("`\n{features}\n`");
		return Ok(RoomMessageEventContent::text_markdown(out));
	}

	let mut features = String::new();
	let enabled = info::rustc::features();
	let available = info::cargo::features();
	for feature in available {
		let active = enabled.contains(&feature.as_str());
		let emoji = if active {
			"✅"
		} else {
			"❌"
		};
		let remark = if active {
			"[enabled]"
		} else {
			""
		};
		writeln!(features, "{emoji} {feature} {remark}")?;
	}

	Ok(RoomMessageEventContent::text_markdown(features))
}

#[admin_command]
pub(super) async fn memory_usage(&self) -> Result<RoomMessageEventContent> {
	let services_usage = self.services.memory_usage().await?;
	let database_usage = self.services.db.db.memory_usage()?;
	let allocator_usage = conduit::alloc::memory_usage().map_or(String::new(), |s| format!("\nAllocator:\n{s}"));

	Ok(RoomMessageEventContent::text_plain(format!(
		"Services:\n{services_usage}\nDatabase:\n{database_usage}{allocator_usage}",
	)))
}

#[admin_command]
pub(super) async fn clear_caches(&self) -> Result<RoomMessageEventContent> {
	self.services.clear_cache().await;

	Ok(RoomMessageEventContent::text_plain("Done."))
}

#[admin_command]
pub(super) async fn list_backups(&self) -> Result<RoomMessageEventContent> {
	let result = self.services.globals.db.backup_list()?;

	if result.is_empty() {
		Ok(RoomMessageEventContent::text_plain("No backups found."))
	} else {
		Ok(RoomMessageEventContent::text_plain(result))
	}
}

#[admin_command]
pub(super) async fn backup_database(&self) -> Result<RoomMessageEventContent> {
	let globals = Arc::clone(&self.services.globals);
	let mut result = self
		.services
		.server
		.runtime()
		.spawn_blocking(move || match globals.db.backup() {
			Ok(()) => String::new(),
			Err(e) => (*e).to_string(),
		})
		.await?;

	if result.is_empty() {
		result = self.services.globals.db.backup_list()?;
	}

	Ok(RoomMessageEventContent::notice_markdown(result))
}

#[admin_command]
pub(super) async fn list_database_files(&self) -> Result<RoomMessageEventContent> {
	let result = self.services.globals.db.file_list()?;

	Ok(RoomMessageEventContent::notice_markdown(result))
}

#[admin_command]
pub(super) async fn admin_notice(&self, message: Vec<String>) -> Result<RoomMessageEventContent> {
	let message = message.join(" ");
	self.services.admin.send_text(&message).await;

	Ok(RoomMessageEventContent::notice_plain("Notice was sent to #admins"))
}

#[admin_command]
pub(super) async fn reload_mods(&self) -> Result<RoomMessageEventContent> {
	self.services.server.reload()?;

	Ok(RoomMessageEventContent::notice_plain("Reloading server..."))
}

#[admin_command]
#[cfg(unix)]
pub(super) async fn restart(&self, force: bool) -> Result<RoomMessageEventContent> {
	use conduit::utils::sys::current_exe_deleted;

	if !force && current_exe_deleted() {
		return Err!(
			"The server cannot be restarted because the executable changed. If this is expected use --force to \
			 override."
		);
	}

	self.services.server.restart()?;

	Ok(RoomMessageEventContent::notice_plain("Restarting server..."))
}

#[admin_command]
pub(super) async fn shutdown(&self) -> Result<RoomMessageEventContent> {
	warn!("shutdown command");
	self.services.server.shutdown()?;

	Ok(RoomMessageEventContent::notice_plain("Shutting down server..."))
}
