use std::{
	net::SocketAddr,
	sync::{atomic::Ordering, Arc},
};

use axum::Router;
use axum_server::{bind, Handle as ServerHandle};
use conduit::{debug_info, Result, Server};
use tokio::task::JoinSet;
use tracing::info;

pub(super) async fn serve(
	server: &Arc<Server>, app: Router, handle: ServerHandle, addrs: Vec<SocketAddr>,
) -> Result<()> {
	let app = app.into_make_service_with_connect_info::<SocketAddr>();
	let mut join_set = JoinSet::new();
	for addr in &addrs {
		join_set.spawn_on(bind(*addr).handle(handle.clone()).serve(app.clone()), server.runtime());
	}

	info!("Listening on {addrs:?}");
	while join_set.join_next().await.is_some() {}

	let spawn_active = server.metrics.requests_spawn_active.load(Ordering::Relaxed);
	let handle_active = server
		.metrics
		.requests_handle_active
		.load(Ordering::Relaxed);
	debug_info!(
		spawn_finished = server
			.metrics
			.requests_spawn_finished
			.load(Ordering::Relaxed),
		handle_finished = server
			.metrics
			.requests_handle_finished
			.load(Ordering::Relaxed),
		panics = server.metrics.requests_panic.load(Ordering::Relaxed),
		spawn_active,
		handle_active,
		"Stopped listening on {addrs:?}",
	);

	debug_assert!(spawn_active == 0, "active request tasks are not joined");
	debug_assert!(handle_active == 0, "active request handles still pending");

	Ok(())
}
