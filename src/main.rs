#[cfg(unix)]
use std::fs::Permissions; // not unix specific, just only for UNIX sockets stuff and *nix container checks
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt as _;
// not unix specific, just only for UNIX sockets stuff and *nix container checks
use std::{io, net::SocketAddr, sync::atomic, time::Duration};

use axum::{
	extract::{DefaultBodyLimit, MatchedPath},
	response::IntoResponse,
	Router,
};
use axum_server::{bind, bind_rustls, tls_rustls::RustlsConfig, Handle as ServerHandle};
#[cfg(feature = "axum_dual_protocol")]
use axum_server_dual_protocol::ServerExt;
pub use conduit::*; // Re-export everything from the library crate
use http::{
	header::{self, HeaderName},
	Method, StatusCode,
};
#[cfg(unix)]
use hyperlocal::SocketIncoming;
use ruma::api::client::{
	error::{Error as RumaError, ErrorBody, ErrorKind},
	uiaa::UiaaResponse,
};
#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
use tikv_jemallocator::Jemalloc;
use tokio::{
	signal,
	sync::oneshot::{self, Sender},
	task::JoinSet,
};
use tower::ServiceBuilder;
use tower_http::{
	cors::{self, CorsLayer},
	trace::{DefaultOnFailure, TraceLayer},
	ServiceBuilderExt as _,
};
use tracing::{debug, error, info, warn, Level};
use tracing_subscriber::{prelude::*, EnvFilter};

mod routes;

#[cfg(all(not(target_env = "msvc"), feature = "jemalloc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

struct Server {
	config: Config,

	runtime: tokio::runtime::Runtime,

	#[cfg(feature = "sentry_telemetry")]
	_sentry_guard: Option<sentry::ClientInitGuard>,
}

fn main() -> Result<(), Error> {
	let args = clap::parse();
	let conduwuit: Server = init(args)?;

	conduwuit
		.runtime
		.block_on(async { async_main(&conduwuit).await })
}

async fn async_main(server: &Server) -> Result<(), Error> {
	if let Err(error) = start(server).await {
		error!("Critical error starting server: {error}");
		return Err(Error::Error(format!("{error}")));
	}

	if let Err(error) = run(server).await {
		error!("Critical error running server: {error}");
		return Err(Error::Error(format!("{error}")));
	};

	if let Err(error) = stop(server).await {
		error!("Critical error stopping server: {error}");
		return Err(Error::Error(format!("{error}")));
	}

	Ok(())
}

async fn run(server: &Server) -> io::Result<()> {
	let app = build(server).await?;
	let (tx, rx) = oneshot::channel::<()>();
	let handle = ServerHandle::new();
	tokio::spawn(shutdown(handle.clone(), tx));

	#[cfg(unix)]
	if server.config.unix_socket_path.is_some() {
		return run_unix_socket_server(server, app, rx).await;
	}

	let addrs = server.config.get_bind_addrs();
	if server.config.tls.is_some() {
		return run_tls_server(server, app, handle, addrs).await;
	}

	let mut join_set = JoinSet::new();
	for addr in &addrs {
		join_set.spawn(bind(*addr).handle(handle.clone()).serve(app.clone()));
	}

	#[allow(clippy::let_underscore_untyped)] // error[E0658]: attributes on expressions are experimental
	#[cfg(feature = "systemd")]
	let _ = sd_notify::notify(true, &[sd_notify::NotifyState::Ready]);

	info!("Listening on {:?}", addrs);
	join_set.join_next().await;

	Ok(())
}

async fn run_tls_server(
	server: &Server, app: axum::routing::IntoMakeService<Router>, handle: ServerHandle, addrs: Vec<SocketAddr>,
) -> io::Result<()> {
	let tls = server.config.tls.as_ref().unwrap();

	debug!(
		"Using direct TLS. Certificate path {} and certificate private key path {}",
		&tls.certs, &tls.key
	);
	info!(
		"Note: It is strongly recommended that you use a reverse proxy instead of running conduwuit directly with TLS."
	);
	let conf = RustlsConfig::from_pem_file(&tls.certs, &tls.key).await?;

	if cfg!(feature = "axum_dual_protocol") {
		info!(
			"conduwuit was built with axum_dual_protocol feature to listen on both HTTP and HTTPS. This will only \
			 take affect if `dual_protocol` is enabled in `[global.tls]`"
		);
	}

	let mut join_set = JoinSet::new();

	if cfg!(feature = "axum_dual_protocol") && tls.dual_protocol {
		#[cfg(feature = "axum_dual_protocol")]
		for addr in &addrs {
			join_set.spawn(
				axum_server_dual_protocol::bind_dual_protocol(*addr, conf.clone())
					.set_upgrade(false)
					.handle(handle.clone())
					.serve(app.clone()),
			);
		}
	} else {
		for addr in &addrs {
			join_set.spawn(
				bind_rustls(*addr, conf.clone())
					.handle(handle.clone())
					.serve(app.clone()),
			);
		}
	}

	#[allow(clippy::let_underscore_untyped)] // error[E0658]: attributes on expressions are experimental
	#[cfg(feature = "systemd")]
	let _ = sd_notify::notify(true, &[sd_notify::NotifyState::Ready]);

	if cfg!(feature = "axum_dual_protocol") && tls.dual_protocol {
		warn!(
			"Listening on {:?} with TLS certificate {} and supporting plain text (HTTP) connections too (insecure!)",
			addrs, &tls.certs
		);
	} else {
		info!("Listening on {:?} with TLS certificate {}", addrs, &tls.certs);
	}

	join_set.join_next().await;

	Ok(())
}

#[cfg(unix)]
async fn run_unix_socket_server(
	server: &Server, app: axum::routing::IntoMakeService<Router>, rx: oneshot::Receiver<()>,
) -> io::Result<()> {
	let path = server.config.unix_socket_path.as_ref().unwrap();

	if path.exists() {
		warn!(
			"UNIX socket path {:#?} already exists (unclean shutdown?), attempting to remove it.",
			path.display()
		);
		tokio::fs::remove_file(&path).await?;
	}

	tokio::fs::create_dir_all(path.parent().unwrap()).await?;

	let socket_perms = server.config.unix_socket_perms.to_string();
	let octal_perms = u32::from_str_radix(&socket_perms, 8).unwrap();

	let listener = tokio::net::UnixListener::bind(path.clone())?;
	tokio::fs::set_permissions(path, Permissions::from_mode(octal_perms))
		.await
		.unwrap();
	let socket = SocketIncoming::from_listener(listener);

	#[allow(clippy::let_underscore_untyped)] // error[E0658]: attributes on expressions are experimental
	#[cfg(feature = "systemd")]
	let _ = sd_notify::notify(true, &[sd_notify::NotifyState::Ready]);
	info!("Listening at {:?}", path);
	let server = hyper::Server::builder(socket).serve(app);
	let graceful = server.with_graceful_shutdown(async {
		rx.await.ok();
	});

	if let Err(e) = graceful.await {
		error!("Server error: {:?}", e);
	}

	Ok(())
}

async fn shutdown(handle: ServerHandle, tx: Sender<()>) -> Result<()> {
	let ctrl_c = async {
		signal::ctrl_c()
			.await
			.expect("failed to install Ctrl+C handler");
	};

	#[cfg(unix)]
	let terminate = async {
		signal::unix::signal(signal::unix::SignalKind::terminate())
			.expect("failed to install SIGTERM handler")
			.recv()
			.await;
	};

	let sig: &str;
	#[cfg(unix)]
	tokio::select! {
		() = ctrl_c => { sig = "Ctrl+C"; },
		() = terminate => { sig = "SIGTERM"; },
	}
	#[cfg(not(unix))]
	tokio::select! {
		_ = ctrl_c => { sig = "Ctrl+C"; },
	}

	warn!("Received {}, shutting down...", sig);
	handle.graceful_shutdown(Some(Duration::from_secs(180)));
	services().globals.shutdown();

	#[allow(clippy::let_underscore_untyped)] // error[E0658]: attributes on expressions are experimental
	#[cfg(feature = "systemd")]
	let _ = sd_notify::notify(true, &[sd_notify::NotifyState::Stopping]);

	tx.send(()).expect(
		"failed sending shutdown transaction to oneshot channel (this is unlikely a conduwuit bug and more so your \
		 system may not be in an okay/ideal state.)",
	);

	Ok(())
}

async fn stop(_server: &Server) -> io::Result<()> {
	info!("Shutdown complete.");

	Ok(())
}

/// Async initializations
async fn start(server: &Server) -> Result<(), Error> {
	let db_load_time = std::time::Instant::now();
	KeyValueDatabase::load_or_create(server.config.clone()).await?;
	info!("Database took {:?} to load", db_load_time.elapsed());

	Ok(())
}

async fn build(server: &Server) -> io::Result<axum::routing::IntoMakeService<Router>> {
	let base_middlewares = ServiceBuilder::new();
	#[cfg(feature = "sentry_telemetry")]
	let base_middlewares = base_middlewares.layer(sentry_tower::NewSentryLayer::<http::Request<_>>::new_from_top());

	let x_forwarded_for = HeaderName::from_static("x-forwarded-for");
	let middlewares = base_middlewares
		.sensitive_headers([header::AUTHORIZATION])
		.sensitive_request_headers([x_forwarded_for].into())
		.layer(axum::middleware::from_fn(request_spawn))
		.layer(
			TraceLayer::new_for_http()
				.make_span_with(tracing_span::<_>)
				.on_failure(DefaultOnFailure::new().level(Level::INFO)),
		)
		.layer(axum::middleware::from_fn(request_handler))
		.layer(cors_layer(server))
		.layer(DefaultBodyLimit::max(
			server
				.config
				.max_request_size
				.try_into()
				.expect("failed to convert max request size"),
		));

	#[cfg(any(feature = "zstd_compression", feature = "gzip_compression", feature = "brotli_compression"))]
	{
		Ok(routes::routes()
			.layer(compression_layer(server))
			.layer(middlewares)
			.into_make_service())
	}
	#[cfg(not(any(feature = "zstd_compression", feature = "gzip_compression", feature = "brotli_compression")))]
	{
		Ok(routes::routes().layer(middlewares).into_make_service())
	}
}

async fn request_spawn<B: Send + 'static>(
	req: http::Request<B>, next: axum::middleware::Next<B>,
) -> std::result::Result<axum::response::Response, StatusCode> {
	if services().globals.shutdown.load(atomic::Ordering::Relaxed) {
		return Err(StatusCode::SERVICE_UNAVAILABLE);
	}
	tokio::spawn(next.run(req))
		.await
		.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn request_handler<B: Send + 'static>(
	req: http::Request<B>, next: axum::middleware::Next<B>,
) -> std::result::Result<axum::response::Response, StatusCode> {
	let method = req.method().clone();
	let uri = req.uri().clone();
	let inner = next.run(req).await;
	if inner.status() == StatusCode::METHOD_NOT_ALLOWED {
		if uri.path().contains("_matrix/") {
			warn!("Method not allowed: {method} {uri}");
		} else {
			info!("Method not allowed: {method} {uri}");
		}
		return Ok(RumaResponse(UiaaResponse::MatrixError(RumaError {
			body: ErrorBody::Standard {
				kind: ErrorKind::Unrecognized,
				message: "M_UNRECOGNIZED: Method not allowed for endpoint".to_owned(),
			},
			status_code: StatusCode::METHOD_NOT_ALLOWED,
		}))
		.into_response());
	}

	Ok(inner)
}

fn cors_layer(_server: &Server) -> CorsLayer {
	let methods = [
		Method::GET,
		Method::HEAD,
		Method::POST,
		Method::PUT,
		Method::DELETE,
		Method::OPTIONS,
	];

	let headers = [
		header::ORIGIN,
		HeaderName::from_static("x-requested-with"),
		header::CONTENT_TYPE,
		header::ACCEPT,
		header::AUTHORIZATION,
	];

	CorsLayer::new()
		.allow_origin(cors::Any)
		.allow_methods(methods)
		.allow_headers(headers)
		.max_age(Duration::from_secs(86400))
}

#[cfg(any(feature = "zstd_compression", feature = "gzip_compression", feature = "brotli_compression"))]
fn compression_layer(server: &Server) -> tower_http::compression::CompressionLayer {
	let mut compression_layer = tower_http::compression::CompressionLayer::new();

	#[cfg(feature = "zstd_compression")]
	{
		if server.config.zstd_compression {
			compression_layer = compression_layer.zstd(true);
		} else {
			compression_layer = compression_layer.no_zstd();
		};
	};

	#[cfg(feature = "gzip_compression")]
	{
		if server.config.gzip_compression {
			compression_layer = compression_layer.gzip(true);
		} else {
			compression_layer = compression_layer.no_gzip();
		};
	};

	#[cfg(feature = "brotli_compression")]
	{
		if server.config.brotli_compression {
			compression_layer = compression_layer.br(true);
		} else {
			compression_layer = compression_layer.no_br();
		};
	};

	compression_layer
}

fn tracing_span<T>(request: &http::Request<T>) -> tracing::Span {
	let path = if let Some(path) = request.extensions().get::<MatchedPath>() {
		path.as_str()
	} else {
		request.uri().path()
	};

	tracing::info_span!("handle", %path)
}

/// Non-async initializations
fn init(args: clap::Args) -> Result<Server, Error> {
	let config = Config::new(args.config)?;

	#[cfg(feature = "sentry_telemetry")]
	let sentry_guard = if config.sentry {
		Some(init_sentry(&config))
	} else {
		None
	};

	if config.allow_jaeger {
		#[cfg(feature = "perf_measurements")]
		init_tracing_jaeger(&config);
	} else if config.tracing_flame {
		#[cfg(feature = "perf_measurements")]
		init_tracing_flame(&config);
	} else {
		init_tracing_sub(&config);
	}

	info!(
		server_name = ?config.server_name,
		database_path = ?config.database_path,
		log_levels = ?config.log,
		"{}",
		env!("CARGO_PKG_VERSION"),
	);

	#[cfg(unix)]
	maximize_fd_limit().expect("Unable to increase maximum soft and hard file descriptor limit");

	Ok(Server {
		config,

		runtime: tokio::runtime::Builder::new_multi_thread()
			.enable_io()
			.enable_time()
			.thread_name("conduwuit:worker")
			.worker_threads(num_cpus::get_physical())
			.build()
			.unwrap(),

		#[cfg(feature = "sentry_telemetry")]
		_sentry_guard: sentry_guard,
	})
}

#[cfg(feature = "sentry_telemetry")]
fn init_sentry(config: &Config) -> sentry::ClientInitGuard {
	sentry::init((
		"https://fe2eb4536aa04949e28eff3128d64757@o4506996327251968.ingest.us.sentry.io/4506996334657536",
		sentry::ClientOptions {
			release: sentry::release_name!(),
			traces_sample_rate: config.sentry_traces_sample_rate,
			server_name: if config.sentry_send_server_name {
				Some(config.server_name.to_string().into())
			} else {
				None
			},
			..Default::default()
		},
	))
}

fn init_tracing_sub(config: &Config) {
	let registry = tracing_subscriber::Registry::default();
	let fmt_layer = tracing_subscriber::fmt::Layer::new();
	let filter_layer = match EnvFilter::try_new(&config.log) {
		Ok(s) => s,
		Err(e) => {
			eprintln!("It looks like your config is invalid. The following error occured while parsing it: {e}");
			EnvFilter::try_new("warn").unwrap()
		},
	};

	#[cfg(feature = "sentry_telemetry")]
	let sentry_layer = sentry_tracing::layer();

	let subscriber;

	#[allow(clippy::unnecessary_operation)] // error[E0658]: attributes on expressions are experimental
	#[cfg(feature = "sentry_telemetry")]
	{
		subscriber = registry
			.with(filter_layer)
			.with(fmt_layer)
			.with(sentry_layer);
	};

	#[allow(clippy::unnecessary_operation)] // error[E0658]: attributes on expressions are experimental
	#[cfg(not(feature = "sentry_telemetry"))]
	{
		subscriber = registry.with(filter_layer).with(fmt_layer);
	};

	tracing::subscriber::set_global_default(subscriber).unwrap();
}

#[cfg(feature = "perf_measurements")]
fn init_tracing_jaeger(config: &Config) {
	opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
	let tracer = opentelemetry_jaeger::new_agent_pipeline()
		.with_auto_split_batch(true)
		.with_service_name("conduwuit")
		.install_batch(opentelemetry_sdk::runtime::Tokio)
		.unwrap();
	let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

	let filter_layer = match EnvFilter::try_new(&config.log) {
		Ok(s) => s,
		Err(e) => {
			eprintln!("It looks like your log config is invalid. The following error occurred: {e}");
			EnvFilter::try_new("warn").unwrap()
		},
	};

	let subscriber = tracing_subscriber::Registry::default()
		.with(filter_layer)
		.with(telemetry);
	tracing::subscriber::set_global_default(subscriber).unwrap();
}

#[cfg(feature = "perf_measurements")]
fn init_tracing_flame(_config: &Config) {
	let registry = tracing_subscriber::Registry::default();
	let (flame_layer, _guard) = tracing_flame::FlameLayer::with_file("./tracing.folded").unwrap();
	let flame_layer = flame_layer.with_empty_samples(false);

	let filter_layer = EnvFilter::new("trace,h2=off");

	let subscriber = registry.with(filter_layer).with(flame_layer);
	tracing::subscriber::set_global_default(subscriber).unwrap();
}

// This is needed for opening lots of file descriptors, which tends to
// happen more often when using RocksDB and making lots of federation
// connections at startup. The soft limit is usually 1024, and the hard
// limit is usually 512000; I've personally seen it hit >2000.
//
// * https://www.freedesktop.org/software/systemd/man/systemd.exec.html#id-1.12.2.1.17.6
// * https://github.com/systemd/systemd/commit/0abf94923b4a95a7d89bc526efc84e7ca2b71741
#[cfg(unix)]
fn maximize_fd_limit() -> Result<(), nix::errno::Errno> {
	use nix::sys::resource::{getrlimit, setrlimit, Resource::RLIMIT_NOFILE as NOFILE};

	let (soft_limit, hard_limit) = getrlimit(NOFILE)?;
	if soft_limit < hard_limit {
		setrlimit(NOFILE, hard_limit, hard_limit)?;
		assert_eq!((hard_limit, hard_limit), getrlimit(NOFILE)?, "getrlimit != setrlimit");
		debug!(to = hard_limit, from = soft_limit, "Raised RLIMIT_NOFILE",);
	}

	Ok(())
}
