use std::{
	fmt::Debug,
	mem,
	net::{IpAddr, SocketAddr},
};

use hickory_resolver::{error::ResolveError, lookup::SrvLookup};
use http::{header::AUTHORIZATION, HeaderValue};
use ipaddress::IPAddress;
use ruma::{
	api::{
		client::error::Error as RumaError, EndpointError, IncomingResponse, MatrixVersion, OutgoingRequest,
		SendAccessToken,
	},
	OwnedServerName, ServerName,
};
use tracing::{debug, error, trace};

use crate::{debug_error, debug_info, debug_warn, services, Error, Result};

/// Wraps either an literal IP address plus port, or a hostname plus complement
/// (colon-plus-port if it was specified).
///
/// Note: A `FedDest::Named` might contain an IP address in string form if there
/// was no port specified to construct a `SocketAddr` with.
///
/// # Examples:
/// ```rust
/// # use conduit::api::server_server::FedDest;
/// # fn main() -> Result<(), std::net::AddrParseError> {
/// FedDest::Literal("198.51.100.3:8448".parse()?);
/// FedDest::Literal("[2001:db8::4:5]:443".parse()?);
/// FedDest::Named("matrix.example.org".to_owned(), String::new());
/// FedDest::Named("matrix.example.org".to_owned(), ":8448".to_owned());
/// FedDest::Named("198.51.100.5".to_owned(), String::new());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FedDest {
	Literal(SocketAddr),
	Named(String, String),
}

struct ActualDestination {
	destination: FedDest,
	host: String,
	string: String,
	cached: bool,
}

#[tracing::instrument(skip_all, name = "send")]
pub(crate) async fn send_request<T>(
	client: &reqwest::Client, destination: &ServerName, req: T,
) -> Result<T::IncomingResponse>
where
	T: OutgoingRequest + Debug,
{
	if !services().globals.allow_federation() {
		return Err(Error::bad_config("Federation is disabled."));
	}

	trace!("Preparing to send request");
	validate_destination(destination)?;
	let actual = get_actual_destination(destination).await?;
	let mut http_request = req
		.try_into_http_request::<Vec<u8>>(&actual.string, SendAccessToken::IfRequired(""), &[MatrixVersion::V1_5])
		.map_err(|e| {
			debug_warn!("Failed to find destination {}: {}", actual.string, e);
			Error::BadServerResponse("Invalid destination")
		})?;

	sign_request::<T>(destination, &mut http_request);
	let request = reqwest::Request::try_from(http_request)?;
	let method = request.method().clone();
	let url = request.url().clone();
	validate_url(&url)?;

	debug!(
		method = ?method,
		url = ?url,
		"Sending request",
	);
	match client.execute(request).await {
		Ok(response) => handle_response::<T>(destination, actual, &method, &url, response).await,
		Err(e) => handle_error::<T>(destination, &actual, &method, &url, e),
	}
}

async fn handle_response<T>(
	destination: &ServerName, actual: ActualDestination, method: &reqwest::Method, url: &reqwest::Url,
	mut response: reqwest::Response,
) -> Result<T::IncomingResponse>
where
	T: OutgoingRequest + Debug,
{
	trace!("Received response from {} for {} with {}", actual.string, url, response.url());
	let status = response.status();
	let mut http_response_builder = http::Response::builder()
		.status(status)
		.version(response.version());
	mem::swap(
		response.headers_mut(),
		http_response_builder
			.headers_mut()
			.expect("http::response::Builder is usable"),
	);

	trace!("Waiting for response body");
	let body = response.bytes().await.unwrap_or_else(|e| {
		debug_error!("server error {}", e);
		Vec::new().into()
	}); // TODO: handle timeout

	let http_response = http_response_builder
		.body(body)
		.expect("reqwest body is valid http body");

	debug!("Got {status:?} for {method} {url}");
	if !status.is_success() {
		return Err(Error::FederationError(
			destination.to_owned(),
			RumaError::from_http_response(http_response),
		));
	}

	let response = T::IncomingResponse::try_from_http_response(http_response);
	if response.is_ok() && !actual.cached {
		services()
			.globals
			.actual_destinations()
			.write()
			.await
			.insert(OwnedServerName::from(destination), (actual.destination, actual.host));
	}

	match response {
		Err(_e) => Err(Error::BadServerResponse("Server returned bad 200 response.")),
		Ok(response) => Ok(response),
	}
}

fn handle_error<T>(
	_destination: &ServerName, actual: &ActualDestination, method: &reqwest::Method, url: &reqwest::Url,
	e: reqwest::Error,
) -> Result<T::IncomingResponse>
where
	T: OutgoingRequest + Debug,
{
	// we do not need to log that servers in a room are dead, this is normal in
	// public rooms and just spams the logs.
	if e.is_timeout() {
		debug_error!("timeout {}: {}", actual.host, e);
	} else if e.is_connect() {
		debug_error!("connect {}: {}", actual.host, e);
	} else if e.is_redirect() {
		debug_error!(
			method = ?method,
			url = ?url,
			final_url = ?e.url(),
			"Redirect loop {}: {}",
			actual.host,
			e,
		);
	} else {
		debug_error!("{}: {}", actual.host, e);
	}

	Err(e.into())
}

#[tracing::instrument(skip_all, name = "resolve")]
async fn get_actual_destination(server_name: &ServerName) -> Result<ActualDestination> {
	let cached;
	let cached_result = services()
		.globals
		.actual_destinations()
		.read()
		.await
		.get(server_name)
		.cloned();

	let (destination, host) = if let Some(result) = cached_result {
		cached = true;
		result
	} else {
		cached = false;
		resolve_actual_destination(server_name).await?
	};

	let string = destination.clone().into_https_string();
	Ok(ActualDestination {
		destination,
		host,
		string,
		cached,
	})
}

/// Returns: `actual_destination`, host header
/// Implemented according to the specification at <https://matrix.org/docs/spec/server_server/r0.1.4#resolving-server-names>
/// Numbers in comments below refer to bullet points in linked section of
/// specification
async fn resolve_actual_destination(destination: &'_ ServerName) -> Result<(FedDest, String)> {
	trace!("Finding actual destination for {destination}");
	let destination_str = destination.as_str().to_owned();
	let mut hostname = destination_str.clone();
	let actual_destination = match get_ip_with_port(&destination_str) {
		Some(host_port) => {
			debug!("1: IP literal with provided or default port");
			host_port
		},
		None => {
			if let Some(pos) = destination_str.find(':') {
				debug!("2: Hostname with included port");

				let (host, port) = destination_str.split_at(pos);
				query_and_cache_override(host, host, port.parse::<u16>().unwrap_or(8448)).await?;

				FedDest::Named(host.to_owned(), port.to_owned())
			} else {
				trace!("Requesting well known for {destination}");
				if let Some(delegated_hostname) = request_well_known(destination.as_str()).await? {
					debug!("3: A .well-known file is available");
					hostname = add_port_to_hostname(&delegated_hostname).into_uri_string();
					match get_ip_with_port(&delegated_hostname) {
						Some(host_and_port) => host_and_port, // 3.1: IP literal in .well-known file
						None => {
							if let Some(pos) = delegated_hostname.find(':') {
								debug!("3.2: Hostname with port in .well-known file");

								let (host, port) = delegated_hostname.split_at(pos);
								query_and_cache_override(host, host, port.parse::<u16>().unwrap_or(8448)).await?;

								FedDest::Named(host.to_owned(), port.to_owned())
							} else {
								trace!("Delegated hostname has no port in this branch");
								if let Some(hostname_override) = query_srv_record(&delegated_hostname).await? {
									debug!("3.3: SRV lookup successful");

									let force_port = hostname_override.port();
									query_and_cache_override(
										&delegated_hostname,
										&hostname_override.hostname(),
										force_port.unwrap_or(8448),
									)
									.await?;

									if let Some(port) = force_port {
										FedDest::Named(delegated_hostname, format!(":{port}"))
									} else {
										add_port_to_hostname(&delegated_hostname)
									}
								} else {
									debug!("3.4: No SRV records, just use the hostname from .well-known");
									query_and_cache_override(&delegated_hostname, &delegated_hostname, 8448).await?;
									add_port_to_hostname(&delegated_hostname)
								}
							}
						},
					}
				} else {
					trace!("4: No .well-known or an error occured");
					if let Some(hostname_override) = query_srv_record(&destination_str).await? {
						debug!("4: No .well-known; SRV record found");

						let force_port = hostname_override.port();
						query_and_cache_override(&hostname, &hostname_override.hostname(), force_port.unwrap_or(8448))
							.await?;

						if let Some(port) = force_port {
							FedDest::Named(hostname.clone(), format!(":{port}"))
						} else {
							add_port_to_hostname(&hostname)
						}
					} else {
						debug!("4: No .well-known; 5: No SRV record found");
						query_and_cache_override(&destination_str, &destination_str, 8448).await?;
						add_port_to_hostname(&destination_str)
					}
				}
			}
		},
	};

	// Can't use get_ip_with_port here because we don't want to add a port
	// to an IP address if it wasn't specified
	let hostname = if let Ok(addr) = hostname.parse::<SocketAddr>() {
		FedDest::Literal(addr)
	} else if let Ok(addr) = hostname.parse::<IpAddr>() {
		FedDest::Named(addr.to_string(), ":8448".to_owned())
	} else if let Some(pos) = hostname.find(':') {
		let (host, port) = hostname.split_at(pos);
		FedDest::Named(host.to_owned(), port.to_owned())
	} else {
		FedDest::Named(hostname, ":8448".to_owned())
	};

	debug!("Actual destination: {actual_destination:?} hostname: {hostname:?}");
	Ok((actual_destination, hostname.into_uri_string()))
}

#[tracing::instrument(skip_all, name = "well-known")]
async fn request_well_known(destination: &str) -> Result<Option<String>> {
	if !services()
		.globals
		.resolver
		.overrides
		.read()
		.unwrap()
		.contains_key(destination)
	{
		query_and_cache_override(destination, destination, 8448).await?;
	}

	let response = services()
		.globals
		.client
		.well_known
		.get(&format!("https://{destination}/.well-known/matrix/server"))
		.send()
		.await;

	trace!("response: {:?}", response);
	if let Err(e) = &response {
		debug!("error: {e:?}");
		return Ok(None);
	}

	let response = response?;
	if !response.status().is_success() {
		debug!("response not 2XX");
		return Ok(None);
	}

	let text = response.text().await?;
	trace!("response text: {:?}", text);
	if text.len() >= 12288 {
		debug_warn!("response contains junk");
		return Ok(None);
	}

	let body: serde_json::Value = serde_json::from_str(&text).unwrap_or_default();

	let m_server = body
		.get("m.server")
		.unwrap_or(&serde_json::Value::Null)
		.as_str()
		.unwrap_or_default();

	if ruma_identifiers_validation::server_name::validate(m_server).is_err() {
		debug_error!("response content missing or invalid");
		return Ok(None);
	}

	debug_info!("{:?} found at {:?}", destination, m_server);
	Ok(Some(m_server.to_owned()))
}

#[tracing::instrument(skip_all, name = "ip")]
async fn query_and_cache_override(overname: &'_ str, hostname: &'_ str, port: u16) -> Result<()> {
	match services()
		.globals
		.dns_resolver()
		.lookup_ip(hostname.to_owned())
		.await
	{
		Err(e) => handle_resolve_error(&e),
		Ok(override_ip) => {
			if hostname != overname {
				debug_info!("{:?} overriden by {:?}", overname, hostname);
			}
			services()
				.globals
				.resolver
				.overrides
				.write()
				.unwrap()
				.insert(overname.to_owned(), (override_ip.iter().collect(), port));

			Ok(())
		},
	}
}

#[tracing::instrument(skip_all, name = "srv")]
async fn query_srv_record(hostname: &'_ str) -> Result<Option<FedDest>> {
	fn handle_successful_srv(srv: &SrvLookup) -> Option<FedDest> {
		srv.iter().next().map(|result| {
			FedDest::Named(
				result.target().to_string().trim_end_matches('.').to_owned(),
				format!(":{}", result.port()),
			)
		})
	}

	async fn lookup_srv(hostname: &str) -> Result<SrvLookup, ResolveError> {
		debug!("querying SRV for {:?}", hostname);
		let hostname = hostname.trim_end_matches('.');
		services()
			.globals
			.dns_resolver()
			.srv_lookup(hostname.to_owned())
			.await
	}

	let hostnames = [format!("_matrix-fed._tcp.{hostname}."), format!("_matrix._tcp.{hostname}.")];

	for hostname in hostnames {
		match lookup_srv(&hostname).await {
			Ok(result) => return Ok(handle_successful_srv(&result)),
			Err(e) => handle_resolve_error(&e)?,
		}
	}

	Ok(None)
}

#[allow(clippy::single_match_else)]
fn handle_resolve_error(e: &ResolveError) -> Result<()> {
	use hickory_resolver::error::ResolveErrorKind;

	match *e.kind() {
		ResolveErrorKind::Io {
			..
		} => {
			error!("{e}");
			Err(Error::Error(e.to_string()))
		},
		_ => {
			debug_error!("{e}");
			Ok(())
		},
	}
}

fn sign_request<T>(destination: &ServerName, http_request: &mut http::Request<Vec<u8>>)
where
	T: OutgoingRequest + Debug,
{
	let mut req_map = serde_json::Map::new();
	if !http_request.body().is_empty() {
		req_map.insert(
			"content".to_owned(),
			serde_json::from_slice(http_request.body()).expect("body is valid json, we just created it"),
		);
	};

	req_map.insert("method".to_owned(), T::METADATA.method.to_string().into());
	req_map.insert(
		"uri".to_owned(),
		http_request
			.uri()
			.path_and_query()
			.expect("all requests have a path")
			.to_string()
			.into(),
	);
	req_map.insert("origin".to_owned(), services().globals.server_name().as_str().into());
	req_map.insert("destination".to_owned(), destination.as_str().into());

	let mut req_json = serde_json::from_value(req_map.into()).expect("valid JSON is valid BTreeMap");
	ruma::signatures::sign_json(
		services().globals.server_name().as_str(),
		services().globals.keypair(),
		&mut req_json,
	)
	.expect("our request json is what ruma expects");

	let req_json: serde_json::Map<String, serde_json::Value> =
		serde_json::from_slice(&serde_json::to_vec(&req_json).unwrap()).unwrap();

	let signatures = req_json["signatures"]
		.as_object()
		.expect("signatures object")
		.values()
		.map(|v| {
			v.as_object()
				.expect("server signatures object")
				.iter()
				.map(|(k, v)| (k, v.as_str().expect("server signature string")))
		});

	for signature_server in signatures {
		for s in signature_server {
			http_request.headers_mut().insert(
				AUTHORIZATION,
				HeaderValue::from_str(&format!(
					"X-Matrix origin={},key=\"{}\",sig=\"{}\"",
					services().globals.server_name(),
					s.0,
					s.1
				))
				.expect("formatted X-Matrix header"),
			);
		}
	}
}

fn validate_url(url: &reqwest::Url) -> Result<()> {
	if let Some(url_host) = url.host_str() {
		if let Ok(ip) = IPAddress::parse(url_host) {
			trace!("Checking request URL IP {ip:?}");
			validate_ip(&ip)?;
		}
	}

	Ok(())
}

fn validate_destination(destination: &ServerName) -> Result<()> {
	if destination == services().globals.server_name() {
		return Err(Error::bad_config("Won't send federation request to ourselves"));
	}

	if destination.is_ip_literal() || IPAddress::is_valid(destination.host()) {
		validate_destination_ip_literal(destination)?;
	}

	Ok(())
}

fn validate_destination_ip_literal(destination: &ServerName) -> Result<()> {
	trace!("Destination is an IP literal, checking against IP range denylist.",);
	debug_assert!(
		destination.is_ip_literal() || !IPAddress::is_valid(destination.host()),
		"Destination is not an IP literal."
	);
	let ip = IPAddress::parse(destination.host()).map_err(|e| {
		debug_error!("Failed to parse IP literal from string: {}", e);
		Error::BadServerResponse("Invalid IP address")
	})?;

	validate_ip(&ip)?;

	Ok(())
}

fn validate_ip(ip: &IPAddress) -> Result<()> {
	if !services().globals.valid_cidr_range(ip) {
		return Err(Error::BadServerResponse("Not allowed to send requests to this IP"));
	}

	Ok(())
}

fn get_ip_with_port(destination_str: &str) -> Option<FedDest> {
	if let Ok(destination) = destination_str.parse::<SocketAddr>() {
		Some(FedDest::Literal(destination))
	} else if let Ok(ip_addr) = destination_str.parse::<IpAddr>() {
		Some(FedDest::Literal(SocketAddr::new(ip_addr, 8448)))
	} else {
		None
	}
}

fn add_port_to_hostname(destination_str: &str) -> FedDest {
	let (host, port) = match destination_str.find(':') {
		None => (destination_str, ":8448"),
		Some(pos) => destination_str.split_at(pos),
	};

	FedDest::Named(host.to_owned(), port.to_owned())
}

impl FedDest {
	fn into_https_string(self) -> String {
		match self {
			Self::Literal(addr) => format!("https://{addr}"),
			Self::Named(host, port) => format!("https://{host}{port}"),
		}
	}

	fn into_uri_string(self) -> String {
		match self {
			Self::Literal(addr) => addr.to_string(),
			Self::Named(host, port) => host + &port,
		}
	}

	fn hostname(&self) -> String {
		match &self {
			Self::Literal(addr) => addr.ip().to_string(),
			Self::Named(host, _) => host.clone(),
		}
	}

	fn port(&self) -> Option<u16> {
		match &self {
			Self::Literal(addr) => Some(addr.port()),
			Self::Named(_, port) => port[1..].parse().ok(),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{add_port_to_hostname, get_ip_with_port, FedDest};

	#[test]
	fn ips_get_default_ports() {
		assert_eq!(
			get_ip_with_port("1.1.1.1"),
			Some(FedDest::Literal("1.1.1.1:8448".parse().unwrap()))
		);
		assert_eq!(
			get_ip_with_port("dead:beef::"),
			Some(FedDest::Literal("[dead:beef::]:8448".parse().unwrap()))
		);
	}

	#[test]
	fn ips_keep_custom_ports() {
		assert_eq!(
			get_ip_with_port("1.1.1.1:1234"),
			Some(FedDest::Literal("1.1.1.1:1234".parse().unwrap()))
		);
		assert_eq!(
			get_ip_with_port("[dead::beef]:8933"),
			Some(FedDest::Literal("[dead::beef]:8933".parse().unwrap()))
		);
	}

	#[test]
	fn hostnames_get_default_ports() {
		assert_eq!(
			add_port_to_hostname("example.com"),
			FedDest::Named(String::from("example.com"), String::from(":8448"))
		);
	}

	#[test]
	fn hostnames_keep_custom_ports() {
		assert_eq!(
			add_port_to_hostname("example.com:1337"),
			FedDest::Named(String::from("example.com"), String::from(":1337"))
		);
	}
}
