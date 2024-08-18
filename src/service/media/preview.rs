use std::{io::Cursor, time::SystemTime};

use conduit::{debug, utils, warn, Err, Result};
use conduit_core::implement;
use image::ImageReader as ImgReader;
use ipaddress::IPAddress;
use ruma::Mxc;
use serde::Serialize;
use url::Url;
use webpage::HTML;

use super::{Service, MXC_LENGTH};

#[derive(Serialize, Default)]
pub struct UrlPreviewData {
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:title"))]
	pub title: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:description"))]
	pub description: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:image"))]
	pub image: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "matrix:image:size"))]
	pub image_size: Option<usize>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:image:width"))]
	pub image_width: Option<u32>,
	#[serde(skip_serializing_if = "Option::is_none", rename(serialize = "og:image:height"))]
	pub image_height: Option<u32>,
}

#[implement(Service)]
pub async fn remove_url_preview(&self, url: &str) -> Result<()> {
	// TODO: also remove the downloaded image
	self.db.remove_url_preview(url)
}

#[implement(Service)]
pub async fn set_url_preview(&self, url: &str, data: &UrlPreviewData) -> Result<()> {
	let now = SystemTime::now()
		.duration_since(SystemTime::UNIX_EPOCH)
		.expect("valid system time");
	self.db.set_url_preview(url, data, now)
}

#[implement(Service)]
pub async fn download_image(&self, url: &str) -> Result<UrlPreviewData> {
	let client = &self.services.client.url_preview;
	let image = client.get(url).send().await?.bytes().await?;
	let mxc = Mxc {
		server_name: self.services.globals.server_name(),
		media_id: &utils::random_string(MXC_LENGTH),
	};

	self.create(&mxc, None, None, None, &image).await?;

	let (width, height) = match ImgReader::new(Cursor::new(&image)).with_guessed_format() {
		Err(_) => (None, None),
		Ok(reader) => match reader.into_dimensions() {
			Err(_) => (None, None),
			Ok((width, height)) => (Some(width), Some(height)),
		},
	};

	Ok(UrlPreviewData {
		image: Some(mxc.to_string()),
		image_size: Some(image.len()),
		image_width: width,
		image_height: height,
		..Default::default()
	})
}

#[implement(Service)]
pub async fn get_url_preview(&self, url: &str) -> Result<UrlPreviewData> {
	if let Some(preview) = self.db.get_url_preview(url) {
		return Ok(preview);
	}

	// ensure that only one request is made per URL
	let _request_lock = self.url_preview_mutex.lock(url).await;

	match self.db.get_url_preview(url) {
		Some(preview) => Ok(preview),
		None => self.request_url_preview(url).await,
	}
}

#[implement(Service)]
async fn request_url_preview(&self, url: &str) -> Result<UrlPreviewData> {
	if let Ok(ip) = IPAddress::parse(url) {
		if !self.services.globals.valid_cidr_range(&ip) {
			return Err!(BadServerResponse("Requesting from this address is forbidden"));
		}
	}

	let client = &self.services.client.url_preview;
	let response = client.head(url).send().await?;

	if let Some(remote_addr) = response.remote_addr() {
		if let Ok(ip) = IPAddress::parse(remote_addr.ip().to_string()) {
			if !self.services.globals.valid_cidr_range(&ip) {
				return Err!(BadServerResponse("Requesting from this address is forbidden"));
			}
		}
	}

	let Some(content_type) = response
		.headers()
		.get(reqwest::header::CONTENT_TYPE)
		.and_then(|x| x.to_str().ok())
	else {
		return Err!(Request(Unknown("Unknown Content-Type")));
	};
	let data = match content_type {
		html if html.starts_with("text/html") => self.download_html(url).await?,
		img if img.starts_with("image/") => self.download_image(url).await?,
		_ => return Err!(Request(Unknown("Unsupported Content-Type"))),
	};

	self.set_url_preview(url, &data).await?;

	Ok(data)
}

#[implement(Service)]
async fn download_html(&self, url: &str) -> Result<UrlPreviewData> {
	let client = &self.services.client.url_preview;
	let mut response = client.get(url).send().await?;

	let mut bytes: Vec<u8> = Vec::new();
	while let Some(chunk) = response.chunk().await? {
		bytes.extend_from_slice(&chunk);
		if bytes.len() > self.services.globals.url_preview_max_spider_size() {
			debug!(
				"Response body from URL {} exceeds url_preview_max_spider_size ({}), not processing the rest of the \
				 response body and assuming our necessary data is in this range.",
				url,
				self.services.globals.url_preview_max_spider_size()
			);
			break;
		}
	}
	let body = String::from_utf8_lossy(&bytes);
	let Ok(html) = HTML::from_string(body.to_string(), Some(url.to_owned())) else {
		return Err!(Request(Unknown("Failed to parse HTML")));
	};

	let mut data = match html.opengraph.images.first() {
		None => UrlPreviewData::default(),
		Some(obj) => self.download_image(&obj.url).await?,
	};

	let props = html.opengraph.properties;

	/* use OpenGraph title/description, but fall back to HTML if not available */
	data.title = props.get("title").cloned().or(html.title);
	data.description = props.get("description").cloned().or(html.description);

	Ok(data)
}

#[implement(Service)]
pub fn url_preview_allowed(&self, url_str: &str) -> bool {
	let url: Url = match Url::parse(url_str) {
		Ok(u) => u,
		Err(e) => {
			warn!("Failed to parse URL from a str: {}", e);
			return false;
		},
	};

	if ["http", "https"]
		.iter()
		.all(|&scheme| scheme != url.scheme().to_lowercase())
	{
		debug!("Ignoring non-HTTP/HTTPS URL to preview: {}", url);
		return false;
	}

	let host = match url.host_str() {
		None => {
			debug!("Ignoring URL preview for a URL that does not have a host (?): {}", url);
			return false;
		},
		Some(h) => h.to_owned(),
	};

	let allowlist_domain_contains = self
		.services
		.globals
		.url_preview_domain_contains_allowlist();
	let allowlist_domain_explicit = self
		.services
		.globals
		.url_preview_domain_explicit_allowlist();
	let denylist_domain_explicit = self.services.globals.url_preview_domain_explicit_denylist();
	let allowlist_url_contains = self.services.globals.url_preview_url_contains_allowlist();

	if allowlist_domain_contains.contains(&"*".to_owned())
		|| allowlist_domain_explicit.contains(&"*".to_owned())
		|| allowlist_url_contains.contains(&"*".to_owned())
	{
		debug!("Config key contains * which is allowing all URL previews. Allowing URL {}", url);
		return true;
	}

	if !host.is_empty() {
		if denylist_domain_explicit.contains(&host) {
			debug!(
				"Host {} is not allowed by url_preview_domain_explicit_denylist (check 1/4)",
				&host
			);
			return false;
		}

		if allowlist_domain_explicit.contains(&host) {
			debug!("Host {} is allowed by url_preview_domain_explicit_allowlist (check 2/4)", &host);
			return true;
		}

		if allowlist_domain_contains
			.iter()
			.any(|domain_s| domain_s.contains(&host.clone()))
		{
			debug!("Host {} is allowed by url_preview_domain_contains_allowlist (check 3/4)", &host);
			return true;
		}

		if allowlist_url_contains
			.iter()
			.any(|url_s| url.to_string().contains(&url_s.to_string()))
		{
			debug!("URL {} is allowed by url_preview_url_contains_allowlist (check 4/4)", &host);
			return true;
		}

		// check root domain if available and if user has root domain checks
		if self.services.globals.url_preview_check_root_domain() {
			debug!("Checking root domain");
			match host.split_once('.') {
				None => return false,
				Some((_, root_domain)) => {
					if denylist_domain_explicit.contains(&root_domain.to_owned()) {
						debug!(
							"Root domain {} is not allowed by url_preview_domain_explicit_denylist (check 1/3)",
							&root_domain
						);
						return true;
					}

					if allowlist_domain_explicit.contains(&root_domain.to_owned()) {
						debug!(
							"Root domain {} is allowed by url_preview_domain_explicit_allowlist (check 2/3)",
							&root_domain
						);
						return true;
					}

					if allowlist_domain_contains
						.iter()
						.any(|domain_s| domain_s.contains(&root_domain.to_owned()))
					{
						debug!(
							"Root domain {} is allowed by url_preview_domain_contains_allowlist (check 3/3)",
							&root_domain
						);
						return true;
					}
				},
			}
		}
	}

	false
}
