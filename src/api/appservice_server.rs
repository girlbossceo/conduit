use std::{fmt::Debug, mem, time::Duration};

use bytes::BytesMut;
use ruma::api::{appservice::Registration, IncomingResponse, MatrixVersion, OutgoingRequest, SendAccessToken};
use tracing::warn;

use crate::{services, utils, Error, Result};

/// Sends a request to an appservice
///
/// Only returns None if there is no url specified in the appservice
/// registration file
pub(crate) async fn send_request<T>(registration: Registration, request: T) -> Option<Result<T::IncomingResponse>>
where
	T: OutgoingRequest + Debug,
{
	if let Some(destination) = registration.url {
		let hs_token = registration.hs_token.as_str();

		let mut http_request = request
			.try_into_http_request::<BytesMut>(
				&destination,
				SendAccessToken::IfRequired(hs_token),
				&[MatrixVersion::V1_0],
			)
			.map_err(|e| {
				warn!("Failed to find destination {}: {}", destination, e);
				Error::BadServerResponse("Invalid destination")
			})
			.unwrap()
			.map(bytes::BytesMut::freeze);

		let mut parts = http_request.uri().clone().into_parts();
		let old_path_and_query = parts.path_and_query.unwrap().as_str().to_owned();
		let symbol = if old_path_and_query.contains('?') {
			"&"
		} else {
			"?"
		};

		parts.path_and_query = Some((old_path_and_query + symbol + "access_token=" + hs_token).parse().unwrap());
		*http_request.uri_mut() = parts.try_into().expect("our manipulation is always valid");

		let mut reqwest_request =
			reqwest::Request::try_from(http_request).expect("all http requests are valid reqwest requests");

		*reqwest_request.timeout_mut() = Some(Duration::from_secs(120));

		let url = reqwest_request.url().clone();
		let mut response = match services().globals.default_client().execute(reqwest_request).await {
			Ok(r) => r,
			Err(e) => {
				warn!(
					"Could not send request to appservice {} at {}: {}",
					registration.id, destination, e
				);
				return Some(Err(e.into()));
			},
		};

		// reqwest::Response -> http::Response conversion
		let status = response.status();
		let mut http_response_builder = http::Response::builder().status(status).version(response.version());
		mem::swap(
			response.headers_mut(),
			http_response_builder.headers_mut().expect("http::response::Builder is usable"),
		);

		let body = response.bytes().await.unwrap_or_else(|e| {
			warn!("server error: {}", e);
			Vec::new().into()
		}); // TODO: handle timeout

		if !status.is_success() {
			warn!(
				"Appservice returned bad response {} {}\n{}\n{:?}",
				destination,
				status,
				url,
				utils::string_from_bytes(&body)
			);
		}

		let response = T::IncomingResponse::try_from_http_response(
			http_response_builder.body(body).expect("reqwest body is valid http body"),
		);
		Some(response.map_err(|_| {
			warn!("Appservice returned invalid response bytes {}\n{}", destination, url);
			Error::BadServerResponse("Server returned bad response.")
		}))
	} else {
		None
	}
}
