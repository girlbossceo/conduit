mod err;
mod log;
mod panic;
mod response;
mod serde;

use std::{any::Any, borrow::Cow, convert::Infallible, fmt};

pub use self::log::*;
use crate::error;

#[derive(thiserror::Error)]
pub enum Error {
	#[error("PANIC!")]
	PanicAny(Box<dyn Any + Send>),
	#[error("PANIC! {0}")]
	Panic(&'static str, Box<dyn Any + Send + 'static>),

	// std
	#[error(transparent)]
	Fmt(#[from] fmt::Error),
	#[error(transparent)]
	FromUtf8(#[from] std::string::FromUtf8Error),
	#[error("I/O error: {0}")]
	Io(#[from] std::io::Error),
	#[error(transparent)]
	ParseFloat(#[from] std::num::ParseFloatError),
	#[error(transparent)]
	ParseInt(#[from] std::num::ParseIntError),
	#[error(transparent)]
	TryFromInt(#[from] std::num::TryFromIntError),
	#[error(transparent)]
	TryFromSlice(#[from] std::array::TryFromSliceError),
	#[error(transparent)]
	Utf8(#[from] std::str::Utf8Error),

	// third-party
	#[error(transparent)]
	CargoToml(#[from] cargo_toml::Error),
	#[error(transparent)]
	Clap(#[from] clap::error::Error),
	#[error(transparent)]
	Extension(#[from] axum::extract::rejection::ExtensionRejection),
	#[error(transparent)]
	Figment(#[from] figment::error::Error),
	#[error(transparent)]
	Http(#[from] http::Error),
	#[error(transparent)]
	HttpHeader(#[from] http::header::InvalidHeaderValue),
	#[error("Image error: {0}")]
	Image(#[from] image::error::ImageError),
	#[error("Join error: {0}")]
	JoinError(#[from] tokio::task::JoinError),
	#[error(transparent)]
	Json(#[from] serde_json::Error),
	#[error(transparent)]
	JsParseInt(#[from] ruma::JsParseIntError), // js_int re-export
	#[error(transparent)]
	JsTryFromInt(#[from] ruma::JsTryFromIntError), // js_int re-export
	#[error(transparent)]
	Path(#[from] axum::extract::rejection::PathRejection),
	#[error("Regex error: {0}")]
	Regex(#[from] regex::Error),
	#[error("Request error: {0}")]
	Reqwest(#[from] reqwest::Error),
	#[error("{0}")]
	SerdeDe(Cow<'static, str>),
	#[error("{0}")]
	SerdeSer(Cow<'static, str>),
	#[error(transparent)]
	TomlDe(#[from] toml::de::Error),
	#[error(transparent)]
	TomlSer(#[from] toml::ser::Error),
	#[error("Tracing filter error: {0}")]
	TracingFilter(#[from] tracing_subscriber::filter::ParseError),
	#[error("Tracing reload error: {0}")]
	TracingReload(#[from] tracing_subscriber::reload::Error),

	// ruma/conduwuit
	#[error("Arithmetic operation failed: {0}")]
	Arithmetic(Cow<'static, str>),
	#[error("{0}: {1}")]
	BadRequest(ruma::api::client::error::ErrorKind, &'static str), //TODO: remove
	#[error("{0}")]
	BadServerResponse(Cow<'static, str>),
	#[error("There was a problem with the '{0}' directive in your configuration: {1}")]
	Config(&'static str, Cow<'static, str>),
	#[error("{0}")]
	Conflict(&'static str), // This is only needed for when a room alias already exists
	#[error(transparent)]
	ContentDisposition(#[from] ruma::http_headers::ContentDispositionParseError),
	#[error("{0}")]
	Database(Cow<'static, str>),
	#[error("Remote server {0} responded with: {1}")]
	Federation(ruma::OwnedServerName, ruma::api::client::error::Error),
	#[error("{0} in {1}")]
	InconsistentRoomState(&'static str, ruma::OwnedRoomId),
	#[error(transparent)]
	IntoHttp(#[from] ruma::api::error::IntoHttpError),
	#[error(transparent)]
	Mxc(#[from] ruma::MxcUriError),
	#[error(transparent)]
	Mxid(#[from] ruma::IdParseError),
	#[error("from {0}: {1}")]
	Redaction(ruma::OwnedServerName, ruma::canonical_json::RedactionError),
	#[error("{0}: {1}")]
	Request(ruma::api::client::error::ErrorKind, Cow<'static, str>, http::StatusCode),
	#[error(transparent)]
	Ruma(#[from] ruma::api::client::error::Error),
	#[error("uiaa")]
	Uiaa(ruma::api::client::uiaa::UiaaInfo),

	// unique / untyped
	#[error("{0}")]
	Err(Cow<'static, str>),
}

impl Error {
	pub fn bad_database(message: &'static str) -> Self { crate::err!(Database(error!("{message}"))) }

	/// Sanitizes public-facing errors that can leak sensitive information.
	pub fn sanitized_string(&self) -> String {
		match self {
			Self::Database(..) => String::from("Database error occurred."),
			Self::Io(..) => String::from("I/O error occurred."),
			_ => self.to_string(),
		}
	}

	pub fn message(&self) -> String {
		match self {
			Self::Federation(ref origin, ref error) => format!("Answer from {origin}: {error}"),
			Self::Ruma(ref error) => response::ruma_error_message(error),
			_ => format!("{self}"),
		}
	}

	/// Returns the Matrix error code / error kind
	#[inline]
	pub fn kind(&self) -> ruma::api::client::error::ErrorKind {
		use ruma::api::client::error::ErrorKind::Unknown;

		match self {
			Self::Federation(_, error) => response::ruma_error_kind(error).clone(),
			Self::BadRequest(kind, ..) | Self::Request(kind, ..) => kind.clone(),
			_ => Unknown,
		}
	}

	pub fn status_code(&self) -> http::StatusCode {
		match self {
			Self::Federation(_, ref error) | Self::Ruma(ref error) => error.status_code,
			Self::Request(ref kind, _, code) => response::status_code(kind, *code),
			Self::BadRequest(ref kind, ..) => response::bad_request_code(kind),
			Self::Conflict(_) => http::StatusCode::CONFLICT,
			_ => http::StatusCode::INTERNAL_SERVER_ERROR,
		}
	}
}

impl fmt::Debug for Error {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { write!(f, "{self}") }
}

#[allow(clippy::fallible_impl_from)]
impl From<Infallible> for Error {
	#[cold]
	#[inline(never)]
	fn from(_e: Infallible) -> Self {
		panic!("infallible error should never exist");
	}
}

#[cold]
#[inline(never)]
pub fn infallible(_e: &Infallible) {
	panic!("infallible error should never exist");
}
