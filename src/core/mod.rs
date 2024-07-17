pub mod alloc;
pub mod config;
pub mod debug;
pub mod error;
pub mod log;
pub mod metrics;
pub mod mods;
pub mod pdu;
pub mod server;
pub mod utils;
pub mod version;

pub use config::Config;
pub use error::Error;
pub use pdu::{PduBuilder, PduCount, PduEvent};
pub use server::Server;
pub use version::version;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(not(conduit_mods))]
pub mod mods {
	#[macro_export]
	macro_rules! mod_ctor {
		() => {};
	}
	#[macro_export]
	macro_rules! mod_dtor {
		() => {};
	}
}
