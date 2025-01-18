use std::sync::Arc;

use conduwuit::{implement, Result};
use ruma::ServerName;

use crate::{globals, Dep};

pub struct Service {
	services: Services,
}

struct Services {
	globals: Dep<globals::Service>,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			services: Services {
				globals: args.depend::<globals::Service>("globals"),
			},
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
pub fn is_remote_server_forbidden(&self, server_name: &ServerName) -> bool {
	// Forbidden if NOT (allowed is empty OR allowed contains server)
	// OR forbidden contains server
	!(self
		.services
		.globals
		.config
		.allowed_remote_server_names
		.is_empty()
		|| self
			.services
			.globals
			.config
			.allowed_remote_server_names
			.contains(server_name))
		|| self
			.services
			.globals
			.config
			.forbidden_remote_server_names
			.contains(server_name)
}
