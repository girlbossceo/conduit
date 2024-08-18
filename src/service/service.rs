use std::{
	any::Any,
	collections::BTreeMap,
	fmt::Write,
	ops::Deref,
	sync::{Arc, OnceLock, RwLock, Weak},
};

use async_trait::async_trait;
use conduit::{err, error::inspect_log, utils::string::split_once_infallible, Err, Result, Server};
use database::Database;

/// Abstract interface for a Service
#[async_trait]
pub(crate) trait Service: Any + Send + Sync {
	/// Implement the construction of the service instance. Services are
	/// generally singletons so expect this to only be called once for a
	/// service type. Note that it may be called again after a server reload,
	/// but the prior instance will have been dropped first. Failure will
	/// shutdown the server with an error.
	fn build(args: Args<'_>) -> Result<Arc<impl Service>>
	where
		Self: Sized;

	/// Implement the service's worker loop. The service manager spawns a
	/// task and calls this function after all services have been built.
	async fn worker(self: Arc<Self>) -> Result<()> { Ok(()) }

	/// Interrupt the service. This is sent to initiate a graceful shutdown.
	/// The service worker should return from its work loop.
	fn interrupt(&self) {}

	/// Clear any caches or similar runtime state.
	fn clear_cache(&self) {}

	/// Memory usage report in a markdown string.
	fn memory_usage(&self, _out: &mut dyn Write) -> Result<()> { Ok(()) }

	/// Return the name of the service.
	/// i.e. `crate::service::make_name(std::module_path!())`
	fn name(&self) -> &str;
}

/// Args are passed to `Service::build` when a service is constructed. This
/// allows for arguments to change with limited impact to the many services.
pub(crate) struct Args<'a> {
	pub(crate) server: &'a Arc<Server>,
	pub(crate) db: &'a Arc<Database>,
	pub(crate) service: &'a Arc<Map>,
}

/// Dep is a reference to a service used within another service.
/// Circular-dependencies between services require this indirection.
pub(crate) struct Dep<T> {
	dep: OnceLock<Arc<T>>,
	service: Weak<Map>,
	name: &'static str,
}

pub(crate) type Map = RwLock<MapType>;
pub(crate) type MapType = BTreeMap<MapKey, MapVal>;
pub(crate) type MapVal = (Weak<dyn Service>, Weak<dyn Any + Send + Sync>);
pub(crate) type MapKey = String;

impl<T: Send + Sync + 'static> Deref for Dep<T> {
	type Target = Arc<T>;

	/// Dereference a dependency. The dependency must be ready or panics.
	fn deref(&self) -> &Self::Target {
		self.dep.get_or_init(|| {
			let service = self
				.service
				.upgrade()
				.expect("services map exists for dependency initialization.");

			require::<T>(&service, self.name)
		})
	}
}

impl<'a> Args<'a> {
	/// Create a lazy-reference to a service when constructing another Service.
	pub(crate) fn depend<T: Send + Sync + 'a + 'static>(&'a self, name: &'static str) -> Dep<T> {
		Dep::<T> {
			dep: OnceLock::new(),
			service: Arc::downgrade(self.service),
			name,
		}
	}

	/// Create a reference immediately to a service when constructing another
	/// Service. The other service must be constructed.
	pub(crate) fn require<T: Send + Sync + 'a + 'static>(&'a self, name: &'static str) -> Arc<T> {
		require::<T>(self.service, name)
	}
}

/// Reference a Service by name. Panics if the Service does not exist or was
/// incorrectly cast.
pub(crate) fn require<'a, 'b, T>(map: &'b Map, name: &'a str) -> Arc<T>
where
	T: Send + Sync + 'a + 'b + 'static,
{
	try_get::<T>(map, name)
		.inspect_err(inspect_log)
		.expect("Failure to reference service required by another service.")
}

/// Reference a Service by name. Returns None if the Service does not exist, but
/// panics if incorrectly cast.
///
/// # Panics
/// Incorrect type is not a silent failure (None) as the type never has a reason
/// to be incorrect.
pub(crate) fn get<'a, 'b, T>(map: &'b Map, name: &'a str) -> Option<Arc<T>>
where
	T: Send + Sync + 'a + 'b + 'static,
{
	map.read()
		.expect("locked for reading")
		.get(name)
		.map(|(_, s)| {
			s.upgrade().map(|s| {
				s.downcast::<T>()
					.expect("Service must be correctly downcast.")
			})
		})?
}

/// Reference a Service by name. Returns Err if the Service does not exist or
/// was incorrectly cast.
pub(crate) fn try_get<'a, 'b, T>(map: &'b Map, name: &'a str) -> Result<Arc<T>>
where
	T: Send + Sync + 'a + 'b + 'static,
{
	map.read()
		.expect("locked for reading")
		.get(name)
		.map_or_else(
			|| Err!("Service {name:?} does not exist or has not been built yet."),
			|(_, s)| {
				s.upgrade().map_or_else(
					|| Err!("Service {name:?} no longer exists."),
					|s| {
						s.downcast::<T>()
							.map_err(|_| err!("Service {name:?} must be correctly downcast."))
					},
				)
			},
		)
}

/// Utility for service implementations; see Service::name() in the trait.
#[inline]
pub(crate) fn make_name(module_path: &str) -> &str { split_once_infallible(module_path, "::").1 }
