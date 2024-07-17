use std::{
	any::Any,
	panic::{panic_any, RefUnwindSafe, UnwindSafe},
};

use super::Error;
use crate::debug;

impl UnwindSafe for Error {}
impl RefUnwindSafe for Error {}

impl Error {
	pub fn panic(self) -> ! { panic_any(self.into_panic()) }

	#[must_use]
	pub fn from_panic(e: Box<dyn Any + Send>) -> Self { Self::Panic(debug::panic_str(&e), e) }

	pub fn into_panic(self) -> Box<dyn Any + Send + 'static> {
		match self {
			Self::Panic(_, e) | Self::PanicAny(e) => e,
			Self::JoinError(e) => e.into_panic(),
			_ => Box::new(self),
		}
	}

	/// Get the panic message string.
	pub fn panic_str(self) -> Option<&'static str> {
		self.is_panic()
			.then_some(debug::panic_str(&self.into_panic()))
	}

	/// Check if the Error is trafficking a panic object.
	#[inline]
	pub fn is_panic(&self) -> bool {
		match &self {
			Self::Panic(..) | Self::PanicAny(..) => true,
			Self::JoinError(e) => e.is_panic(),
			_ => false,
		}
	}
}
