use crate::Result;

pub const EMPTY: &str = "";

/// Constant expression to bypass format! if the argument is a string literal
/// but not a format string. If the literal is a format string then String is
/// returned otherwise the input (i.e. &'static str) is returned. If multiple
/// arguments are provided the first is assumed to be a format string.
#[macro_export]
macro_rules! format_maybe {
	($s:literal) => {
		if $crate::is_format!($s) { std::format!($s).into() } else { $s.into() }
	};

	($($args:expr),*) => {
		std::format!($($args),*).into()
	};
}

/// Constant expression to decide if a literal is a format string. Note: could
/// use some improvement.
#[macro_export]
macro_rules! is_format {
	($s:literal) => {
		::const_str::contains!($s, "{") && ::const_str::contains!($s, "}")
	};
}

/// Find the common prefix from a collection of strings and return a slice
/// ```
/// use conduit_core::utils::string::common_prefix;
/// let input = ["conduwuit", "conduit", "construct"];
/// common_prefix(&input) == "con";
/// ```
#[must_use]
#[allow(clippy::string_slice)]
pub fn common_prefix<'a>(choice: &'a [&str]) -> &'a str {
	choice.first().map_or(EMPTY, move |best| {
		choice.iter().skip(1).fold(*best, |best, choice| {
			&best[0..choice
				.char_indices()
				.zip(best.char_indices())
				.take_while(|&(a, b)| a == b)
				.count()]
		})
	})
}

#[inline]
#[must_use]
pub fn split_once_infallible<'a>(input: &'a str, delim: &'_ str) -> (&'a str, &'a str) {
	input.split_once(delim).unwrap_or((input, EMPTY))
}

/// Parses the bytes into a string.
pub fn string_from_bytes(bytes: &[u8]) -> Result<String> {
	let str: &str = str_from_bytes(bytes)?;
	Ok(str.to_owned())
}

/// Parses the bytes into a string.
#[inline]
pub fn str_from_bytes(bytes: &[u8]) -> Result<&str> { Ok(std::str::from_utf8(bytes)?) }
