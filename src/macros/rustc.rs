use proc_macro::TokenStream;
use quote::quote;

pub(super) fn flags_capture(args: TokenStream) -> TokenStream {
	let cargo_crate_name = std::env::var("CARGO_CRATE_NAME");
	let crate_name = match cargo_crate_name.as_ref() {
		Err(_) => return args,
		Ok(crate_name) => crate_name.trim_start_matches("conduit_"),
	};

	let flag = std::env::args().collect::<Vec<_>>();
	let flag_len = flag.len();
	let ret = quote! {
		pub static RUSTC_FLAGS: [&str; #flag_len] = [#( #flag ),*];

		#[conduit_core::ctor]
		fn _set_rustc_flags() {
			conduit_core::info::rustc::FLAGS.lock().expect("locked").insert(#crate_name, &RUSTC_FLAGS);
		}

		// static strings have to be yanked on module unload
		#[conduit_core::dtor]
		fn _unset_rustc_flags() {
			conduit_core::info::rustc::FLAGS.lock().expect("locked").remove(#crate_name);
		}
	};

	ret.into()
}
