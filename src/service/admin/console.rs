#![cfg(feature = "console")]
use std::{
	collections::VecDeque,
	sync::{Arc, Mutex},
};

use conduit::{debug, defer, error, log};
use futures_util::future::{AbortHandle, Abortable};
use ruma::events::room::message::RoomMessageEventContent;
use rustyline_async::{Readline, ReadlineError, ReadlineEvent};
use termimad::MadSkin;
use tokio::task::JoinHandle;

use crate::services;

pub struct Console {
	worker_join: Mutex<Option<JoinHandle<()>>>,
	input_abort: Mutex<Option<AbortHandle>>,
	command_abort: Mutex<Option<AbortHandle>>,
	history: Mutex<VecDeque<String>>,
	output: MadSkin,
}

const PROMPT: &str = "uwu> ";
const HISTORY_LIMIT: usize = 48;

impl Console {
	#[must_use]
	pub fn new() -> Arc<Self> {
		Arc::new(Self {
			worker_join: None.into(),
			input_abort: None.into(),
			command_abort: None.into(),
			history: VecDeque::with_capacity(HISTORY_LIMIT).into(),
			output: configure_output(MadSkin::default_dark()),
		})
	}

	pub(super) async fn handle_signal(self: &Arc<Self>, sig: &'static str) {
		if !services().server.running() {
			self.interrupt();
		} else if sig == "SIGINT" {
			self.interrupt_command();
			self.start().await;
		}
	}

	pub async fn start(self: &Arc<Self>) {
		let mut worker_join = self.worker_join.lock().expect("locked");
		if worker_join.is_none() {
			let self_ = Arc::clone(self);
			_ = worker_join.insert(services().server.runtime().spawn(self_.worker()));
		}
	}

	pub async fn close(self: &Arc<Self>) {
		self.interrupt();
		let Some(worker_join) = self.worker_join.lock().expect("locked").take() else {
			return;
		};

		_ = worker_join.await;
	}

	pub fn interrupt(self: &Arc<Self>) {
		self.interrupt_command();
		self.interrupt_readline();
		self.worker_join
			.lock()
			.expect("locked")
			.as_ref()
			.map(JoinHandle::abort);
	}

	pub fn interrupt_readline(self: &Arc<Self>) {
		if let Some(input_abort) = self.input_abort.lock().expect("locked").take() {
			debug!("Interrupting console readline...");
			input_abort.abort();
		}
	}

	pub fn interrupt_command(self: &Arc<Self>) {
		if let Some(command_abort) = self.command_abort.lock().expect("locked").take() {
			debug!("Interrupting console command...");
			command_abort.abort();
		}
	}

	#[tracing::instrument(skip_all, name = "console")]
	async fn worker(self: Arc<Self>) {
		debug!("session starting");
		while services().server.running() {
			match self.readline().await {
				Ok(event) => match event {
					ReadlineEvent::Line(string) => self.clone().handle(string).await,
					ReadlineEvent::Interrupted => continue,
					ReadlineEvent::Eof => break,
					ReadlineEvent::Quit => services()
						.server
						.shutdown()
						.unwrap_or_else(error::default_log),
				},
				Err(error) => match error {
					ReadlineError::Closed => break,
					ReadlineError::IO(error) => {
						error!("console I/O: {error:?}");
						break;
					},
				},
			}
		}

		debug!("session ending");
		self.worker_join.lock().expect("locked").take();
	}

	async fn readline(self: &Arc<Self>) -> Result<ReadlineEvent, ReadlineError> {
		let _suppression = log::Suppress::new(&services().server);

		let (mut readline, _writer) = Readline::new(PROMPT.to_owned())?;
		readline.set_tab_completer(Self::tab_complete);
		self.set_history(&mut readline);

		let future = readline.readline();

		let (abort, abort_reg) = AbortHandle::new_pair();
		let future = Abortable::new(future, abort_reg);
		_ = self.input_abort.lock().expect("locked").insert(abort);
		defer! {{
			_ = self.input_abort.lock().expect("locked").take();
		}}

		let Ok(result) = future.await else {
			return Ok(ReadlineEvent::Eof);
		};

		readline.flush()?;
		result
	}

	async fn handle(self: Arc<Self>, line: String) {
		if line.trim().is_empty() {
			return;
		}

		self.add_history(line.clone());
		let future = self.clone().process(line);
		let (abort, abort_reg) = AbortHandle::new_pair();
		let future = Abortable::new(future, abort_reg);
		_ = self.command_abort.lock().expect("locked").insert(abort);
		defer! {{
			_ = self.command_abort.lock().expect("locked").take();
		}}

		_ = future.await;
	}

	async fn process(self: Arc<Self>, line: String) {
		match services().admin.command_in_place(line, None).await {
			Ok(Some(content)) => self.output(content).await,
			Err(e) => error!("processing command: {e}"),
			_ => (),
		}
	}

	async fn output(self: Arc<Self>, output_content: RoomMessageEventContent) {
		self.output.print_text(output_content.body());
	}

	fn set_history(&self, readline: &mut Readline) {
		self.history
			.lock()
			.expect("locked")
			.iter()
			.rev()
			.for_each(|entry| {
				readline
					.add_history_entry(entry.clone())
					.expect("added history entry");
			});
	}

	fn add_history(&self, line: String) {
		let mut history = self.history.lock().expect("locked");
		history.push_front(line);
		history.truncate(HISTORY_LIMIT);
	}

	fn tab_complete(line: &str) -> String {
		services()
			.admin
			.complete_command(line)
			.unwrap_or_else(|| line.to_owned())
	}
}

fn configure_output(mut output: MadSkin) -> MadSkin {
	use termimad::{crossterm::style::Color, Alignment, CompoundStyle, LineStyle};

	let code_style = CompoundStyle::with_fgbg(Color::AnsiValue(40), Color::AnsiValue(234));
	output.inline_code = code_style.clone();
	output.code_block = LineStyle {
		left_margin: 0,
		right_margin: 0,
		align: Alignment::Left,
		compound_style: code_style,
	};

	let table_style = CompoundStyle::default();
	output.table = LineStyle {
		left_margin: 1,
		right_margin: 1,
		align: Alignment::Left,
		compound_style: table_style,
	};

	output
}
