#![allow(dead_code)]

use rust_rocksdb::{
	BlockBasedOptions, Cache, DBCompactionStyle, DBCompressionType, DBRecoveryMode, Env, LogLevel, Options,
	UniversalCompactOptions, UniversalCompactionStopStyle,
};

use super::Config;

/// Create database-wide options suitable for opening the database. This also
/// sets our default column options in case of opening a column with the same
/// resulting value. Note that we require special per-column options on some
/// columns, therefor columns should only be opened after passing this result
/// through cf_options().
pub(crate) fn db_options(config: &Config, env: &Env, row_cache: &Cache, col_cache: &Cache) -> Options {
	let mut opts = Options::default();

	// Logging
	set_logging_defaults(&mut opts, config);

	// Processing
	let threads = if config.rocksdb_parallelism_threads == 0 {
		num_cpus::get_physical() // max cores if user specified 0
	} else {
		config.rocksdb_parallelism_threads
	};

	opts.set_max_background_jobs(threads.try_into().unwrap());
	opts.set_max_subcompactions(threads.try_into().unwrap());
	opts.set_max_file_opening_threads(0);

	// IO
	opts.set_manual_wal_flush(true);
	opts.set_use_direct_reads(true);
	opts.set_use_direct_io_for_flush_and_compaction(true);
	if config.rocksdb_optimize_for_spinning_disks {
		// speeds up opening DB on hard drives
		opts.set_skip_checking_sst_file_sizes_on_db_open(true);
		opts.set_skip_stats_update_on_db_open(true);
		//opts.set_max_file_opening_threads(threads.try_into().unwrap());
	}

	// Blocks
	let mut table_opts = table_options(config);
	table_opts.set_block_cache(col_cache);
	opts.set_row_cache(row_cache);

	// Buffers
	opts.set_write_buffer_size(2 * 1024 * 1024);
	opts.set_max_write_buffer_number(2);
	opts.set_min_write_buffer_number(1);

	// Files
	opts.set_max_total_wal_size(96 * 1024 * 1024);
	opts.set_level_zero_file_num_compaction_trigger(2);
	set_level_defaults(&mut opts, config);
	opts.set_ttl(14 * 24 * 60 * 60);

	// Compression
	set_compression_defaults(&mut opts, config);

	// Misc
	opts.create_if_missing(true);

	// Default: https://github.com/facebook/rocksdb/wiki/WAL-Recovery-Modes#ktoleratecorruptedtailrecords
	//
	// Unclean shutdowns of a Matrix homeserver are likely to be fine when
	// recovered in this manner as it's likely any lost information will be
	// restored via federation.
	opts.set_wal_recovery_mode(match config.rocksdb_recovery_mode {
		0 => DBRecoveryMode::AbsoluteConsistency,
		1 => DBRecoveryMode::TolerateCorruptedTailRecords,
		2 => DBRecoveryMode::PointInTime,
		3 => DBRecoveryMode::SkipAnyCorruptedRecord,
		4_u8..=u8::MAX => unimplemented!(),
	});

	opts.set_block_based_table_factory(&table_opts);
	opts.set_env(env);
	opts
}

/// Adjust options for the specific column by name. Provide the result of
/// db_options() as the argument to this function and use the return value in
/// the arguments to open the specific column.
pub(crate) fn cf_options(name: &str, mut opts: Options, config: &Config) -> Options {
	match name {
		"backupid_algorithm"
		| "backupid_etag"
		| "backupkeyid_backup"
		| "roomid_shortroomid"
		| "shorteventid_shortstatehash"
		| "shorteventid_eventid"
		| "shortstatekey_statekey"
		| "shortstatehash_statediff"
		| "userdevicetxnid_response"
		| "userfilterid_filter" => set_for_sequential_small_uc(&mut opts, config),
		&_ => {},
	}

	opts
}

fn set_logging_defaults(opts: &mut Options, config: &Config) {
	let rocksdb_log_level = match config.rocksdb_log_level.as_ref() {
		"debug" => LogLevel::Debug,
		"info" => LogLevel::Info,
		"warn" => LogLevel::Warn,
		"fatal" => LogLevel::Fatal,
		_ => LogLevel::Error,
	};

	opts.set_log_level(rocksdb_log_level);
	opts.set_max_log_file_size(config.rocksdb_max_log_file_size);
	opts.set_log_file_time_to_roll(config.rocksdb_log_time_to_roll);
	opts.set_keep_log_file_num(config.rocksdb_max_log_files);
	opts.set_stats_dump_period_sec(0);
}

fn set_compression_defaults(opts: &mut Options, config: &Config) {
	let rocksdb_compression_algo = match config.rocksdb_compression_algo.as_ref() {
		"zlib" => DBCompressionType::Zlib,
		"lz4" => DBCompressionType::Lz4,
		"bz2" => DBCompressionType::Bz2,
		_ => DBCompressionType::Zstd,
	};

	if config.rocksdb_bottommost_compression {
		opts.set_bottommost_compression_type(rocksdb_compression_algo);
		opts.set_bottommost_zstd_max_train_bytes(0, true);

		// -14 w_bits is only read by zlib.
		opts.set_bottommost_compression_options(-14, config.rocksdb_bottommost_compression_level, 0, 0, true);
	}

	// -14 w_bits is only read by zlib.
	opts.set_compression_options(-14, config.rocksdb_compression_level, 0, 0);
	opts.set_compression_type(rocksdb_compression_algo);
}

fn set_for_random_small_uc(opts: &mut Options, config: &Config) {
	let uco = uc_options(config);
	set_for_random_small(opts, config);
	opts.set_universal_compaction_options(&uco);
	opts.set_compaction_style(DBCompactionStyle::Universal);
	opts.set_level_zero_file_num_compaction_trigger(1);
}

fn set_for_sequential_small_uc(opts: &mut Options, config: &Config) {
	let uco = uc_options(config);
	set_for_sequential_small(opts, config);
	opts.set_universal_compaction_options(&uco);
	opts.set_compaction_style(DBCompactionStyle::Universal);
	opts.set_level_zero_file_num_compaction_trigger(1);
}

fn set_for_random_small(opts: &mut Options, config: &Config) {
	set_for_random(opts, config);

	opts.set_write_buffer_size(1024 * 1024);
	opts.set_target_file_size_base(65536);
	opts.set_max_bytes_for_level_base(131072);
}

fn set_for_sequential_small(opts: &mut Options, config: &Config) {
	set_for_random(opts, config);

	opts.set_write_buffer_size(1024 * 1024);
	opts.set_target_file_size_base(65536);
	opts.set_max_bytes_for_level_base(131072);
}

fn set_for_random(opts: &mut Options, config: &Config) {
	set_level_defaults(opts, config);

	let pri = "compaction_pri=kOldestSmallestSeqFirst";
	opts.set_options_from_string(pri)
		.expect("set compaction priority string");

	opts.set_max_bytes_for_level_base(8 * 1024 * 1024);
	opts.set_max_bytes_for_level_multiplier(1.0);
	opts.set_max_bytes_for_level_multiplier_additional(&[0, 1, 1, 3, 7, 15, 31]);
}

fn set_for_sequential(opts: &mut Options, config: &Config) {
	set_level_defaults(opts, config);

	let pri = "compaction_pri=kOldestLargestSeqFirst";
	opts.set_options_from_string(pri)
		.expect("set compaction priority string");

	opts.set_target_file_size_base(2 * 1024 * 1024);
	opts.set_target_file_size_multiplier(2);

	opts.set_max_bytes_for_level_base(32 * 1024 * 1024);
	opts.set_max_bytes_for_level_multiplier(1.0);
	opts.set_max_bytes_for_level_multiplier_additional(&[0, 1, 1, 3, 7, 15, 31]);
}

fn set_level_defaults(opts: &mut Options, _config: &Config) {
	opts.set_target_file_size_base(1024 * 1024);
	opts.set_target_file_size_multiplier(2);

	opts.set_level_compaction_dynamic_level_bytes(false);
	opts.set_max_bytes_for_level_base(8 * 1024 * 1024);
	opts.set_max_bytes_for_level_multiplier(2.0);
}

fn uc_options(_config: &Config) -> UniversalCompactOptions {
	let mut opts = UniversalCompactOptions::default();

	opts.set_stop_style(UniversalCompactionStopStyle::Total);
	opts.set_max_size_amplification_percent(10000);
	opts.set_compression_size_percent(-1);
	opts.set_size_ratio(1);

	opts.set_min_merge_width(2);
	opts.set_max_merge_width(16);

	opts
}

fn table_options(_config: &Config) -> BlockBasedOptions {
	let mut opts = BlockBasedOptions::default();

	opts.set_block_size(4 * 1024);
	opts.set_metadata_block_size(4 * 1024);

	opts.set_bloom_filter(9.6, true);
	opts.set_optimize_filters_for_memory(true);
	opts.set_cache_index_and_filter_blocks(true);
	opts.set_pin_top_level_index_and_filter(true);

	opts
}
