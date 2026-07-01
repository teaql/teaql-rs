use std::collections::BTreeMap;

use teaql_core::{Record, Value};

use crate::collector::Collector;
use crate::error::LinuxProviderError;

/// Collects per-process information from /proc by iterating all processes.
pub struct ProcessCollector;

impl Collector for ProcessCollector {
    fn entity_name(&self) -> &str {
        "Process"
    }

    fn collect_all(&self) -> Result<Vec<Record>, LinuxProviderError> {
        let page_size = procfs::page_size();
        let ticks_per_second = procfs::ticks_per_second();
        let boot_time_secs = procfs::boot_time_secs().unwrap_or(0);
        let mut records = Vec::new();

        let all_procs = procfs::process::all_processes()?;
        for proc_result in all_procs {
            let process = match proc_result {
                Ok(p) => p,
                Err(_) => continue,
            };

            let stat = match process.stat() {
                Ok(s) => s,
                Err(_) => continue,
            };

            let cmdline = process
                .cmdline()
                .unwrap_or_default()
                .join(" ");

            let pid = stat.pid;
            let mut record = BTreeMap::new();
            record.insert("id".to_owned(), Value::I64(pid as i64));
            record.insert("version".to_owned(), Value::I64(1));
            record.insert("pid".to_owned(), Value::I64(pid as i64));
            record.insert("name".to_owned(), Value::Text(stat.comm.clone()));
            record.insert("state".to_owned(), Value::Text(stat.state.to_string()));
            record.insert("ppid".to_owned(), Value::I64(stat.ppid as i64));
            record.insert("cmdline".to_owned(), Value::Text(cmdline));
            record.insert(
                "thread_count".to_owned(),
                Value::I64(stat.num_threads),
            );
            record.insert(
                "memory_rss_kb".to_owned(),
                Value::I64(((stat.rss * page_size / 1024).min(i32::MAX as u64)) as i64),
            );
            record.insert(
                "memory_vms_kb".to_owned(),
                Value::I64(((stat.vsize / 1024).min(i32::MAX as u64)) as i64),
            );
            record.insert(
                "cpu_user_ticks".to_owned(),
                Value::I64((stat.utime).min(i32::MAX as u64) as i64),
            );
            record.insert(
                "cpu_system_ticks".to_owned(),
                Value::I64((stat.stime).min(i32::MAX as u64) as i64),
            );

            let create_time_secs = boot_time_secs + (stat.starttime / ticks_per_second);
            if let Some(create_time) = chrono::DateTime::from_timestamp(create_time_secs as i64, 0) {
                record.insert("create_time".to_owned(), Value::Timestamp(create_time));
                record.insert("update_time".to_owned(), Value::Timestamp(create_time));
            }

            records.push(record);
        }

        Ok(records)
    }
}
