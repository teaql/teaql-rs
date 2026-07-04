use std::collections::BTreeMap;

use teaql_core::{Record, Value};

use crate::collector::Collector;
use crate::error::LinuxProviderError;

/// Collects per-thread information by iterating all processes and their tasks.
pub struct ThreadCollector;

impl Collector for ThreadCollector {
    fn entity_name(&self) -> &str {
        "Thread"
    }

    fn collect_all(&self) -> Result<Vec<Record>, LinuxProviderError> {
        let mut records = Vec::new();

        let all_procs = procfs::process::all_processes()?;
        for proc_result in all_procs {
            let process = match proc_result {
                Ok(p) => p,
                Err(_) => continue,
            };

            let process_pid = process.pid();

            let tasks = match process.tasks() {
                Ok(t) => t,
                Err(_) => continue,
            };

            for task_result in tasks {
                let task = match task_result {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                let stat = match task.stat() {
                    Ok(s) => s,
                    Err(_) => continue,
                };

                let tid = stat.pid;
                let mut record = BTreeMap::new();
                record.insert("id".to_owned(), Value::I64(stat.pid as i64));
                record.insert("version".to_owned(), Value::I64(1));
                record.insert("tid".to_owned(), Value::I64(tid as i64));
                record.insert("name".to_owned(), Value::Text(stat.comm.clone()));
                record.insert("state".to_owned(), Value::Text(stat.state.to_string()));
                record.insert(
                    "process_pid".to_owned(),
                    Value::I64(process_pid as i64),
                );
                record.insert(
                    "cpu_user_ticks".to_owned(),
                    Value::I64(stat.utime.min(i32::MAX as u64) as i64),
                );
                record.insert(
                    "cpu_system_ticks".to_owned(),
                    Value::I64(stat.stime.min(i32::MAX as u64) as i64),
                );

                records.push(record);
            }
        }

        Ok(records)
    }
}
