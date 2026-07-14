use std::collections::BTreeMap;

use teaql_core::{Record, Value};

use crate::collector::Collector;
use crate::error::LinuxProviderError;

pub struct SystemInfoCollector;

impl Collector for SystemInfoCollector {
    fn entity_name(&self) -> &str {
        "SystemInfo"
    }

    fn collect_all(&self) -> Result<Vec<Record>, LinuxProviderError> {
        use procfs::Current;

        let mut record = BTreeMap::new();
        record.insert("id".to_owned(), Value::I64(1));
        record.insert("version".to_owned(), Value::I64(1));

        let hostname = std::fs::read_to_string("/proc/sys/kernel/hostname")
            .unwrap_or_default()
            .trim()
            .to_owned();
        record.insert("hostname".to_owned(), Value::Text(hostname));

        let (mem_total, mem_available) = procfs::Meminfo::current()
            .map(|m| (m.mem_total as i64, m.mem_available.unwrap_or(0) as i64))
            .unwrap_or((0, 0));
        record.insert("memory_total_bytes".to_owned(), Value::I64(mem_total));
        record.insert(
            "memory_available_bytes".to_owned(),
            Value::I64(mem_available),
        );

        {
            use std::str::FromStr;
            let (avg1, avg5, avg15) = procfs::LoadAverage::current()
                .map(|la| {
                    (
                        teaql_core::Decimal::from_str(&la.one.to_string()).unwrap_or_default(),
                        teaql_core::Decimal::from_str(&la.five.to_string()).unwrap_or_default(),
                        teaql_core::Decimal::from_str(&la.fifteen.to_string()).unwrap_or_default(),
                    )
                })
                .unwrap_or_default();
            record.insert("load_avg_1".to_owned(), Value::Decimal(avg1));
            record.insert("load_avg_5".to_owned(), Value::Decimal(avg5));
            record.insert("load_avg_15".to_owned(), Value::Decimal(avg15));
        }

        {
            use std::str::FromStr;
            let uptime_dec = procfs::Uptime::current()
                .map(|u| teaql_core::Decimal::from_str(&u.uptime.to_string()).unwrap_or_default())
                .unwrap_or_default();
            record.insert("uptime_seconds".to_owned(), Value::Decimal(uptime_dec));
        }

        let cpu_count = procfs::CpuInfo::current()
            .map(|c| c.num_cores() as i64)
            .unwrap_or(0);
        record.insert("cpu_count".to_owned(), Value::I64(cpu_count));

        Ok(vec![record])
    }
}
