use std::collections::{BTreeSet, HashMap};
use teaql_core::{Value, Record};
use teaql_runtime::{EntityRoot, EntityKey};
use teaql_runtime::InternalIdGenerator;

#[derive(Debug)]
struct SequentialIdGenerator {
    current: std::sync::atomic::AtomicU64,
}

impl SequentialIdGenerator {
    fn new(start: u64) -> Self {
        Self {
            current: std::sync::atomic::AtomicU64::new(start),
        }
    }
}

impl InternalIdGenerator for SequentialIdGenerator {
    fn generate_id(&self, _entity: &str) -> Result<u64, teaql_runtime::RuntimeError> {
        Ok(self.current.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }
}

#[test]
fn test_incremental_ledger_observability() {
    let id_generator = SequentialIdGenerator::new(1);
    let root = EntityRoot::default();
    
    // --- 1. Create (Insert) ---
    // 假设这些对象是 new 出来的
    let task1_id = id_generator.generate_id("Task").unwrap(); // ID 1
    let task1_key = EntityKey::new("Task", task1_id);
    root.set(task1_key.clone(), "name", Value::Text("New Task 1".to_string()));
    root.set(task1_key.clone(), "status", Value::U64(1001));
    
    let task2_id = id_generator.generate_id("Task").unwrap(); // ID 2
    let task2_key = EntityKey::new("Task", task2_id);
    root.set(task2_key.clone(), "name", Value::Text("New Task 2".to_string()));
    root.set(task2_key.clone(), "status", Value::U64(1001));

    // --- 2. Update (Modify) ---
    // 假设这些是加载出来的已存在对象
    let existing_task_id = 99_u64;
    let existing_task_key = EntityKey::new("Task", existing_task_id);
    root.set(existing_task_key.clone(), "name", Value::Text("Updated Name".to_string()));
    
    let existing_task_id2 = 100_u64;
    let existing_task_key2 = EntityKey::new("Task", existing_task_id2);
    // 此对象有着相同的 Signature (只更新了 name)
    root.set(existing_task_key2.clone(), "name", Value::Text("Another Updated Name".to_string()));
    
    let existing_task_id3 = 101_u64;
    let existing_task_key3 = EntityKey::new("Task", existing_task_id3);
    // 此对象有着不同的 Signature (更新了 name AND status)
    root.set(existing_task_key3.clone(), "name", Value::Text("Different Sig".to_string()));
    root.set(existing_task_key3.clone(), "status", Value::U64(1004));

    // --- 3. Delete ---
    let delete_task_id = 200_u64;
    let delete_task_key = EntityKey::new("Task", delete_task_id);
    root.mark_as_delete(delete_task_key.clone());
    
    let delete_task_id2 = 201_u64;
    let delete_task_key2 = EntityKey::new("Task", delete_task_id2);
    root.mark_as_delete(delete_task_key2.clone());

    // --- 4. Double Update & Version Tampering Detection ---
    root.set_comment("Admin forces state override");
    
    // Simulate double update on task 99
    root.set(existing_task_key.clone(), "name", Value::Text("Double Updated Name".to_string()));
    
    // Simulate version tampering on task 100
    root.set_comment("Hacker tries to override version");
    root.set(existing_task_key2.clone(), "version", Value::U64(999));
    
    // Setup original versions registry
    root.record_version(existing_task_key.clone(), 3);
    root.record_version(existing_task_key2.clone(), 5);
    root.record_version(existing_task_key3.clone(), 2);
    root.record_version(delete_task_key.clone(), 1);
    root.record_version(delete_task_key2.clone(), 1);

    // --- OBSERVABILITY OUTPUT ---
    let change_set = root.current_change_set();
    
    println!("\n=== Raw Ledger State (扁平化账本状态) ===");
    for (key, record) in change_set.changes() {
        println!("{:?} => {:?}", key, record);
    }
    
    let deleted = root.deleted_keys();
    println!("\n=== Deleted Keys (待删除主键) ===");
    for key in &deleted {
        let version = root.get_original_version(key).unwrap_or(0);
        println!("{:?} (Original Version: {})", key, version);
    }
    
    println!("\n=== Simulated Executor Batching (执行引擎的智能合并) ===");
    
    // Simulate batching deletes
    if !deleted.is_empty() {
        let mut ids: Vec<_> = deleted.iter().map(|k| k.id.clone()).collect();
        ids.sort_by(|a, b| a.try_u64().unwrap().cmp(&b.try_u64().unwrap()));
        // Note: in a real executor, it groups by the expected version or uses parameter binding
        println!("> BATCH DELETE FROM Task WHERE id IN {:?} AND version = [各自的基准版本]", ids);
    }
    
    // Simulate grouping updates by signature
    let mut batches: HashMap<String, Vec<EntityKey>> = HashMap::new();
    for (key, record) in change_set.changes() {
        // DETECT VERSION TAMPERING
        if record.contains_key("version") {
            println!("! [FATAL] 侦测到手工篡改 version 字段: {:?}", key);
        }
        
        let mut keys: Vec<String> = record.keys().cloned().collect();
        keys.sort();
        let signature = keys.join(", ");
        batches.entry(signature).or_default().push(key.clone());
    }
    
    for (sig, mut keys) in batches {
        keys.sort_by(|a, b| a.id.try_u64().unwrap().cmp(&b.id.try_u64().unwrap()));
        println!("> BATCH UPDATE/INSERT Task SET [{}] FOR IDs: {:?}", sig, keys.iter().map(|k| &k.id).collect::<Vec<_>>());
    }
    println!("==========================================================\n");
}
