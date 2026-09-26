use std::collections::{BTreeMap, BTreeSet, HashMap};
use teaql_core::Value;
use teaql_runtime::InternalIdGenerator;
use teaql_runtime::{EntityKey, EntityRuntimeState};

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
        Ok(self
            .current
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }
}

#[test]
fn test_incremental_ledger_observability() {
    let id_generator = SequentialIdGenerator::new(1);
    let root = EntityRuntimeState::default();

    // --- 1. Create (Insert) ---
    // 假设这些对象是 new 出来的
    let task1_id = id_generator.generate_id("Task").unwrap(); // ID 1
    let task1_key = EntityKey::new("Task", task1_id);
    root.set(
        task1_key.clone(),
        "name",
        Value::Text("New Task 1".to_string()),
    );
    root.set(task1_key.clone(), "status", Value::U64(1001));

    let task2_id = id_generator.generate_id("Task").unwrap(); // ID 2
    let task2_key = EntityKey::new("Task", task2_id);
    root.set(
        task2_key.clone(),
        "name",
        Value::Text("New Task 2".to_string()),
    );
    root.set(task2_key.clone(), "status", Value::U64(1001));

    // --- 2. Update (Modify) ---
    // 假设这些是加载出来的已存在对象
    let existing_task_id = 99_u64;
    let existing_task_key = EntityKey::new("Task", existing_task_id);
    root.set(
        existing_task_key.clone(),
        "name",
        Value::Text("Updated Name".to_string()),
    );

    let existing_task_id2 = 100_u64;
    let existing_task_key2 = EntityKey::new("Task", existing_task_id2);
    // 此对象有着相同的 Signature (只更新了 name)
    root.set(
        existing_task_key2.clone(),
        "name",
        Value::Text("Another Updated Name".to_string()),
    );

    let existing_task_id3 = 101_u64;
    let existing_task_key3 = EntityKey::new("Task", existing_task_id3);
    // 此对象有着不同的 Signature (更新了 name AND status)
    root.set(
        existing_task_key3.clone(),
        "name",
        Value::Text("Different Sig".to_string()),
    );
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
    root.set(
        existing_task_key.clone(),
        "name",
        Value::Text("Double Updated Name".to_string()),
    );

    // Simulate version tampering on task 100
    root.set_comment("Hacker tries to override version");
    root.set(existing_task_key2.clone(), "version", Value::U64(999));

    // Setup original versions registry
    root.set_original_version(existing_task_key.clone(), 3);
    root.set_original_version(existing_task_key2.clone(), 5);
    root.set_original_version(existing_task_key3.clone(), 2);
    root.set_original_version(delete_task_key.clone(), 1);
    root.set_original_version(delete_task_key2.clone(), 1);

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
        ids.sort_by_key(|a| a.try_u64().unwrap());
        // Note: in a real executor, it groups by the expected version or uses parameter binding
        println!(
            "> BATCH DELETE FROM Task WHERE id IN {:?} AND version = [各自的基准版本]",
            ids
        );
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
        keys.sort_by_key(|a| a.id.try_u64().unwrap());
        println!(
            "> BATCH UPDATE/INSERT Task SET [{}] FOR IDs: {:?}",
            sig,
            keys.iter().map(|k| &k.id).collect::<Vec<_>>()
        );
    }
    println!("==========================================================\n");
}

#[test]
fn successful_commit_cleanup_removes_all_replayable_root_state() {
    let root = EntityRuntimeState::default();
    let created = EntityKey::new("CompanyTenant", 1_u64);
    let deleted = EntityKey::new("UserAccount", 2_u64);

    root.set(created.clone(), "name", Value::Text("TeaQL".to_owned()));
    root.mark_as_new(created.clone());
    root.set_original_version(created.clone(), 3);
    root.mark_as_delete(deleted);
    root.set_comment("create tenant");

    root.clear_committed();

    assert!(root.current_change_set().changes().is_empty());
    assert!(root.new_keys().is_empty());
    assert!(root.deleted_keys().is_empty());
    assert_eq!(root.get_original_version(&created), None);
    assert_eq!(root.get_comment(), None);

    // A later independent save on the same UserContext/root starts clean.
    let next = EntityKey::new("UserAccount", 3_u64);
    root.set(next.clone(), "name", Value::Text("Ada".to_owned()));
    assert_eq!(root.current_change_set().changes().len(), 1);
    assert!(root.current_change_set().changes().contains_key(&next));
}

#[test]
fn deterministic_randomized_ledger_sequences_preserve_identity_and_cleanup_invariants() {
    const SEEDS: [u64; 8] = [
        0x5445_4151_4c52_554e,
        1,
        2,
        3,
        0xdead_beef,
        0x0123_4567_89ab_cdef,
        u64::MAX - 1,
        u64::MAX,
    ];
    const STEPS: usize = 2_048;
    const ROOT_COUNT: usize = 3;

    for seed in SEEDS {
        let roots = std::array::from_fn::<_, ROOT_COUNT, _>(|_| EntityRuntimeState::default());
        let mut fields = std::array::from_fn::<_, ROOT_COUNT, _>(|_| BTreeMap::new());
        let mut new_keys = std::array::from_fn::<_, ROOT_COUNT, _>(|_| BTreeSet::new());
        let mut deleted_keys = std::array::from_fn::<_, ROOT_COUNT, _>(|_| BTreeSet::new());
        let mut versions = std::array::from_fn::<_, ROOT_COUNT, _>(|_| BTreeMap::new());
        let mut random = seed;

        for step in 0..STEPS {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let root_index = ((random >> 57) as usize) % ROOT_COUNT;
            let root = &roots[root_index];
            let entity = if (random >> 63) == 0 {
                "Order"
            } else {
                "Invoice"
            };
            let id = ((random >> 17) % 32) + 1;
            let operation = ((random >> 8) % 6) as u8;
            let model_key = (entity.to_owned(), id);
            let runtime_key = EntityKey::new(entity, id);

            match operation {
                0 | 1 => {
                    let field = if operation == 0 { "amount" } else { "status" };
                    let value = Value::U64(random & 0xffff);
                    root.set(runtime_key.clone(), field, value.clone());
                    fields[root_index].insert((entity.to_owned(), id, field.to_owned()), value);
                }
                2 => {
                    root.mark_as_new(runtime_key.clone());
                    new_keys[root_index].insert(model_key.clone());
                }
                3 => {
                    root.mark_as_delete(runtime_key.clone());
                    fields[root_index].retain(|(stored_entity, stored_id, _), _| {
                        stored_entity != entity || *stored_id != id
                    });
                    deleted_keys[root_index].insert(model_key.clone());
                }
                4 => {
                    let version = ((random >> 32) % 20) as i64;
                    root.set_original_version(runtime_key.clone(), version);
                    versions[root_index].insert(model_key.clone(), version);
                }
                5 => {
                    root.clear_committed();
                    fields[root_index].clear();
                    new_keys[root_index].clear();
                    deleted_keys[root_index].clear();
                    versions[root_index].clear();
                }
                _ => unreachable!(),
            }

            for observed_root in 0..ROOT_COUNT {
                let actual_fields = roots[observed_root]
                    .current_change_set()
                    .changes()
                    .iter()
                    .flat_map(|(key, values)| {
                        values.iter().map(|(field, value)| {
                            (
                                (
                                    key.entity.to_string(),
                                    key.id.try_u64().unwrap(),
                                    field.clone(),
                                ),
                                value.clone(),
                            )
                        })
                    })
                    .collect::<BTreeMap<_, _>>();
                assert_eq!(
                    actual_fields, fields[observed_root],
                    "seed={seed:#x} step={step} root={observed_root} changed fields"
                );
                assert_eq!(
                    roots[observed_root]
                        .new_keys()
                        .iter()
                        .map(|key| (key.entity.to_string(), key.id.try_u64().unwrap()))
                        .collect::<BTreeSet<_>>(),
                    new_keys[observed_root],
                    "seed={seed:#x} step={step} root={observed_root} new keys"
                );
                assert_eq!(
                    roots[observed_root]
                        .deleted_keys()
                        .iter()
                        .map(|key| (key.entity.to_string(), key.id.try_u64().unwrap()))
                        .collect::<BTreeSet<_>>(),
                    deleted_keys[observed_root],
                    "seed={seed:#x} step={step} root={observed_root} deleted keys"
                );
                for ((stored_entity, stored_id), expected) in &versions[observed_root] {
                    let key = EntityKey::new(stored_entity.clone(), *stored_id);
                    assert_eq!(
                        roots[observed_root].get_original_version(&key),
                        Some(*expected),
                        "seed={seed:#x} step={step} root={observed_root} version={stored_entity}#{stored_id}"
                    );
                }
            }

            let same_numeric_id_other_type = EntityKey::new(
                if entity == "Order" {
                    "Invoice"
                } else {
                    "Order"
                },
                id,
            );
            assert_ne!(runtime_key, same_numeric_id_other_type);
        }
    }
}

#[test]
fn deterministic_nested_change_sets_preserve_latest_value_and_support_rollback() {
    const SEED: u64 = 0x4348_414e_4745_5345;
    const STEPS: usize = 4_096;

    let root = EntityRuntimeState::default();
    let mut stack = vec![BTreeMap::<(String, u64, String), Value>::new()];
    let mut random = SEED;

    for step in 0..STEPS {
        random = random
            .wrapping_mul(2_862_933_555_777_941_757)
            .wrapping_add(3_037_000_493);
        let entity = if (random & 1) == 0 {
            "Order"
        } else {
            "Invoice"
        };
        let id = ((random >> 11) % 16) + 1;
        let field = if ((random >> 7) & 1) == 0 {
            "amount"
        } else {
            "status"
        };
        let key = (entity.to_owned(), id, field.to_owned());
        let runtime_key = EntityKey::new(entity, id);

        match (random >> 3) % 5 {
            0 | 1 => {
                let value = Value::U64(random & 0xffff);
                root.set(runtime_key.clone(), field, value.clone());
                stack.last_mut().unwrap().insert(key.clone(), value);
            }
            2 if stack.len() < 8 => {
                root.push_change_set();
                stack.push(BTreeMap::new());
            }
            3 if stack.len() > 1 => {
                assert_eq!(
                    root.pop_change_set(),
                    stack.pop().map(|changes| {
                        let mut expected = teaql_runtime::EntityChangeSet::default();
                        for ((entity, id, field), value) in changes {
                            expected.set(EntityKey::new(entity, id), field, value);
                        }
                        expected
                    })
                );
            }
            _ => {
                root.clear_current_change_set();
                stack.last_mut().unwrap().clear();
            }
        }

        let expected = stack
            .iter()
            .rev()
            .find_map(|changes| changes.get(&key))
            .cloned();
        assert_eq!(
            root.get(&runtime_key, field),
            expected,
            "seed={SEED:#x} step={step} stack_depth={}",
            stack.len()
        );
    }
}
