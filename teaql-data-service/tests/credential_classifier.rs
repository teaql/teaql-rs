use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use teaql_data_service::is_credential_log_name;

thread_local! {
    static COUNTING: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
struct CountAlloc;
fn record() {
    if COUNTING.try_with(Cell::get).unwrap_or(false) {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
    }
}
unsafe impl GlobalAlloc for CountAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record();
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record();
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record();
        unsafe { System.realloc(ptr, layout, size) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: CountAlloc = CountAlloc;

const WORDS: [&str; 13] = [
    "password",
    "passwd",
    "passphrase",
    "privatekey",
    "secret",
    "accesstoken",
    "refreshtoken",
    "idtoken",
    "apikey",
    "authorization",
    "credential",
    "sessiontoken",
    "magiclinktoken",
];

fn original_classifier(name: &str) -> bool {
    let normalized: String = name
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_lowercase())
        .collect();
    WORDS.iter().any(|word| normalized.contains(word))
}

#[test]
fn credential_matching_preserves_normalization_and_substrings() {
    let mut cases = vec![
        String::new(),
        "库存地址".into(),
        "ordinary_order_no".into(),
        "passwor".into(),
        "pasxword".into(),
        "api2key".into(),
        "apıkey".into(),
        "paßword".into(),
        "\0PASSWORD\0".into(),
        "ssecret".into(),
    ];
    for word in WORDS {
        for offset in 0..40 {
            for separator in ["", "_", "-.", "界🦀", "\0"] {
                let decorated = word
                    .to_uppercase()
                    .chars()
                    .map(|c| c.to_string())
                    .collect::<Vec<_>>()
                    .join(separator);
                cases.push(format!("{}{}suffix", "x".repeat(offset), decorated));
            }
        }
        cases.push(format!("{}{}", "z".repeat(100_000), word));
        for split in 1..word.len() {
            cases.push(format!("{}9{}", &word[..split], &word[split..]));
        }
    }
    // Deterministic mixed input exercises partial matches and rolling suffixes.
    let alphabet = [
        "p", "a", "s", "w", "o", "r", "d", "K", "9", "_", "界", "é", "🦀",
    ];
    let mut seed = 17u64;
    for i in 0..4000 {
        let mut value = String::new();
        for _ in 0..(i % 100) {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            value.push_str(alphabet[(seed >> 32) as usize % alphabet.len()]);
        }
        if i % 3 == 0 {
            value.push_str(WORDS[i % WORDS.len()]);
        }
        cases.push(value);
    }
    for name in cases {
        assert_eq!(
            is_credential_log_name(&name),
            original_classifier(&name),
            "{name:?}"
        );
    }
}

#[test]
fn credential_matching_allocates_nothing() {
    let names = [
        "ordinary_order_no".to_owned(),
        "private_key".to_owned(),
        "a界P_i-KEY".to_owned(),
        "é".repeat(10000),
        "x".repeat(10000),
    ];
    ALLOCATIONS.with(|count| count.set(0));
    COUNTING.with(|counting| counting.set(true));
    for _ in 0..10 {
        for name in &names {
            std::hint::black_box(is_credential_log_name(std::hint::black_box(name)));
        }
    }
    COUNTING.with(|counting| counting.set(false));
    assert_eq!(ALLOCATIONS.with(Cell::get), 0);
}
