use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use teaql_runtime::{AeadEntityReferenceCodec, UserContext};

const GOLDEN: &str = "tqr1.AAAAAjMzMzMzMzMzMzMzM3bKiZgRSQQhfIj2cBXRDZIloUGHWLBp8QrXL_aejwIXPFtvV_E71O7wbOXy3cvYo_SwxvuS-89x572T9CO_pDAY4tbjWCNv";

fn main() {
    let now: DateTime<Utc> = DateTime::parse_from_rfc3339("2026-09-26T12:00:00Z")
        .expect("valid fixed time")
        .to_utc();
    let codec = AeadEntityReferenceCodec::new(2, [(2, vec![0x22; 32])])
        .expect("valid key ring")
        .with_clock(move || now)
        .with_nonce_source(|| [0x33; 12]);
    let context = UserContext::new().with_entity_reference_codec(Arc::new(codec));

    let token = context
        .encode_entity_reference("OrderItem", 42, 7, "edit-order", Duration::from_secs(3600))
        .expect("encode through UserContext");
    assert_eq!(token, GOLDEN, "portable vector must match Java/Go/.NET");
    let claims = context
        .decode_entity_reference(&token, "OrderItem", "edit-order")
        .expect("decode through UserContext");
    assert_eq!((claims.id, claims.version), (42, 7));
    assert!(
        context
            .decode_entity_reference(&token, "OrderItem", "view-order")
            .is_err(),
        "purpose substitution must fail closed"
    );

    println!("PASS Rust security foundations: opaque reference is portable and purpose-bound");
}
