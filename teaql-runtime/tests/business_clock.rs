use chrono::{TimeZone, Utc};
use teaql_runtime::{BusinessDate, FixedBusinessClock, UserContext};

#[test]
fn context_owned_clock_is_deterministic() {
    let expected = Utc
        .with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
        .single()
        .expect("valid fixture time");
    let context = UserContext::new().with_business_clock(FixedBusinessClock::new(expected));

    assert_eq!(context.business_time(), expected);
    assert_eq!(context.business_date(), expected.date_naive());
}

#[test]
fn existing_business_date_resource_remains_a_compatible_override() {
    let expected_time = Utc
        .with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
        .single()
        .expect("valid fixture time");
    let override_date = chrono::NaiveDate::from_ymd_opt(2040, 1, 2).expect("valid fixture date");
    let mut context =
        UserContext::new().with_business_clock(FixedBusinessClock::new(expected_time));
    context.insert_resource(BusinessDate(override_date));

    assert_eq!(context.business_time(), expected_time);
    assert_eq!(context.business_date(), override_date);
}

#[test]
fn standalone_fix_consumes_the_configured_business_clock() {
    let expected = Utc
        .with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
        .single()
        .unwrap();
    let context = UserContext::new().with_business_clock(FixedBusinessClock::new(expected));
    assert_eq!(context.fix_time().as_millis(), expected.timestamp_millis());
}

#[test]
fn the_next_standalone_fix_observes_the_replacement_clock() {
    let first = Utc
        .with_ymd_and_hms(2032, 2, 29, 10, 15, 30)
        .single()
        .unwrap();
    let next = first + chrono::Duration::hours(1);
    let mut context = UserContext::new().with_business_clock(FixedBusinessClock::new(first));
    assert_eq!(context.fix_time().as_millis(), first.timestamp_millis());
    context.set_business_clock(FixedBusinessClock::new(next));
    assert_eq!(context.fix_time().as_millis(), next.timestamp_millis());
}
