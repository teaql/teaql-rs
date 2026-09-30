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
