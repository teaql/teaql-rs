
use std::collections::BTreeMap;
use crate::TeaqlRuntime;
use crate::Q;
use teaql_core::Entity as _;
use crate::request_support::AuditedSave as _;

pub trait IntoU64 {
    fn into_u64(self) -> u64;
}

impl IntoU64 for u64 {
    fn into_u64(self) -> u64 {
        self
    }
}

impl IntoU64 for Option<&teaql_core::Value> {
    fn into_u64(self) -> u64 {
        self.and_then(|v| v.try_u64()).unwrap_or_default()
    }
}

#[derive(Debug, Copy, Clone)]
pub enum SampleDataScale {
    Tiny,
    Small,
    Medium,
}

pub struct SampleDataPlan {
    pub scale: SampleDataScale,
    pub seed: u64,
}

impl SampleDataPlan {
    pub fn small() -> Self {
        Self {
            scale: SampleDataScale::Small,
            seed: 0,
        }
    }
}

pub struct SampleDataReport {
    pub generated: BTreeMap<&'static str, usize>,
    pub skipped: Vec<SampleDataSkipped>,
}

pub struct SampleDataSkipped {
    pub entity: &'static str,
    pub reason: String,
}

#[derive(Debug)]
pub struct SampleDataError {
    message: String,
}

impl SampleDataError {
    fn from_display(error: impl std::fmt::Display) -> Self {
        Self {
            message: error.to_string(),
        }
    }
}

impl std::fmt::Display for SampleDataError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for SampleDataError {}

pub struct SampleDataState {
    pub plan: SampleDataPlan,
    pub references: BTreeMap<&'static str, Vec<u64>>,
    pub generated: BTreeMap<&'static str, usize>,
    pub skipped: Vec<SampleDataSkipped>,
}

impl SampleDataState {
    pub fn new(plan: SampleDataPlan) -> Self {
        Self {
            plan,
            references: BTreeMap::new(),
            generated: BTreeMap::new(),
            skipped: Vec::new(),
        }
    }

    pub fn add_reference(&mut self, entity: &'static str, id: u64) {
        self.references.entry(entity).or_default().push(id);
    }

    pub fn ids(&self, entity: &'static str) -> &[u64] {
        self.references.get(entity).map(|v| v.as_slice()).unwrap_or(&[])
    }

    pub fn pick_id(&self, entity: &'static str, salt: usize) -> Option<u64> {
        let ids = self.ids(entity);
        if ids.is_empty() {
            None
        } else {
            Some(ids[salt % ids.len()])
        }
    }

    pub fn pick_unused_id(&self, entity: &'static str, salt: usize, used: &std::collections::HashSet<u64>) -> Option<u64> {
        let ids = self.ids(entity);
        if ids.is_empty() {
            return None;
        }

        let best_id = ids[salt % ids.len()];
        if !used.contains(&best_id) {
            return Some(best_id);
        }

        for id in ids {
            if !used.contains(id) {
                return Some(*id);
            }
        }

        Some(best_id)
    }

    pub fn record_generated(&mut self, entity: &'static str) {
        *self.generated.entry(entity).or_default() += 1;
    }

    pub fn record_skipped(&mut self, entity: &'static str, reason: String) {
        self.skipped.push(SampleDataSkipped { entity, reason });
    }

    pub fn into_report(self) -> SampleDataReport {
        SampleDataReport {
            generated: self.generated,
            skipped: self.skipped,
        }
    }
}

pub async fn generate_sample_data<C>(
    context: &C,
    plan: SampleDataPlan,
) -> Result<SampleDataReport, SampleDataError>
where
    C: TeaqlRuntime + ?Sized,
{
    log::info!("Starting sample data generation. Scale: {:?}, Seed: {}", plan.scale, plan.seed);
    let mut state = SampleDataState::new(plan);

    load_root_platforms(context, &mut state).await?; //depth: 0


    generate_customer_orders(context, &mut state).await?;

    generate_order_items(context, &mut state).await?;

    generate_payments(context, &mut state).await?;

    generate_shipments(context, &mut state).await?;

    generate_payment_attempts(context, &mut state).await?;


    let report = state.into_report();
    log::info!("Sample data generation completed successfully. Generated: {} tables, Skipped: {} tables.", report.generated.len(), report.skipped.len());
    Ok(report)
}

async fn load_root_platforms<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized,
{
    let list = Q::platforms().comment("what: inspect existing entities before sample-data initialization").purpose("why: avoid duplicate sample records").execute_for_list(context).await.unwrap_or_default();
    for item in list {
        state.add_reference(crate::Platform::ENTITY_NAME, item.id().into_u64());
    }
    Ok(())
}


async fn generate_customer_orders<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized,
{
        if state.ids("Platform").is_empty() {
            state.record_skipped(crate::CustomerOrder::ENTITY_NAME, "Required dependency Platform is missing in reference pool".to_string());
            log::info!("Skipped generating Customer Order: Required dependency Platform is missing in reference pool.");
            return Ok(());
        }


    let object_fields_count = 0 + 1;
    let base_fanout = std::cmp::max(1, object_fields_count) * 20;

    let fanout = match state.plan.scale {
        SampleDataScale::Tiny => base_fanout,
        SampleDataScale::Small => base_fanout * 5,
        SampleDataScale::Medium => base_fanout * 50,
    };

    log::info!("Generating sample data for Customer Order (expected: {})...", fanout);

    for i in 0..fanout {
        let mut entity = Q::customer_orders().comment("what: initialize a sample entity").purpose("why: populate the requested sample dataset").new_entity(context);
        let mut used_refs = std::collections::HashSet::new();

                if let Some(ref_id) = state.pick_unused_id("Platform", i as usize, &used_refs) {
                    entity.update_platform_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                entity.update_order_number(format!("{} {}", "TRACE-ORDER-001", i + 1));

                entity.update_description(format!("{} {}", "Draft order", i + 1));



        let entity = entity.audit_as("Init Sample Data").save(context).await.map_err(SampleDataError::from_display)?;

        state.record_generated(crate::CustomerOrder::ENTITY_NAME);

        if i % 20 == 0 {
            log::info!("Generating Customer Order: {}/{}", i, fanout);
        }

        state.add_reference(crate::CustomerOrder::ENTITY_NAME, entity.id().into_u64());
    }

    log::info!("Successfully generated sample records for Customer Order.");
    Ok(())
}


async fn generate_order_items<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized,
{
        if state.ids("Customer Order").is_empty() {
            state.record_skipped(crate::OrderItem::ENTITY_NAME, "Required dependency Customer Order is missing in reference pool".to_string());
            log::info!("Skipped generating Order Item: Required dependency Customer Order is missing in reference pool.");
            return Ok(());
        }


    let object_fields_count = 0 + 1;
    let base_fanout = std::cmp::max(1, object_fields_count) * 20;

    let fanout = match state.plan.scale {
        SampleDataScale::Tiny => base_fanout,
        SampleDataScale::Small => base_fanout * 5,
        SampleDataScale::Medium => base_fanout * 50,
    };

    log::info!("Generating sample data for Order Item (expected: {})...", fanout);

    for i in 0..fanout {
        let mut entity = Q::order_items().comment("what: initialize a sample entity").purpose("why: populate the requested sample dataset").new_entity(context);
        let mut used_refs = std::collections::HashSet::new();

                if let Some(ref_id) = state.pick_unused_id("Customer Order", i as usize, &used_refs) {
                    entity.update_customer_order_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                entity.update_name(format!("{} {}", "Available item", i + 1));



entity.audit_as("Init Sample Data").save(context).await.map_err(SampleDataError::from_display)?;

        state.record_generated(crate::OrderItem::ENTITY_NAME);

        if i % 20 == 0 {
            log::info!("Generating Order Item: {}/{}", i, fanout);
        }

    }

    log::info!("Successfully generated sample records for Order Item.");
    Ok(())
}


async fn generate_payments<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized,
{
        if state.ids("Customer Order").is_empty() {
            state.record_skipped(crate::Payment::ENTITY_NAME, "Required dependency Customer Order is missing in reference pool".to_string());
            log::info!("Skipped generating Payment: Required dependency Customer Order is missing in reference pool.");
            return Ok(());
        }


    let object_fields_count = 0 + 1;
    let base_fanout = std::cmp::max(1, object_fields_count) * 20;

    let fanout = match state.plan.scale {
        SampleDataScale::Tiny => base_fanout,
        SampleDataScale::Small => base_fanout * 5,
        SampleDataScale::Medium => base_fanout * 50,
    };

    log::info!("Generating sample data for Payment (expected: {})...", fanout);

    for i in 0..fanout {
        let mut entity = Q::payments().comment("what: initialize a sample entity").purpose("why: populate the requested sample dataset").new_entity(context);
        let mut used_refs = std::collections::HashSet::new();

                if let Some(ref_id) = state.pick_unused_id("Customer Order", i as usize, &used_refs) {
                    entity.update_customer_order_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                entity.update_reference_code(format!("{} {}", "TRACE-PAYMENT-001", i + 1));



        let entity = entity.audit_as("Init Sample Data").save(context).await.map_err(SampleDataError::from_display)?;

        state.record_generated(crate::Payment::ENTITY_NAME);

        if i % 20 == 0 {
            log::info!("Generating Payment: {}/{}", i, fanout);
        }

        state.add_reference(crate::Payment::ENTITY_NAME, entity.id().into_u64());
    }

    log::info!("Successfully generated sample records for Payment.");
    Ok(())
}


async fn generate_shipments<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized,
{
        if state.ids("Customer Order").is_empty() {
            state.record_skipped(crate::Shipment::ENTITY_NAME, "Required dependency Customer Order is missing in reference pool".to_string());
            log::info!("Skipped generating Shipment: Required dependency Customer Order is missing in reference pool.");
            return Ok(());
        }


    let object_fields_count = 0 + 1;
    let base_fanout = std::cmp::max(1, object_fields_count) * 20;

    let fanout = match state.plan.scale {
        SampleDataScale::Tiny => base_fanout,
        SampleDataScale::Small => base_fanout * 5,
        SampleDataScale::Medium => base_fanout * 50,
    };

    log::info!("Generating sample data for Shipment (expected: {})...", fanout);

    for i in 0..fanout {
        let mut entity = Q::shipments().comment("what: initialize a sample entity").purpose("why: populate the requested sample dataset").new_entity(context);
        let mut used_refs = std::collections::HashSet::new();

                if let Some(ref_id) = state.pick_unused_id("Customer Order", i as usize, &used_refs) {
                    entity.update_customer_order_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                entity.update_reference_code(format!("{} {}", "TRACE-SHIPMENT-001", i + 1));



entity.audit_as("Init Sample Data").save(context).await.map_err(SampleDataError::from_display)?;

        state.record_generated(crate::Shipment::ENTITY_NAME);

        if i % 20 == 0 {
            log::info!("Generating Shipment: {}/{}", i, fanout);
        }

    }

    log::info!("Successfully generated sample records for Shipment.");
    Ok(())
}


async fn generate_payment_attempts<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized,
{
        if state.ids("Payment").is_empty() {
            state.record_skipped(crate::PaymentAttempt::ENTITY_NAME, "Required dependency Payment is missing in reference pool".to_string());
            log::info!("Skipped generating Payment Attempt: Required dependency Payment is missing in reference pool.");
            return Ok(());
        }


    let object_fields_count = 0 + 1;
    let base_fanout = std::cmp::max(1, object_fields_count) * 20;

    let fanout = match state.plan.scale {
        SampleDataScale::Tiny => base_fanout,
        SampleDataScale::Small => base_fanout * 5,
        SampleDataScale::Medium => base_fanout * 50,
    };

    log::info!("Generating sample data for Payment Attempt (expected: {})...", fanout);

    for i in 0..fanout {
        let mut entity = Q::payment_attempts().comment("what: initialize a sample entity").purpose("why: populate the requested sample dataset").new_entity(context);
        let mut used_refs = std::collections::HashSet::new();

                if let Some(ref_id) = state.pick_unused_id("Payment", i as usize, &used_refs) {
                    entity.update_payment_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                entity.update_reference_code(format!("{} {}", "TRACE-ATTEMPT-001", i + 1));



entity.audit_as("Init Sample Data").save(context).await.map_err(SampleDataError::from_display)?;

        state.record_generated(crate::PaymentAttempt::ENTITY_NAME);

        if i % 20 == 0 {
            log::info!("Generating Payment Attempt: {}/{}", i, fanout);
        }

    }

    log::info!("Successfully generated sample records for Payment Attempt.");
    Ok(())
}
