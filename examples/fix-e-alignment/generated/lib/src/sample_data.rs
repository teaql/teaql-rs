
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

    load_root_tenants(context, &mut state).await?; //depth: 0


    generate_purchase_orders(context, &mut state).await?;

    generate_order_items(context, &mut state).await?;


    let report = state.into_report();
    log::info!("Sample data generation completed successfully. Generated: {} tables, Skipped: {} tables.", report.generated.len(), report.skipped.len());
    Ok(report)
}

async fn load_root_tenants<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized,
{
    let list = Q::tenants().comment("what: inspect existing entities before sample-data initialization").purpose("why: avoid duplicate sample records").execute_for_list(context).await.unwrap_or_default();
    for item in list {
        state.add_reference(crate::Tenant::ENTITY_NAME, item.id().into_u64());
    }
    Ok(())
}


async fn generate_purchase_orders<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized,
{
        if state.ids("tenant").is_empty() {
            state.record_skipped(crate::PurchaseOrder::ENTITY_NAME, "Required dependency tenant is missing in reference pool".to_string());
            log::info!("Skipped generating purchase_order: Required dependency tenant is missing in reference pool.");
            return Ok(());
        }


    let object_fields_count = 0 + 1;
    let base_fanout = std::cmp::max(1, object_fields_count) * 20;

    let fanout = match state.plan.scale {
        SampleDataScale::Tiny => base_fanout,
        SampleDataScale::Small => base_fanout * 5,
        SampleDataScale::Medium => base_fanout * 50,
    };

    log::info!("Generating sample data for purchase_order (expected: {})...", fanout);

    for i in 0..fanout {
        let mut entity = Q::purchase_orders().comment("what: initialize a sample entity").purpose("why: populate the requested sample dataset").new_entity(context).map_err(SampleDataError::from_display)?;
        let mut used_refs = std::collections::HashSet::<u64>::new();

                if let Some(ref_id) = state.pick_unused_id("tenant", i as usize, &used_refs) {
                    entity.update_tenant_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                entity.update_contact_phone(format!("{} {}", "Contact Phone", i + 1));

                entity.update_internal_note(format!("{} {}", "Optional note", i + 1));

                {
                    let days = ((i as u64 + state.plan.seed) % (365 * 3)) as i64;
                    let past = chrono::Utc::now().naive_utc() - chrono::Duration::try_days(days).unwrap_or_default();
                    entity.update_create_time(teaql_core::time::Timestamp(past.and_utc().timestamp_millis()));
                }

                {
                    let days = ((i as u64 + state.plan.seed) % (365 * 3)) as i64;
                    let past = chrono::Utc::now().naive_utc() - chrono::Duration::try_days(days).unwrap_or_default();
                    entity.update_update_time(teaql_core::time::Timestamp(past.and_utc().timestamp_millis()));
                }



        let entity = entity.audit_as("Init Sample Data").save(context).await.map_err(SampleDataError::from_display)?;

        state.record_generated(crate::PurchaseOrder::ENTITY_NAME);

        if i % 20 == 0 {
            log::info!("Generating purchase_order: {}/{}", i, fanout);
        }

        state.add_reference(crate::PurchaseOrder::ENTITY_NAME, entity.id().into_u64());
    }

    log::info!("Successfully generated sample records for purchase_order.");
    Ok(())
}


async fn generate_order_items<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized,
{
        if state.ids("purchase_order").is_empty() {
            state.record_skipped(crate::OrderItem::ENTITY_NAME, "Required dependency purchase_order is missing in reference pool".to_string());
            log::info!("Skipped generating order_item: Required dependency purchase_order is missing in reference pool.");
            return Ok(());
        }

        if state.ids("tenant").is_empty() {
            state.record_skipped(crate::OrderItem::ENTITY_NAME, "Required dependency tenant is missing in reference pool".to_string());
            log::info!("Skipped generating order_item: Required dependency tenant is missing in reference pool.");
            return Ok(());
        }


    let object_fields_count = 0 + 1 + 1;
    let base_fanout = std::cmp::max(1, object_fields_count) * 20;

    let fanout = match state.plan.scale {
        SampleDataScale::Tiny => base_fanout,
        SampleDataScale::Small => base_fanout * 5,
        SampleDataScale::Medium => base_fanout * 50,
    };

    log::info!("Generating sample data for order_item (expected: {})...", fanout);

    for i in 0..fanout {
        let mut entity = Q::order_items().comment("what: initialize a sample entity").purpose("why: populate the requested sample dataset").new_entity(context).map_err(SampleDataError::from_display)?;
        let mut used_refs = std::collections::HashSet::<u64>::new();

                if let Some(ref_id) = state.pick_unused_id("purchase_order", i as usize, &used_refs) {
                    entity.update_purchase_order_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                if let Some(ref_id) = state.pick_unused_id("tenant", i as usize, &used_refs) {
                    entity.update_tenant_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                entity.update_product_name(format!("{} {}", "Product Name", i + 1));

                {
                    let max_val: u64 = "10.00".parse().unwrap_or(1000);
                    let rand_val = (i as u64 + state.plan.seed) % max_val.max(1) + 1;
                    entity.update_unit_price(rand_val as i64);
                }

                {
                    let days = ((i as u64 + state.plan.seed) % (365 * 3)) as i64;
                    let past = chrono::Utc::now().naive_utc() - chrono::Duration::try_days(days).unwrap_or_default();
                    entity.update_create_time(teaql_core::time::Timestamp(past.and_utc().timestamp_millis()));
                }

                {
                    let days = ((i as u64 + state.plan.seed) % (365 * 3)) as i64;
                    let past = chrono::Utc::now().naive_utc() - chrono::Duration::try_days(days).unwrap_or_default();
                    entity.update_update_time(teaql_core::time::Timestamp(past.and_utc().timestamp_millis()));
                }



entity.audit_as("Init Sample Data").save(context).await.map_err(SampleDataError::from_display)?;

        state.record_generated(crate::OrderItem::ENTITY_NAME);

        if i % 20 == 0 {
            log::info!("Generating order_item: {}/{}", i, fanout);
        }

    }

    log::info!("Successfully generated sample records for order_item.");
    Ok(())
}
