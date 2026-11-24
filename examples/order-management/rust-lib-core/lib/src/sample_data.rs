
use std::collections::BTreeMap;
use crate::TeaqlRuntime;
use crate::Q;
use teaql_core::Entity as _;
use crate::request_support::TeaqlUserContextExt as _;
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
    C: TeaqlRuntime + ?Sized + crate::TeaqlRepositoryProvider,
{
    log::info!("Starting sample data generation. Scale: {:?}, Seed: {}", plan.scale, plan.seed);
    let mut state = SampleDataState::new(plan);

    load_root_commerce_platforms(context, &mut state).await?; //depth: 0

    load_constant_order_statuses(context, &mut state).await?;

    context.user_context().transaction_data(|| async {
        Box::pin(generate_customers(context, &mut state)).await.map_err(|e| {
            teaql_runtime::DataServiceError::Runtime(teaql_runtime::RuntimeError::Graph(e.to_string()))
        })
    }).await.map_err(SampleDataError::from_display)?;

    context.user_context().transaction_data(|| async {
        Box::pin(generate_order_search_presets(context, &mut state)).await.map_err(|e| {
            teaql_runtime::DataServiceError::Runtime(teaql_runtime::RuntimeError::Graph(e.to_string()))
        })
    }).await.map_err(SampleDataError::from_display)?;

    context.user_context().transaction_data(|| async {
        Box::pin(generate_products(context, &mut state)).await.map_err(|e| {
            teaql_runtime::DataServiceError::Runtime(teaql_runtime::RuntimeError::Graph(e.to_string()))
        })
    }).await.map_err(SampleDataError::from_display)?;

    context.user_context().transaction_data(|| async {
        Box::pin(generate_customer_orders(context, &mut state)).await.map_err(|e| {
            teaql_runtime::DataServiceError::Runtime(teaql_runtime::RuntimeError::Graph(e.to_string()))
        })
    }).await.map_err(SampleDataError::from_display)?;

    context.user_context().transaction_data(|| async {
        Box::pin(generate_order_lines(context, &mut state)).await.map_err(|e| {
            teaql_runtime::DataServiceError::Runtime(teaql_runtime::RuntimeError::Graph(e.to_string()))
        })
    }).await.map_err(SampleDataError::from_display)?;


    let report = state.into_report();
    log::info!("Sample data generation completed successfully. Generated: {} tables, Skipped: {} tables.", report.generated.len(), report.skipped.len());
    Ok(report)
}

async fn load_root_commerce_platforms<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized + crate::TeaqlRepositoryProvider,
{
    let list = Q::commerce_platforms().comment("what: inspect existing entities before sample-data initialization").purpose("why: avoid duplicate sample records").execute_for_list(context).await.unwrap_or_default();
    for item in list {
        state.add_reference(crate::CommercePlatform::ENTITY_NAME, item.id().into_u64());
    }
    Ok(())
}

async fn load_constant_order_statuses<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized + crate::TeaqlRepositoryProvider,
{
    let list = Q::order_statuses().comment("what: inspect existing entities before sample-data initialization").purpose("why: avoid duplicate sample records").execute_for_list(context).await.unwrap_or_default();
    for item in list {
        state.add_reference(crate::OrderStatus::ENTITY_NAME, item.id().into_u64());
    }
    Ok(())
}

async fn generate_customers<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized + crate::TeaqlRepositoryProvider,
{
        if state.ids("commerce_platform").is_empty() {
            state.record_skipped(crate::Customer::ENTITY_NAME, "Required dependency commerce_platform is missing in reference pool".to_string());
            log::info!("Skipped generating customer: Required dependency commerce_platform is missing in reference pool.");
            return Ok(());
        }


    let object_fields_count = 0 + 1;
    let base_fanout = std::cmp::max(1, object_fields_count) * 20;

    let fanout = match state.plan.scale {
        SampleDataScale::Tiny => base_fanout,
        SampleDataScale::Small => base_fanout * 5,
        SampleDataScale::Medium => base_fanout * 50,
    };

    log::info!("Generating sample data for customer (expected: {})...", fanout);

    for i in 0..fanout {
        let mut entity = Q::customers().comment("what: initialize a sample entity").purpose("why: populate the requested sample dataset").new_entity(context);
        let mut used_refs = std::collections::HashSet::new();

                if let Some(ref_id) = state.pick_unused_id("commerce_platform", i as usize, &used_refs) {
                    entity.update_commerce_platform_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                entity.update_name(format!("{} {}", "Acme Retail", i + 1));

                entity.update_email(format!("{} {}", "customer@example.com", i + 1));

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

        state.record_generated(crate::Customer::ENTITY_NAME);

        if i % 20 == 0 {
            log::info!("Generating customer: {}/{}", i, fanout);
        }

        state.add_reference(crate::Customer::ENTITY_NAME, entity.id().into_u64());
    }

    log::info!("Successfully generated sample records for customer.");
    Ok(())
}


async fn generate_order_search_presets<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized + crate::TeaqlRepositoryProvider,
{
        if state.ids("commerce_platform").is_empty() {
            state.record_skipped(crate::OrderSearchPreset::ENTITY_NAME, "Required dependency commerce_platform is missing in reference pool".to_string());
            log::info!("Skipped generating order_search_preset: Required dependency commerce_platform is missing in reference pool.");
            return Ok(());
        }


    let object_fields_count = 0 + 1;
    let base_fanout = std::cmp::max(1, object_fields_count) * 20;

    let fanout = match state.plan.scale {
        SampleDataScale::Tiny => base_fanout,
        SampleDataScale::Small => base_fanout * 5,
        SampleDataScale::Medium => base_fanout * 50,
    };

    log::info!("Generating sample data for order_search_preset (expected: {})...", fanout);

    for i in 0..fanout {
        let mut entity = Q::order_search_presets().comment("what: initialize a sample entity").purpose("why: populate the requested sample dataset").new_entity(context);
        let mut used_refs = std::collections::HashSet::new();

                if let Some(ref_id) = state.pick_unused_id("commerce_platform", i as usize, &used_refs) {
                    entity.update_commerce_platform_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                entity.update_name(format!("{} {}", "Pending web orders", i + 1));

                entity.update_filter_json(format!("{} {}", "{}", i + 1));

                entity.update_request_id(format!("{} {}", "quick-start-pending-orders", i + 1));

                entity.update_owner_user_id(format!("{} {}", "operator-1", i + 1));

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

        state.record_generated(crate::OrderSearchPreset::ENTITY_NAME);

        if i % 20 == 0 {
            log::info!("Generating order_search_preset: {}/{}", i, fanout);
        }

    }

    log::info!("Successfully generated sample records for order_search_preset.");
    Ok(())
}


async fn generate_products<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized + crate::TeaqlRepositoryProvider,
{
        if state.ids("commerce_platform").is_empty() {
            state.record_skipped(crate::Product::ENTITY_NAME, "Required dependency commerce_platform is missing in reference pool".to_string());
            log::info!("Skipped generating product: Required dependency commerce_platform is missing in reference pool.");
            return Ok(());
        }


    let object_fields_count = 0 + 1;
    let base_fanout = std::cmp::max(1, object_fields_count) * 20;

    let fanout = match state.plan.scale {
        SampleDataScale::Tiny => base_fanout,
        SampleDataScale::Small => base_fanout * 5,
        SampleDataScale::Medium => base_fanout * 50,
    };

    log::info!("Generating sample data for product (expected: {})...", fanout);

    for i in 0..fanout {
        let mut entity = Q::products().comment("what: initialize a sample entity").purpose("why: populate the requested sample dataset").new_entity(context);
        let mut used_refs = std::collections::HashSet::new();

                if let Some(ref_id) = state.pick_unused_id("commerce_platform", i as usize, &used_refs) {
                    entity.update_commerce_platform_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                entity.update_name(format!("{} {}", "Tea", i + 1));

                entity.update_sku(format!("{} {}", "TEA-001", i + 1));

                entity.update_image_url(format!("{} {}", "https://example.com/tea.png", i + 1));

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

        state.record_generated(crate::Product::ENTITY_NAME);

        if i % 20 == 0 {
            log::info!("Generating product: {}/{}", i, fanout);
        }

        state.add_reference(crate::Product::ENTITY_NAME, entity.id().into_u64());
    }

    log::info!("Successfully generated sample records for product.");
    Ok(())
}


async fn generate_customer_orders<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized + crate::TeaqlRepositoryProvider,
{
        if state.ids("order_status").is_empty() {
            state.record_skipped(crate::CustomerOrder::ENTITY_NAME, "Required dependency order_status is missing in reference pool".to_string());
            log::info!("Skipped generating customer_order: Required dependency order_status is missing in reference pool.");
            return Ok(());
        }

        if state.ids("customer").is_empty() {
            state.record_skipped(crate::CustomerOrder::ENTITY_NAME, "Required dependency customer is missing in reference pool".to_string());
            log::info!("Skipped generating customer_order: Required dependency customer is missing in reference pool.");
            return Ok(());
        }

        if state.ids("commerce_platform").is_empty() {
            state.record_skipped(crate::CustomerOrder::ENTITY_NAME, "Required dependency commerce_platform is missing in reference pool".to_string());
            log::info!("Skipped generating customer_order: Required dependency commerce_platform is missing in reference pool.");
            return Ok(());
        }


    let object_fields_count = 0 + 1 + 1 + 1;
    let base_fanout = std::cmp::max(1, object_fields_count) * 20;

    let fanout = match state.plan.scale {
        SampleDataScale::Tiny => base_fanout,
        SampleDataScale::Small => base_fanout * 5,
        SampleDataScale::Medium => base_fanout * 50,
    };

    log::info!("Generating sample data for customer_order (expected: {})...", fanout);

    for i in 0..fanout {
        let mut entity = Q::customer_orders().comment("what: initialize a sample entity").purpose("why: populate the requested sample dataset").new_entity(context);
        let mut used_refs = std::collections::HashSet::new();

                if let Some(ref_id) = state.pick_unused_id("order_status", i as usize, &used_refs) {
                    entity.update_status_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                if let Some(ref_id) = state.pick_unused_id("customer", i as usize, &used_refs) {
                    entity.update_customer_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                if let Some(ref_id) = state.pick_unused_id("commerce_platform", i as usize, &used_refs) {
                    entity.update_commerce_platform_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                entity.update_order_number(format!("{} {}", "WEB-2026-001", i + 1));

                {
                    let days = ((i as u64 + state.plan.seed) % (365 * 3)) as i64;
                    let past = chrono::Utc::now().naive_utc() - chrono::Duration::try_days(days).unwrap_or_default();
                    entity.update_order_date(past.date());
                }

                {
                    let max_val: u64 = "129.95".parse().unwrap_or(1000);
                    let rand_val = (i as u64 + state.plan.seed) % max_val.max(1) + 1;
                    entity.update_total_amount(rand_val as i64);
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



        let entity = entity.audit_as("Init Sample Data").save(context).await.map_err(SampleDataError::from_display)?;

        state.record_generated(crate::CustomerOrder::ENTITY_NAME);

        if i % 20 == 0 {
            log::info!("Generating customer_order: {}/{}", i, fanout);
        }

        state.add_reference(crate::CustomerOrder::ENTITY_NAME, entity.id().into_u64());
    }

    log::info!("Successfully generated sample records for customer_order.");
    Ok(())
}


async fn generate_order_lines<C>(
    context: &C,
    state: &mut SampleDataState,
) -> Result<(), SampleDataError>
where
    C: TeaqlRuntime + ?Sized + crate::TeaqlRepositoryProvider,
{
        if state.ids("customer_order").is_empty() {
            state.record_skipped(crate::OrderLine::ENTITY_NAME, "Required dependency customer_order is missing in reference pool".to_string());
            log::info!("Skipped generating order_line: Required dependency customer_order is missing in reference pool.");
            return Ok(());
        }

        if state.ids("product").is_empty() {
            state.record_skipped(crate::OrderLine::ENTITY_NAME, "Required dependency product is missing in reference pool".to_string());
            log::info!("Skipped generating order_line: Required dependency product is missing in reference pool.");
            return Ok(());
        }

        if state.ids("commerce_platform").is_empty() {
            state.record_skipped(crate::OrderLine::ENTITY_NAME, "Required dependency commerce_platform is missing in reference pool".to_string());
            log::info!("Skipped generating order_line: Required dependency commerce_platform is missing in reference pool.");
            return Ok(());
        }


    let object_fields_count = 0 + 1 + 1 + 1;
    let base_fanout = std::cmp::max(1, object_fields_count) * 20;

    let fanout = match state.plan.scale {
        SampleDataScale::Tiny => base_fanout,
        SampleDataScale::Small => base_fanout * 5,
        SampleDataScale::Medium => base_fanout * 50,
    };

    log::info!("Generating sample data for order_line (expected: {})...", fanout);

    for i in 0..fanout {
        let mut entity = Q::order_lines().comment("what: initialize a sample entity").purpose("why: populate the requested sample dataset").new_entity(context);
        let mut used_refs = std::collections::HashSet::new();

                if let Some(ref_id) = state.pick_unused_id("customer_order", i as usize, &used_refs) {
                    entity.update_customer_order_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                if let Some(ref_id) = state.pick_unused_id("product", i as usize, &used_refs) {
                    entity.update_product_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                if let Some(ref_id) = state.pick_unused_id("commerce_platform", i as usize, &used_refs) {
                    entity.update_commerce_platform_id(ref_id);
                    used_refs.insert(ref_id);
                } else {
                    // Optional relation was missing in reference pool
                }
                entity.update_product_name(format!("{} {}", "Tea", i + 1));

                entity.update_sku(format!("{} {}", "TEA-001", i + 1));

                {
                    let max_val: u64 = "1".parse().unwrap_or(1000);
                    let rand_val = (i as u64 + state.plan.seed) % max_val.max(1) + 1;
                    entity.update_quantity(rand_val as i64);
                }

                {
                    let days = ((i as u64 + state.plan.seed) % (365 * 3)) as i64;
                    let past = chrono::Utc::now().naive_utc() - chrono::Duration::try_days(days).unwrap_or_default();
                    entity.update_create_time(teaql_core::time::Timestamp(past.and_utc().timestamp_millis()));
                }



entity.audit_as("Init Sample Data").save(context).await.map_err(SampleDataError::from_display)?;

        state.record_generated(crate::OrderLine::ENTITY_NAME);

        if i % 20 == 0 {
            log::info!("Generating order_line: {}/{}", i, fanout);
        }

    }

    log::info!("Successfully generated sample records for order_line.");
    Ok(())
}
