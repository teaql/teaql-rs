use aws_sdk_s3::{
    config::{BehaviorVersion, Credentials, Region},
    Client,
};
use bytes::Bytes;
use order_attachment_service_core::{
    request_support::AuditedSave as _, service_runtime, teaql_core::Entity as _, OrderAttachment,
    ServiceRuntimeConfig, E, Q,
};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};
use teaql_object_storage_s3::S3ObjectStorage;
use teaql_runtime::{
    object_storage::{
        ObjectKey, ObjectStorageObserver, PutOptions, StorageError, StorageErrorCode, StorageEvent,
        WriteMode,
    },
    RawAuditEventKind, RuntimeError, SafeAuditEvent, SafeAuditEventSink, UserContext,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const RECEIPT: &[u8] = b"TeaQL order receipt fixture\nItem: tea\nTotal: 12.50\n";
const FILENAME: &str = "receipt.txt";
const MEDIA_TYPE: &str = "text/plain";

#[derive(Clone, Default)]
struct Audits(Arc<Mutex<Vec<SafeAuditEvent>>>);
impl SafeAuditEventSink for Audits {
    fn on_safe_event(
        &self,
        _: &UserContext,
        event: &SafeAuditEvent,
    ) -> std::result::Result<(), RuntimeError> {
        if matches!(event.entity.as_str(), "CustomerOrder" | "OrderAttachment")
            && event.kind == RawAuditEventKind::Created
        {
            self.0.lock().unwrap().push(event.clone());
        }
        Ok(())
    }
}

#[derive(Default)]
struct Observations(Mutex<Vec<StorageEvent>>);
impl ObjectStorageObserver for Observations {
    fn record(&self, event: StorageEvent) {
        self.0.lock().unwrap().push(event);
    }
}

fn require(ok: bool, message: &'static str) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(message.into())
    }
}

fn checksum(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

async fn attachments(context: &UserContext, order_id: u64) -> Result<Vec<OrderAttachment>> {
    let rows = Q::order_attachments()
        .with_customer_order_matching(Q::customer_orders_minimal().with_id_is(order_id))
        .limit(2)
        .comment("load order attachments")
        .purpose("retrieve this order's stored receipt")
        .execute_for_list(context)
        .await?;
    Ok(rows.into_iter().collect())
}

async fn verify(database: &str, download: &str, round: u8) -> Result<()> {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(std::env::var("TEAQL_S3_ENDPOINT")?)
        .credentials_provider(Credentials::new(
            std::env::var("TEAQL_S3_ACCESS_KEY")?,
            std::env::var("TEAQL_S3_SECRET_KEY")?,
            None,
            None,
            "local-test",
        ))
        .force_path_style(true)
        .build();
    let provider = Arc::new(S3ObjectStorage::new(
        Client::from_conf(config),
        &std::env::var("TEAQL_S3_BUCKET")?,
    )?);
    let audit = Audits::default();
    let observations = Arc::new(Observations::default());
    let trace = format!("order-attachment-{round}");
    let mut context = service_runtime(ServiceRuntimeConfig {
        database_url: database.into(),
    })
    .await?
    .with_custom_event_sink(audit.clone())
    .with_trace_id(trace.clone())
    .with_object_storage(provider)
    .with_object_storage_observer(observations.clone());
    context.disable_sql_log();
    context.ensure_schema().await?;
    let platform = Q::commerce_platforms_minimal()
        .with_id_is(1_u64)
        .limit(1)
        .comment("load the commerce platform")
        .purpose("associate the new order to its root")
        .execute_for_one(&context)
        .await?
        .ok_or("generated platform missing")?;

    let order_number = format!("ATTACH-ORDER-{round}");
    let mut order = Q::customer_orders()
        .comment("prepare receipt order")
        .purpose("file a receipt against this order")
        .new_entity(&context)?;
    order
        .update_order_number(order_number.as_str())
        .update_commerce_platform_id(platform.id());
    let order = order
        .audit_as("place the order receiving the receipt")
        .save(&context)
        .await?;
    let order_id = order.id();

    let storage = context.object_storage("file and retrieve an order receipt")?;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let key = ObjectKey::new(format!(
        "private-order-attachments/{}-{nonce}",
        std::process::id()
    ))?;
    let digest = checksum(RECEIPT);
    assert!(
        !storage.exists(&key).await?,
        "new attachment key already exists"
    );
    let options = PutOptions::new(RECEIPT.len() as u64, WriteMode::CreateOnly)?
        .with_content_type(MEDIA_TYPE)?
        .with_metadata(BTreeMap::from([("receipt-sha256".into(), digest.clone())]))?;
    storage
        .put_bytes(&key, Bytes::from_static(RECEIPT), &options)
        .await?;
    let mut uploaded = true;
    let result: Result<_> = async {
        let mut attachment = Q::order_attachments().comment("prepare receipt attachment")
            .purpose("save the uploaded receipt reference on its order").new_entity(&context)?;
        attachment.update_customer_order_id(order_id).update_file_name(FILENAME).update_content_type(MEDIA_TYPE)
            .update_content_length(RECEIPT.len() as i64).update_content_sha256(digest.as_str()).update_object_key(key.as_str());
        let attachment = attachment.audit_as("associate the stored receipt with its order").save(&context).await?;
        let attachment_id = attachment.id();
        let rows = attachments(&context, order_id).await?;
        require(rows.len() == 1 && rows[0].id() == attachment_id, "persisted order association mismatch")?;
        let expression = E::order_attachment(&rows[0]);
        require(expression.get_customer_order_id().eval() == Some(order_id)
            && expression.get_file_name().eval().as_deref() == Some(FILENAME)
            && expression.get_content_type().eval().as_deref() == Some(MEDIA_TYPE),
            "stored receipt owner, filename or type mismatch")?;
        require(expression.get_content_length().eval() == Some(RECEIPT.len() as i64)
            && expression.get_content_sha256().eval().as_deref() == Some(digest.as_str()),
            "stored receipt bytes metadata mismatch")?;
        let value = expression.get_object_key().eval().ok_or("stored receipt reference missing")?;
        require(value == key.as_str(), "stored receipt reference mismatch")?;
        let persisted_key = ObjectKey::new(value)?;
        let metadata = storage.head(&persisted_key).await?.ok_or("object metadata missing")?;
        require(metadata.size == RECEIPT.len() as u64 && metadata.content_type.as_deref() == Some(MEDIA_TYPE)
            && metadata.custom_metadata.get("receipt-sha256") == Some(&digest), "provider metadata mismatch")?;
        let replacement = Bytes::from_static(b"replacement");
        let rejected = storage.put_bytes(&persisted_key, replacement.clone(),
            &PutOptions::new(replacement.len() as u64, WriteMode::CreateOnly)?).await;
        require(matches!(rejected, Err(StorageError(StorageErrorCode::Conflict))), "duplicate upload was not refused")?;
        require(matches!(storage.get_bytes(&persisted_key, RECEIPT.len()-1).await, Err(StorageError(StorageErrorCode::TooLarge))), "undersized read was not refused")?;
        let downloaded = storage.get_bytes(&persisted_key, RECEIPT.len()).await?;
        require(downloaded.as_slice() == RECEIPT && checksum(&downloaded) == digest, "retrieved receipt changed")?;
        std::fs::write(download, &downloaded)?;

        storage.delete(&persisted_key).await?;
        uploaded = false;
        require(matches!(storage.get(&persisted_key).await, Err(StorageError(StorageErrorCode::NotFound)))
            && !storage.exists(&persisted_key).await?, "missing object reported present")?;
        let persisted = attachments(&context, order_id).await?;
        require(persisted.len() == 1 && persisted[0].id() == attachment_id, "missing object changed the order association")?;
        let events = observations.0.lock().unwrap();
        let saved = audit.0.lock().unwrap();
        require(!events.is_empty() && events.iter().all(|event| event.trace_id == trace), "storage observation escaped the attachment Context")?;
        require(saved.iter().map(|event| event.entity.as_str()).eq(["CustomerOrder", "OrderAttachment"]), "committed order and attachment audits missing")?;
        require(!format!("{events:?} {saved:?}").contains(key.as_str()), "private object key reached safe events")?;
        Ok(json!({
            "round": round, "order": order_id, "attachment": attachment_id, "orderNumber": order_number,
            "filename": FILENAME, "contentType": MEDIA_TYPE, "length": downloaded.len(), "sha256": digest,
            "conflict": true, "sizeRefused": true, "missing": true, "businessAudits": saved.len(),
        }))
    }.await;
    if uploaded {
        storage.delete(&key).await?;
    }
    println!("ORDER_ATTACHMENT {}", result?);
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().collect();
    if args.len() != 4 {
        return Err("provide database, downloaded receipt path and round".into());
    }
    verify(&args[1], &args[2], args[3].parse()?).await
}
