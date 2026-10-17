use aws_sdk_s3::{
    Client,
    config::{BehaviorVersion, Credentials, Region},
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use teaql_object_storage_s3::S3ObjectStorage;
use teaql_runtime::{UserContext, object_storage::*};

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("Required test setting: {name}"))
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(required("TEAQL_S3_ENDPOINT"))
        .credentials_provider(Credentials::new(
            required("TEAQL_S3_ACCESS_KEY"),
            required("TEAQL_S3_SECRET_KEY"),
            None,
            None,
            "local-test",
        ))
        .force_path_style(true)
        .build();
    let client = Client::from_conf(config);
    let bucket = required("TEAQL_S3_BUCKET");
    // Dedicated test fixture setup; the runtime provider does not administer buckets.
    if let Err(error) = client.head_bucket().bucket(&bucket).send().await {
        if error
            .raw_response()
            .is_none_or(|response| response.status().as_u16() != 404)
        {
            return Err("test bucket access failed".into());
        }
        client
            .create_bucket()
            .bucket(&bucket)
            .send()
            .await
            .map_err(|_| "test bucket creation failed")?;
    }
    let provider = Arc::new(S3ObjectStorage::new(client.clone(), &bucket)?);
    let context = UserContext::new().with_object_storage(provider.clone());
    let storage = context.object_storage("verify binary object storage")?;
    let run_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_nanos();
    let key = ObjectKey::new(format!("rust/{run_id}/二进制 space+%.bin"))?;
    let empty = ObjectKey::new(format!("rust/{run_id}/empty"))?;
    let race = ObjectKey::new(format!("rust/{run_id}/race"))?;
    let bytes = Bytes::from((0..256 * 1024).map(|i| (i % 251) as u8).collect::<Vec<_>>());
    let options = PutOptions::new(bytes.len() as u64, WriteMode::CreateOnly)?
        .with_metadata(BTreeMap::from([("fixture".into(), "binary".into())]))?;
    assert!(!storage.exists(&key).await?);
    assert!(matches!(
        storage.get(&key).await,
        Err(StorageError(StorageErrorCode::NotFound))
    ));
    storage.put_bytes(&key, bytes.clone(), &options).await?;
    assert_eq!(
        storage.head(&key).await?.unwrap().custom_metadata["fixture"],
        "binary"
    );
    assert_eq!(storage.get_bytes(&key, bytes.len()).await?, bytes);
    assert_eq!(
        storage
            .put_bytes(
                &key,
                Bytes::new(),
                &PutOptions::new(0, WriteMode::CreateOnly)?
            )
            .await
            .err()
            .unwrap()
            .0,
        StorageErrorCode::Conflict
    );
    assert_eq!(
        storage
            .get_bytes(&key, bytes.len() - 1)
            .await
            .unwrap_err()
            .0,
        StorageErrorCode::TooLarge
    );
    let url = storage
        .get_read_url(
            &key,
            &ReadUrlOptions::default()
                .with_disposition(Disposition::Attachment, Some("sample.bin"))?,
        )
        .await?;
    assert!(!format!("{url:?}").contains("X-Amz"));
    let http = reqwest::Client::new();
    let downloaded = http
        .get(url.as_str())
        .send()
        .await
        .map_err(|_| "signed GET failed")?;
    assert_eq!(downloaded.status(), 200);
    assert!(
        downloaded.headers()["content-disposition"]
            .to_str()?
            .contains("sample.bin")
    );
    assert_eq!(downloaded.bytes().await?, bytes);
    let expiring = storage
        .get_read_url(&key, &ReadUrlOptions::new(Duration::from_secs(1))?)
        .await?;
    tokio::time::sleep(Duration::from_millis(2200)).await;
    assert_eq!(
        http.get(expiring.as_str())
            .send()
            .await
            .map_err(|_| "expired GET failed")?
            .status(),
        403
    );
    storage
        .put_bytes(
            &empty,
            Bytes::new(),
            &PutOptions::new(0, WriteMode::CreateOnly)?,
        )
        .await?;
    assert!(storage.get_bytes(&empty, 0).await?.is_empty());
    assert_eq!(
        storage
            .put_bytes(
                &empty,
                Bytes::from_static(b"x"),
                &PutOptions::new(0, WriteMode::CreateOnly)?
            )
            .await
            .err()
            .unwrap()
            .0,
        StorageErrorCode::InvalidInput
    );
    storage
        .put_bytes(
            &key,
            Bytes::from_static(b"replace"),
            &PutOptions::new(7, WriteMode::Replace)?,
        )
        .await?;
    assert_eq!(storage.get_bytes(&key, 7).await?, b"replace");
    let context2 = UserContext::new().with_object_storage(provider);
    let storage2 = context2.object_storage("verify atomic create")?;
    let race_options = PutOptions::new(1, WriteMode::CreateOnly)?;
    let (a, b) = tokio::join!(
        storage.put_bytes(&race, Bytes::from_static(b"a"), &race_options),
        storage2.put_bytes(&race, Bytes::from_static(b"b"), &race_options)
    );
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert_eq!(a.err().or(b.err()).unwrap().0, StorageErrorCode::Conflict);
    let wrong = UserContext::new().with_object_storage(Arc::new(S3ObjectStorage::new(
        client,
        format!("missing-{run_id}"),
    )?));
    assert_eq!(
        wrong
            .object_storage("verify missing bucket")?
            .exists(&key)
            .await
            .unwrap_err()
            .0,
        StorageErrorCode::Unavailable
    );
    storage.delete(&key).await?;
    storage.delete(&key).await?;
    storage.delete(&empty).await?;
    storage.delete(&race).await?;
    assert!(!storage.exists(&key).await?);
    println!(
        "PASS Rust S3: binary, empty, metadata, URL download/expiry, atomic create, replace, bounded reads, missing bucket, repeated delete"
    );
    Ok(())
}
