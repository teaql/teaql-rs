use futures_util::stream;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::{Cursor, Read, Seek, SeekFrom, Write},
    os::unix::fs::{PermissionsExt, symlink},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::Duration,
};
use teaql_blob_content_native::*;
use teaql_blob_reference::*;
use tempfile::TempDir;
fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}
fn fixture(extension: &str) -> Vec<u8> {
    fs::read(fixtures().join(format!("files/sample.{extension}"))).unwrap()
}
fn content(data: Vec<u8>) -> BlobContentStream {
    Box::pin(stream::iter(
        data.chunks(37)
            .map(|part| Ok(Bytes::copy_from_slice(part)))
            .collect::<Vec<_>>(),
    ))
}
fn temp() -> TempDir {
    tempfile::Builder::new()
        .prefix("teaql-r5-native-")
        .tempdir()
        .unwrap()
}
async fn verifier(limits: VerificationLimits) -> (TempDir, NativeBlobContentVerifier) {
    let temp = temp();
    let verifier = NativeBlobContentVerifier::new(temp.path().join("scratch"), limits).unwrap();
    verifier.initialize().await.unwrap();
    (temp, verifier)
}
#[track_caller]
fn code<T>(result: BlobResult<T>, expected: BlobErrorCode) {
    match result {
        Err(error) => assert_eq!(error.code, expected),
        Ok(_) => panic!("unexpected acceptance"),
    }
}
fn repack(changes: BTreeMap<String, Vec<u8>>, extras: Vec<(String, Vec<u8>)>) -> Vec<u8> {
    use zip::write::SimpleFileOptions;
    let bytes = fixture("docx");
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).unwrap();
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).unwrap();
        let name = entry.name().to_owned();
        let data = if let Some(data) = changes.get(&name) {
            data.clone()
        } else {
            let mut data = Vec::new();
            entry.read_to_end(&mut data).unwrap();
            data
        };
        writer
            .start_file(
                name,
                SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated),
            )
            .unwrap();
        writer.write_all(&data).unwrap();
    }
    for (name, data) in extras {
        writer
            .start_file(
                name,
                SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated),
            )
            .unwrap();
        writer.write_all(&data).unwrap();
    }
    writer.finish().unwrap().into_inner()
}
#[tokio::test]
async fn all_ten_actual_formats_have_exact_bytes_hash_and_real_structure() {
    let (_temp, verifier) = verifier(VerificationLimits::default()).await;
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(fixtures().join("files.json")).unwrap()).unwrap();
    let files = manifest["files"].as_array().unwrap();
    assert_eq!(files.len(), 10);
    for item in files {
        let bytes = fs::read(fixtures().join(item["path"].as_str().unwrap())).unwrap();
        let expected = format!("{:x}", Sha256::digest(&bytes));
        let result = verifier
            .verify(
                content(bytes),
                item["extension"].as_str().unwrap(),
                item["sizeBytes"].as_u64().unwrap(),
                false,
            )
            .await
            .unwrap();
        assert_eq!(result.size_bytes(), item["sizeBytes"].as_u64().unwrap());
        assert_eq!(result.content_type(), item["contentType"].as_str().unwrap());
        assert_eq!(result.sha256(), item["sha256"].as_str().unwrap());
        assert_eq!(result.sha256(), expected);
    }
}
#[tokio::test]
async fn all_seven_shared_upload_byte_verification_cases() {
    let (_temp, verifier) = verifier(VerificationLimits::default()).await;
    let cases: serde_json::Value =
        serde_json::from_slice(&fs::read(fixtures().join("upload-cases.json")).unwrap()).unwrap();
    let cases = cases["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 7);
    for case in cases {
        let bytes = fs::read(fixtures().join(case["path"].as_str().unwrap())).unwrap();
        let extension = case["fileName"]
            .as_str()
            .unwrap()
            .rsplit('.')
            .next()
            .unwrap();
        // Declared MIME is intentionally absent from the production verifier API.
        let result = verifier
            .verify(
                content(bytes),
                extension,
                case["maxSizeBytes"].as_u64().unwrap(),
                false,
            )
            .await;
        if case.get("expected").is_some() {
            assert!(result.is_ok(), "{}", case["id"]);
        } else {
            assert_eq!(
                result.unwrap_err().code.code(),
                case["error"].as_str().unwrap(),
                "{}",
                case["id"]
            );
        }
    }
}
#[tokio::test]
async fn fabricated_headers_truncation_and_cross_format_substitution_reject() {
    let (_temp, verifier) = verifier(VerificationLimits::default()).await;
    for extension in [
        "jpg", "png", "mp4", "mp3", "doc", "docx", "xls", "xlsx", "pdf",
    ] {
        let mut header = fixture(extension);
        header.truncate(16);
        code(
            verifier
                .verify(content(header), extension, 100000, false)
                .await,
            BlobErrorCode::FormatNotAllowed,
        );
    }
    for (actual, claimed) in [
        ("jpg", "png"),
        ("pdf", "doc"),
        ("doc", "xls"),
        ("xls", "doc"),
        ("docx", "xlsx"),
        ("xlsx", "docx"),
        ("mp3", "mp4"),
        ("mp4", "mp3"),
    ] {
        code(
            verifier
                .verify(content(fixture(actual)), claimed, 100000, false)
                .await,
            BlobErrorCode::FormatNotAllowed,
        );
    }
    for extension in [
        "jpg", "png", "mp4", "mp3", "doc", "docx", "xls", "xlsx", "pdf",
    ] {
        let mut bytes = fixture(extension);
        bytes.truncate(bytes.len() / 2);
        assert!(
            verifier
                .verify(content(bytes), extension, 100000, false)
                .await
                .is_err(),
            "{extension}"
        );
    }
}
#[tokio::test]
async fn office_xml_dtd_invalid_namespace_structure_and_content_types_reject() {
    let (_temp, verifier) = verifier(VerificationLimits::default()).await;
    for document in [b"<!DOCTYPE document [<!ENTITY leak SYSTEM 'file:///etc/passwd'>]><w:document xmlns:w='http://schemas.openxmlformats.org/wordprocessingml/2006/main'><w:body>&leak;</w:body></w:document>".as_slice(),
        b"<w:document xmlns:w='wrong'><w:body/></w:document>",b"<w:document xmlns:w='http://schemas.openxmlformats.org/wordprocessingml/2006/main'/>",
        b"<document><body></document>",b"<w:document xmlns:w='http://schemas.openxmlformats.org/wordprocessingml/2006/main'><w:body>&#not-a-number;</w:body></w:document>"]{
        let bytes=repack(BTreeMap::from([("word/document.xml".into(),document.to_vec())]),vec![]);code(verifier.verify(content(bytes),"docx",1000000,false).await,BlobErrorCode::FormatNotAllowed);
    }
    let types=b"<Types xmlns='http://schemas.openxmlformats.org/package/2006/content-types'><Override PartName='/word/document.xml' ContentType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml'/></Types>";
    code(
        verifier
            .verify(
                content(repack(
                    BTreeMap::from([("[Content_Types].xml".into(), types.to_vec())]),
                    vec![],
                )),
                "docx",
                1000000,
                false,
            )
            .await,
        BlobErrorCode::FormatNotAllowed,
    );
}
#[tokio::test]
async fn zip_traversal_duplicate_crc_and_actual_expansion_are_checked() {
    let (_temp, verifier) = verifier(VerificationLimits::default()).await;
    for name in [
        "../outside",
        "/absolute",
        "word/../../outside",
        "word\\outside",
    ] {
        let bytes = repack(BTreeMap::new(), vec![(name.into(), b"blocked".to_vec())]);
        code(
            verifier
                .verify(content(bytes), "docx", 1000000, false)
                .await,
            BlobErrorCode::FormatNotAllowed,
        );
    }
    let mut bytes = repack(
        BTreeMap::new(),
        vec![("aa.bin".into(), vec![1]), ("bb.bin".into(), vec![2])],
    );
    for index in 0..bytes.len() - 6 {
        if &bytes[index..index + 6] == b"bb.bin" {
            bytes[index..index + 6].copy_from_slice(b"aa.bin");
        }
    }
    code(
        verifier
            .verify(content(bytes), "docx", 1000000, false)
            .await,
        BlobErrorCode::FormatNotAllowed,
    );
    let bytes = repack(
        BTreeMap::new(),
        vec![("payload.bin".into(), vec![0; 200000])],
    );
    let mut bad_crc = fixture("docx");
    let central = bad_crc
        .windows(4)
        .position(|part| part == b"PK\x01\x02")
        .unwrap();
    bad_crc[central + 16] ^= 1;
    code(
        verifier
            .verify(content(bad_crc), "docx", 1000000, false)
            .await,
        BlobErrorCode::FormatNotAllowed,
    );
    let (_other, limited) = verifier_with_expansion(32000).await;
    code(
        limited
            .verify(content(bytes.clone()), "docx", 1000000, false)
            .await,
        BlobErrorCode::SizeExceeded,
    );
    // Lie about the central-directory expanded length. Actual inflation remains bounded.
    let mut forged = bytes;
    for index in 0..forged.len() - 46 {
        if &forged[index..index + 4] == b"PK\x01\x02" {
            let name_len =
                u16::from_le_bytes(forged[index + 28..index + 30].try_into().unwrap()) as usize;
            if index + 46 + name_len <= forged.len()
                && &forged[index + 46..index + 46 + name_len] == b"payload.bin"
            {
                forged[index + 24..index + 28].copy_from_slice(&1u32.to_le_bytes());
            }
        }
    }
    assert!(
        limited
            .verify(content(forged), "docx", 1000000, false)
            .await
            .is_err()
    );
}
async fn verifier_with_expansion(maximum: usize) -> (TempDir, NativeBlobContentVerifier) {
    verifier(VerificationLimits {
        maximum_expanded_bytes: maximum,
        ..Default::default()
    })
    .await
}
#[tokio::test]
async fn size_empty_entry_text_cell_and_word_piece_budgets_fail_closed() {
    let (_temp, verifier) = verifier(VerificationLimits::default()).await;
    code(
        verifier
            .verify(content(fixture("pdf")), "pdf", 1, false)
            .await,
        BlobErrorCode::SizeExceeded,
    );
    code(
        verifier.verify(content(vec![]), "pdf", 100, false).await,
        BlobErrorCode::SizeExceeded,
    );
    code(
        verifier.verify(content(vec![]), "pdf", 100, true).await,
        BlobErrorCode::FormatNotAllowed,
    );
    for limits in [
        VerificationLimits {
            maximum_package_entries: 1,
            ..Default::default()
        },
        VerificationLimits {
            maximum_text_characters: 1,
            ..Default::default()
        },
    ] {
        let (_temp, limited) = self::verifier(limits).await;
        code(
            limited
                .verify(content(fixture("docx")), "docx", 100000, false)
                .await,
            BlobErrorCode::SizeExceeded,
        );
    }
    let mut bytes = fixture("doc");
    {
        let mut container = cfb::CompoundFile::open(Cursor::new(&mut bytes)).unwrap();
        let mut word = container.open_stream("/WordDocument").unwrap();
        let mut csw = [0; 2];
        word.seek(SeekFrom::Start(32)).unwrap();
        word.read_exact(&mut csw).unwrap();
        let rglw = 34 + u16::from_le_bytes(csw) as u64 * 2 + 2;
        word.seek(SeekFrom::Start(rglw + 12)).unwrap();
        word.write_all(&u32::MAX.to_le_bytes()).unwrap();
    }
    code(
        verifier.verify(content(bytes), "doc", 100000, false).await,
        BlobErrorCode::SizeExceeded,
    );
    let mut archive = zip::ZipArchive::new(Cursor::new(fixture("xlsx"))).unwrap();
    let mut payloads = Vec::new();
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).unwrap();
        let name = entry.name().to_owned();
        let mut bytes = Vec::new();
        entry.read_to_end(&mut bytes).unwrap();
        if name.starts_with("xl/worksheets/") {
            bytes = String::from_utf8(bytes)
                .unwrap()
                .replace("ref=\"A1:B2\"", "ref=\"A1:XFD1048576\"")
                .into_bytes();
        }
        payloads.push((name, bytes));
    }
    let mut zip = zip::ZipWriter::new(Cursor::new(Vec::new()));
    for (name, bytes) in payloads {
        zip.start_file(name, zip::write::SimpleFileOptions::default())
            .unwrap();
        zip.write_all(&bytes).unwrap();
    }
    code(
        verifier
            .verify(
                content(zip.finish().unwrap().into_inner()),
                "xlsx",
                100000,
                false,
            )
            .await,
        BlobErrorCode::SizeExceeded,
    );
}
#[tokio::test]
async fn private_passive_readiness_foreign_roots_and_replacement_reject() {
    let temp = temp();
    let root = temp.path().join("scratch");
    let verifier = NativeBlobContentVerifier::new(&root, VerificationLimits::default()).unwrap();
    assert!(!root.exists());
    code(
        verifier.validate_ready().await,
        BlobErrorCode::CapabilityMissing,
    );
    verifier.initialize().await.unwrap();
    verifier.initialize().await.unwrap();
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
    fs::rename(&root, temp.path().join("old")).unwrap();
    fs::create_dir(&root).unwrap();
    fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
    assert!(verifier.validate_ready().await.is_err());
    let foreign = temp.path().join("foreign");
    fs::create_dir(&foreign).unwrap();
    fs::set_permissions(&foreign, fs::Permissions::from_mode(0o700)).unwrap();
    fs::write(foreign.join("keep"), b"retained").unwrap();
    assert!(
        NativeBlobContentVerifier::new(&foreign, VerificationLimits::default())
            .unwrap()
            .initialize()
            .await
            .is_err()
    );
    assert_eq!(fs::read_dir(&foreign).unwrap().count(), 1);
    let link = temp.path().join("link");
    symlink(&foreign, &link).unwrap();
    assert!(
        NativeBlobContentVerifier::new(link, VerificationLimits::default())
            .unwrap()
            .initialize()
            .await
            .is_err()
    );
    assert!(NativeBlobContentVerifier::new("relative", VerificationLimits::default()).is_err());
    assert!(
        NativeBlobContentVerifier::new(
            temp.path(),
            VerificationLimits {
                concurrent_verifications: 0,
                ..Default::default()
            }
        )
        .is_err()
    );
}
#[tokio::test]
async fn cancellation_capacity_and_source_failure_release_anonymous_scratch() {
    let (temp, verifier) = verifier(VerificationLimits {
        concurrent_verifications: 1,
        ..Default::default()
    })
    .await;
    let (send, ready) = tokio::sync::oneshot::channel();
    let input = Box::pin(stream::once(async move {
        let _ = send.send(());
        std::future::pending::<BlobResult<Bytes>>().await
    }));
    let v = verifier.clone();
    let work = tokio::spawn(async move { v.verify(input, "pdf", 100000, false).await });
    ready.await.unwrap();
    assert_eq!(
        fs::read_dir(temp.path().join("scratch")).unwrap().count(),
        1
    );
    code(
        verifier
            .verify(content(fixture("pdf")), "pdf", 100000, false)
            .await,
        BlobErrorCode::VerificationBusy,
    );
    work.abort();
    assert!(work.await.unwrap_err().is_cancelled());
    verifier
        .verify(content(fixture("pdf")), "pdf", 100000, false)
        .await
        .unwrap();
    code(
        verifier
            .verify(
                Box::pin(stream::iter([Err(BlobErrorCode::ReadDenied.into())])),
                "pdf",
                100000,
                false,
            )
            .await,
        BlobErrorCode::StoreFailed,
    );
    assert_eq!(
        fs::read_dir(temp.path().join("scratch")).unwrap().count(),
        1
    );
}
struct Process(Child);
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_process_kill_leaves_no_named_payload_or_recovery_deletion() {
    if std::env::var("TEAQL_NATIVE_CHILD").as_deref() == Ok("scratch") {
        let root = PathBuf::from(std::env::var_os("TEAQL_NATIVE_ROOT").unwrap());
        let verifier = NativeBlobContentVerifier::new(root, VerificationLimits::default()).unwrap();
        verifier.initialize().await.unwrap();
        let input = Box::pin(stream::once(async {
            fs::write(std::env::var_os("TEAQL_NATIVE_READY").unwrap(), b"ready").unwrap();
            tokio::task::spawn_blocking(|| {
                let mut x = [0];
                std::io::stdin().read_exact(&mut x).unwrap();
            })
            .await
            .unwrap();
            Ok(Bytes::from_static(b"blocked"))
        }));
        let _ = verifier.verify(input, "pdf", 100000, false).await;
        return;
    }
    let (temp, verifier) = verifier(VerificationLimits::default()).await;
    let root = temp.path().join("scratch");
    let ready = temp.path().join("ready");
    let mut process = Process(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "real_process_kill_leaves_no_named_payload_or_recovery_deletion",
                "--nocapture",
            ])
            .env("TEAQL_NATIVE_CHILD", "scratch")
            .env("TEAQL_NATIVE_ROOT", &root)
            .env("TEAQL_NATIVE_READY", &ready)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    tokio::time::timeout(Duration::from_secs(10), async {
        while !ready.exists() {
            assert!(process.0.try_wait().unwrap().is_none());
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let descriptors = fs::read_dir(format!("/proc/{}/fd", process.0.id()))
        .unwrap()
        .filter_map(|entry| fs::read_link(entry.ok()?.path()).ok())
        .collect::<Vec<_>>();
    assert!(
        descriptors
            .iter()
            .any(|p| p.to_string_lossy().contains(root.to_str().unwrap())
                && p.to_string_lossy().contains("(deleted)"))
    );
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
    process.0.kill().unwrap();
    assert!(!process.0.wait().unwrap().success());
    assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
    verifier
        .verify(content(fixture("pdf")), "pdf", 100000, false)
        .await
        .unwrap();
}
#[test]
fn verified_facts_are_closed_and_diagnostics_redact_content() {
    let verified = VerifiedBlobContent::new("application/pdf", 1, "a".repeat(64)).unwrap();
    assert!(!format!("{verified:?}").contains(&"a".repeat(64)));
    assert!(VerifiedBlobContent::new("text/plain", 1, "a".repeat(64)).is_err());
    assert!(VerifiedBlobContent::new("application/pdf", 1, "A".repeat(64)).is_err());
}

fn atom(kind: &[u8; 4], body: Vec<u8>) -> Vec<u8> {
    let mut result = ((body.len() + 8) as u32).to_be_bytes().to_vec();
    result.extend_from_slice(kind);
    result.extend(body);
    result
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mp4_metadata_zero_boxes_and_sample_work_are_bounded() {
    if let Some(path) = std::env::var_os("TEAQL_NATIVE_METADATA_INPUT") {
        let root = PathBuf::from(std::env::var_os("TEAQL_NATIVE_ROOT").unwrap());
        let verifier = NativeBlobContentVerifier::new(root, VerificationLimits::default()).unwrap();
        verifier.initialize().await.unwrap();
        let expected = if std::env::var("TEAQL_NATIVE_METADATA_ERROR").as_deref() == Ok("size") {
            BlobErrorCode::SizeExceeded
        } else {
            BlobErrorCode::FormatNotAllowed
        };
        code(
            verifier
                .verify(content(fs::read(path).unwrap()), "mp4", 1000000, false)
                .await,
            expected,
        );
        return;
    }
    let temp = temp();
    let mut zero = fixture("mp4");
    let meta = atom(b"meta", [vec![0; 4], vec![0; 4], b"free".to_vec()].concat());
    let mut at = 0;
    loop {
        let size = u32::from_be_bytes(zero[at..at + 4].try_into().unwrap()) as usize;
        if &zero[at + 4..at + 8] == b"moov" {
            zero[at..at + 4].copy_from_slice(&((size + meta.len()) as u32).to_be_bytes());
            zero.splice(at + size..at + size, meta);
            break;
        }
        at += size;
        assert!(at < zero.len());
    }
    let mut work = fixture("mp4");
    work.extend(atom(
        b"moof",
        atom(
            b"traf",
            atom(
                b"trun",
                [vec![0; 4], u32::MAX.to_be_bytes().to_vec()].concat(),
            ),
        ),
    ));
    for (index, (payload, expected)) in [(zero, "format"), (work, "size")].into_iter().enumerate() {
        let input = temp.path().join(format!("case{index}.mp4"));
        fs::write(&input, payload).unwrap();
        let mut process = Process(
            Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "mp4_metadata_zero_boxes_and_sample_work_are_bounded",
                    "--nocapture",
                ])
                .env("TEAQL_NATIVE_METADATA_INPUT", input)
                .env(
                    "TEAQL_NATIVE_ROOT",
                    temp.path().join(format!("case{index}-scratch")),
                )
                .env("TEAQL_NATIVE_METADATA_ERROR", expected)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let status = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(status) = process.0.try_wait().unwrap() {
                    break status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("owned metadata parser child exceeded deadline");
        assert!(status.success());
    }
}
