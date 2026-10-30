use teaql_blob_contracts::{BlobErrorCode, SignedBlobRead};

#[test]
fn rejects_raw_unicode_whitespace_without_rewriting_signed_urls() {
    let invalid = [
        "\u{00a0}", // no-break space
        "\u{2003}", // em space
        "\u{2028}", // line separator
        "\u{2029}", // paragraph separator
    ];
    for character in invalid {
        let raw = format!(
            "https://storage.example.test/blob/{character}file?X-Amz-Date=20261030T000000Z"
        );
        let error = SignedBlobRead::new(raw, 1_800_000_000_000).unwrap_err();
        assert_eq!(
            error.code,
            BlobErrorCode::SignFailed,
            "U+{:04X}",
            character.chars().next().unwrap() as u32
        );
    }

    let valid = "https://storage.example.test/blob/%E2%80%A8file?X-Amz-Date=20261030T000000Z&X-Amz-Signature=a%2Bb%2Fc";
    let signed = SignedBlobRead::new(valid, 1_800_000_000_000).unwrap();
    assert_eq!(signed.as_str(), valid);
}
