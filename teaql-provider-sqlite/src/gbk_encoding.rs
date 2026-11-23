use encoding_rs::{EncoderResult, GBK};

// PostgreSQL's UTF8 -> GBK profile, not WHATWG's GB18030-backed superset.
// All BMP scalar values are qualified by the conformance codec probe. MS936
// private-use mappings and later GB18030 additions are not portable GBK keys.
pub(crate) fn encode(text: &str) -> Option<Vec<u8>> {
    if text
        .chars()
        .any(|c| c == '\0' || ('\u{e000}'..='\u{f8ff}').contains(&c))
    {
        return None;
    }
    let mut encoder = GBK.new_encoder();
    let mut output = vec![0; encoder.max_buffer_length_from_utf8_without_replacement(text.len())?];
    let (result, read, written) =
        encoder.encode_from_utf8_without_replacement(text, &mut output, true);
    if result != EncoderResult::InputEmpty || read != text.len() {
        return None;
    }
    output.truncate(written);
    let mut bytes = output.iter().copied();
    while let Some(first) = bytes.next() {
        if first <= 0x80 {
            continue;
        }
        let second = bytes.next()?;
        let code = u16::from_be_bytes([first, second]);
        if matches!(code, 0xa6d9..=0xa6df | 0xa6ec..=0xa6ed | 0xa6f3 |
            0xa8bc | 0xa8bf | 0xa989..=0xa995 | 0xfe50..=0xfefe)
        {
            return None;
        }
    }
    Some(output)
}
