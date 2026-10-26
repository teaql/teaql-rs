mod office;
use crate::VerificationLimits;
use std::{collections::BTreeSet, io::Cursor};
use teaql_blob_reference::{BlobError, BlobErrorCode as Code, BlobResult};
pub(super) fn verify(
    bytes: &[u8],
    file: &std::fs::File,
    extension: &str,
    limits: &VerificationLimits,
) -> BlobResult<()> {
    match extension {
        "jpg" | "jpeg" => jpeg(bytes, limits),
        "png" => png(bytes, limits),
        "mp4" => mp4(bytes, limits),
        "mp3" => {
            mpeg_frame_bounds(bytes)?;
            mp3(file, limits)
        }
        "pdf" => pdf(bytes, limits),
        "doc" | "xls" | "docx" | "xlsx" => office::verify(bytes, extension, limits),
        _ => Err(Code::FormatNotAllowed.into()),
    }
}
fn invalid(_: impl std::fmt::Debug) -> BlobError {
    Code::FormatNotAllowed.into()
}
fn require(condition: bool) -> BlobResult<()> {
    if condition {
        Ok(())
    } else {
        Err(Code::FormatNotAllowed.into())
    }
}
fn budget(condition: bool) -> BlobResult<()> {
    if condition {
        Ok(())
    } else {
        Err(Code::SizeExceeded.into())
    }
}
fn jpeg(bytes: &[u8], limits: &VerificationLimits) -> BlobResult<()> {
    require(bytes.starts_with(&[0xff, 0xd8]))?;
    let mut decoder = jpeg_decoder::Decoder::new(Cursor::new(bytes));
    decoder.set_max_decoding_buffer_size(limits.maximum_expanded_bytes);
    decoder.read_info().map_err(invalid)?;
    let info = decoder.info().ok_or(Code::FormatNotAllowed)?;
    require(info.width > 0 && info.height > 0)?;
    budget(
        (info.width as usize)
            .checked_mul(info.height as usize)
            .and_then(|n| n.checked_mul(info.pixel_format.pixel_bytes()))
            .is_some_and(|n| n <= limits.maximum_expanded_bytes),
    )?;
    let decoded = decoder.decode().map_err(invalid)?;
    require(!decoded.is_empty())
}
fn png(bytes: &[u8], limits: &VerificationLimits) -> BlobResult<()> {
    require(bytes.starts_with(b"\x89PNG\r\n\x1a\n"))?;
    let mut decoder = png::Decoder::new_with_limits(
        Cursor::new(bytes),
        png::Limits {
            bytes: limits.maximum_expanded_bytes,
        },
    );
    decoder.set_ignore_text_chunk(true);
    let mut reader = decoder.read_info().map_err(invalid)?;
    require(reader.info().width > 0 && reader.info().height > 0)?;
    let size = reader.output_buffer_size().ok_or(Code::SizeExceeded)?;
    budget(size <= limits.maximum_expanded_bytes)?;
    // Decode real scanlines/CRC rather than accepting a fabricated dimensions header.
    let mut output = vec![0; size];
    reader.next_frame(&mut output).map_err(invalid)?;
    reader.finish().map_err(invalid)?;
    Ok(())
}
fn mp3(file: &std::fs::File, limits: &VerificationLimits) -> BlobResult<()> {
    use std::io::{Seek, SeekFrom};
    use symphonia::core::{
        codecs::{CODEC_TYPE_MP3, DecoderOptions},
        formats::FormatOptions,
        io::MediaSourceStream,
        meta::{Limit, MetadataOptions},
        probe::Hint,
    };
    let mut reader = file.try_clone().map_err(invalid)?;
    reader.seek(SeekFrom::Start(0)).map_err(invalid)?;
    let source = MediaSourceStream::new(Box::new(reader), Default::default());
    // Only the MPEG audio feature is compiled; no filename/MIME hint is supplied.
    let metadata = MetadataOptions {
        limit_metadata_bytes: Limit::Maximum(limits.maximum_expanded_bytes),
        limit_visual_bytes: Limit::Maximum(limits.maximum_expanded_bytes),
    };
    let mut format = symphonia::default::get_probe()
        .format(&Hint::new(), source, &FormatOptions::default(), &metadata)
        .map_err(invalid)?
        .format;
    let track = format.default_track().ok_or(Code::FormatNotAllowed)?;
    require(
        track.codec_params.codec == CODEC_TYPE_MP3
            && track.codec_params.sample_rate.is_some_and(|v| v > 0),
    )?;
    let id = track.id;
    let mut decoder = symphonia::default::get_codecs()
        .make(&track.codec_params, &DecoderOptions::default())
        .map_err(invalid)?;
    let mut count = 0usize;
    let mut decoded = false;
    loop {
        let packet = match format.next_packet() {
            Ok(packet) => packet,
            Err(symphonia::core::errors::Error::IoError(error))
                if error.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Err(error) => return Err(invalid(error)),
        };
        count += 1;
        budget(count <= limits.maximum_expanded_bytes / 4)?;
        if packet.track_id() == id {
            let audio = decoder.decode(&packet).map_err(invalid)?;
            decoded |= audio.frames() > 0;
        }
    }
    require(decoded)
}
fn mpeg_frame_bounds(bytes: &[u8]) -> BlobResult<()> {
    let mut at = 0usize;
    let mut end = bytes.len();
    let mut frames = 0;
    if end >= 128 && bytes.get(end - 128..end - 125) == Some(b"TAG") {
        end -= 128;
    }
    if end >= 32 && bytes.get(end - 32..end - 24) == Some(b"APETAGEX") {
        let size = u32::from_le_bytes(bytes[end - 20..end - 16].try_into().unwrap()) as usize;
        let flags = u32::from_le_bytes(bytes[end - 12..end - 8].try_into().unwrap());
        require(size >= 32 && size <= end)?;
        end -= size;
        if flags & 0x80000000 != 0 {
            require(end >= 32 && bytes.get(end - 32..end - 24) == Some(b"APETAGEX"))?;
            end -= 32;
        }
    }
    while at < end {
        if bytes.get(at..at + 3) == Some(b"ID3") {
            require(
                end - at >= 10
                    && (2..=4).contains(&bytes[at + 3])
                    && bytes[at + 6..at + 10].iter().all(|b| b & 0x80 == 0),
            )?;
            let length = bytes[at + 6..at + 10]
                .iter()
                .fold(0usize, |n, b| (n << 7) | *b as usize);
            let footer = bytes[at + 3] == 4 && bytes[at + 5] & 0x10 != 0;
            require(at.checked_add(10 + length).is_some_and(|n| n <= end))?;
            at += 10 + length;
            if footer {
                require(at + 10 <= end && bytes.get(at..at + 3) == Some(b"3DI"))?;
                at += 10;
            }
            continue;
        }
        require(end - at >= 4)?;
        let header = u32::from_be_bytes(bytes[at..at + 4].try_into().unwrap());
        let version = (header >> 19) & 3;
        let bitrate = ((header >> 12) & 15) as usize;
        let frequency = ((header >> 10) & 3) as usize;
        require(
            header & 0xffe00000 == 0xffe00000
                && version != 1
                && (header >> 17) & 3 == 1
                && bitrate > 0
                && bitrate < 15
                && frequency < 3,
        )?;
        let rates = if version == 3 {
            [
                0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320,
            ]
        } else {
            [0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160]
        };
        let rate = [44100, 48000, 32000][frequency]
            / if version == 3 {
                1
            } else if version == 2 {
                2
            } else {
                4
            };
        let length = if version == 3 { 144 } else { 72 } * rates[bitrate] * 1000 / rate
            + ((header >> 9) & 1) as usize;
        require(length >= 4 && at.checked_add(length).is_some_and(|n| n <= end))?;
        at += length;
        frames += 1;
    }
    require(frames > 0 && at == end)
}
fn mp4(bytes: &[u8], limits: &VerificationLimits) -> BlobResult<()> {
    let mut boxes = 0;
    let mut work = 0;
    let mut ftyp = false;
    let mut mdat = false;
    walk_boxes(
        bytes, 0, limits, &mut boxes, &mut work, &mut ftyp, &mut mdat,
    )?;
    require(ftyp && mdat)?;
    let reader =
        mp4::Mp4Reader::read_header(Cursor::new(bytes), bytes.len() as u64).map_err(invalid)?;
    require(reader.tracks().values().any(|track| {
        track
            .track_type()
            .is_ok_and(|kind| kind == mp4::TrackType::Video)
            && track.width() > 0
            && track.height() > 0
            && track.sample_count() > 0
    }))
}
fn walk_boxes(
    mut bytes: &[u8],
    depth: usize,
    limits: &VerificationLimits,
    count: &mut usize,
    work: &mut usize,
    ftyp: &mut bool,
    mdat: &mut bool,
) -> BlobResult<()> {
    budget(depth <= 64)?;
    while !bytes.is_empty() {
        require(bytes.len() >= 8)?;
        *count += 1;
        budget(*count <= limits.maximum_package_entries)?;
        let short = u32::from_be_bytes(bytes[..4].try_into().unwrap());
        let kind = &bytes[4..8];
        require(short != 0 || (depth == 0 && kind == b"mdat"))?;
        let (size, header) = if short == 1 {
            require(bytes.len() >= 16)?;
            (u64::from_be_bytes(bytes[8..16].try_into().unwrap()), 16)
        } else if short == 0 {
            (bytes.len() as u64, 8)
        } else {
            (short as u64, 8)
        };
        require(size >= header as u64 && size <= bytes.len() as u64)?;
        let size = size as usize;
        let body = &bytes[header..size];
        if kind == b"ftyp" {
            require(depth == 0 && body.len() >= 8 && (body.len() - 8).is_multiple_of(4))?;
            *ftyp = std::iter::once(&body[..4])
                .chain(body[8..].chunks_exact(4))
                .any(|brand| {
                    matches!(
                        brand,
                        b"isom"
                            | b"iso2"
                            | b"iso3"
                            | b"iso4"
                            | b"iso5"
                            | b"iso6"
                            | b"mp41"
                            | b"mp42"
                            | b"avc1"
                            | b"dash"
                            | b"M4V "
                    )
                });
        } else if kind == b"mdat" {
            require(!body.is_empty())?;
            *mdat = true;
        } else if matches!(
            kind,
            b"moov"
                | b"trak"
                | b"mdia"
                | b"minf"
                | b"stbl"
                | b"moof"
                | b"traf"
                | b"udta"
                | b"edts"
                | b"mvex"
                | b"dinf"
                | b"ilst"
                | b"\xa9nam"
                | b"\xa9day"
                | b"covr"
                | b"desc"
        ) {
            walk_boxes(body, depth + 1, limits, count, work, ftyp, mdat)?;
        } else if kind == b"meta" {
            require(body.len() >= 4)?;
            let skip = if body[..4] == [0; 4] {
                4
            } else {
                require(body.get(4..8) == Some(b"hdlr"))?;
                0
            };
            walk_boxes(&body[skip..], depth + 1, limits, count, work, ftyp, mdat)?;
        } else if matches!(kind, b"stsd" | b"dref") {
            require(body.len() >= 8)?;
            let entries = u32::from_be_bytes(body[4..8].try_into().unwrap()) as usize;
            budget(entries <= limits.maximum_package_entries)?;
            require(entries <= (body.len() - 8) / 8)?;
            walk_boxes(&body[8..], depth + 1, limits, count, work, ftyp, mdat)?;
        } else if matches!(kind, b"avc1" | b"hev1" | b"vp09") {
            require(body.len() >= 78)?;
            walk_boxes(&body[78..], depth + 1, limits, count, work, ftyp, mdat)?;
        } else if kind == b"mp4a" {
            require(body.len() >= 28)?;
            walk_boxes(&body[28..], depth + 1, limits, count, work, ftyp, mdat)?;
        } else if kind == b"trun" {
            require(body.len() >= 8)?;
            let flags = u32::from_be_bytes(body[..4].try_into().unwrap()) & 0x00ffffff;
            let entries = u32::from_be_bytes(body[4..8].try_into().unwrap()) as usize;
            *work = work.checked_add(entries).ok_or(Code::SizeExceeded)?;
            budget(*work <= limits.maximum_expanded_bytes / 4)?;
            let prefix =
                8 + if flags & 1 != 0 { 4 } else { 0 } + if flags & 4 != 0 { 4 } else { 0 };
            let width = (flags & 0xf00).count_ones() as usize * 4;
            require(
                entries
                    .checked_mul(width)
                    .and_then(|n| n.checked_add(prefix))
                    .is_some_and(|n| n <= body.len()),
            )?;
        } else if matches!(
            kind,
            b"stts" | b"ctts" | b"stsc" | b"stco" | b"co64" | b"stss"
        ) {
            require(body.len() >= 8)?;
            let entries = u32::from_be_bytes(body[4..8].try_into().unwrap()) as usize;
            *work = work.checked_add(entries).ok_or(Code::SizeExceeded)?;
            budget(*work <= limits.maximum_expanded_bytes / 4)?;
            let width = if kind == b"stsc" {
                12
            } else if matches!(kind, b"stts" | b"ctts" | b"co64") {
                8
            } else {
                4
            };
            budget(entries <= limits.maximum_expanded_bytes / width)?;
            require(
                entries
                    .checked_mul(width)
                    .and_then(|n| n.checked_add(8))
                    .is_some_and(|n| n <= body.len()),
            )?;
        } else if kind == b"stsz" {
            require(body.len() >= 12)?;
            let sample_size = u32::from_be_bytes(body[4..8].try_into().unwrap()) as usize;
            let entries = u32::from_be_bytes(body[8..12].try_into().unwrap()) as usize;
            *work = work.checked_add(entries).ok_or(Code::SizeExceeded)?;
            budget(*work <= limits.maximum_expanded_bytes / 4)?;
            budget(entries <= limits.maximum_expanded_bytes / 4)?;
            if sample_size == 0 {
                require(
                    entries
                        .checked_mul(4)
                        .and_then(|n| n.checked_add(12))
                        .is_some_and(|n| n <= body.len()),
                )?;
                for item in body[12..12 + entries * 4].chunks_exact(4) {
                    budget(
                        u32::from_be_bytes(item.try_into().unwrap()) as usize
                            <= limits.maximum_expanded_bytes,
                    )?;
                }
            } else {
                budget(sample_size <= limits.maximum_expanded_bytes)?;
            }
        }
        bytes = &bytes[size..];
    }
    Ok(())
}
fn pdf(bytes: &[u8], limits: &VerificationLimits) -> BlobResult<()> {
    use lopdf::{Document, LoadOptions, Object};
    require(bytes.starts_with(b"%PDF-"))?;
    let document = Document::load_mem_with_options(
        bytes,
        LoadOptions {
            strict: true,
            max_decompressed_size: Some(limits.maximum_expanded_bytes),
            ..Default::default()
        },
    )
    .map_err(invalid)?;
    require(!document.is_encrypted())?;
    budget(document.objects.len() <= limits.maximum_package_entries)?;
    let root = document
        .trailer
        .get(b"Root")
        .and_then(Object::as_reference)
        .map_err(invalid)?;
    let catalog = document.get_dictionary(root).map_err(invalid)?;
    require(
        catalog
            .get(b"Type")
            .and_then(Object::as_name)
            .map_err(invalid)?
            == b"Catalog",
    )?;
    let mut pending = vec![
        catalog
            .get(b"Pages")
            .and_then(Object::as_reference)
            .map_err(invalid)?,
    ];
    let mut seen = BTreeSet::new();
    let mut pages = 0;
    let mut expanded = 0usize;
    let mut text = 0usize;
    while let Some(id) = pending.pop() {
        require(seen.insert(id))?;
        budget(seen.len() <= limits.maximum_package_entries)?;
        let dict = document.get_dictionary(id).map_err(invalid)?;
        match dict
            .get(b"Type")
            .and_then(Object::as_name)
            .map_err(invalid)?
        {
            b"Pages" => {
                let kids = dict
                    .get(b"Kids")
                    .and_then(Object::as_array)
                    .map_err(invalid)?;
                budget(
                    kids.len()
                        <= limits.maximum_package_entries
                            - pending.len().min(limits.maximum_package_entries),
                )?;
                for kid in kids {
                    pending.push(kid.as_reference().map_err(invalid)?);
                }
            }
            b"Page" => {
                pages += 1;
                let content = document
                    .get_page_content_with_limit(id, limits.maximum_expanded_bytes - expanded)
                    .map_err(invalid)?;
                expanded = expanded
                    .checked_add(content.len())
                    .ok_or(Code::SizeExceeded)?;
                budget(expanded <= limits.maximum_expanded_bytes)?;
                let parsed = lopdf::content::Content::decode(&content).map_err(invalid)?;
                budget(parsed.operations.len() <= limits.maximum_package_entries)?;
                for operation in parsed.operations {
                    for value in operation.operands {
                        pdf_text(&value, 0, &mut text, limits)?;
                    }
                }
            }
            _ => return Err(Code::FormatNotAllowed.into()),
        }
    }
    require(pages > 0)
}
fn pdf_text(
    object: &lopdf::Object,
    depth: usize,
    total: &mut usize,
    limits: &VerificationLimits,
) -> BlobResult<()> {
    budget(depth <= 64)?;
    match object {
        lopdf::Object::String(bytes, _) => {
            *total = total.checked_add(bytes.len()).ok_or(Code::SizeExceeded)?;
            budget(*total <= limits.maximum_text_characters)?;
        }
        lopdf::Object::Array(items) => {
            budget(items.len() <= limits.maximum_package_entries)?;
            for item in items {
                pdf_text(item, depth + 1, total, limits)?;
            }
        }
        _ => (),
    };
    Ok(())
}
