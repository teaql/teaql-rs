use super::{budget, invalid, require};
use crate::VerificationLimits;
use calamine::Reader;
use std::{
    collections::BTreeSet,
    io::{BufRead, BufReader, Cursor, Read, Seek, SeekFrom},
};
use teaql_blob_contracts::{BlobErrorCode as Code, BlobResult};

const CFB: &[u8] = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1";
const WORD_NS: &[u8] = b"http://schemas.openxmlformats.org/wordprocessingml/2006/main";
const EXCEL_NS: &[u8] = b"http://schemas.openxmlformats.org/spreadsheetml/2006/main";
pub(super) fn verify(bytes: &[u8], extension: &str, limits: &VerificationLimits) -> BlobResult<()> {
    if matches!(extension, "doc" | "xls") {
        require(bytes.starts_with(CFB))?;
        let mut container = cfb::CompoundFile::open(Cursor::new(bytes)).map_err(invalid)?;
        let mut streams = Vec::new();
        let mut expanded = 0u64;
        let mut entries = 0;
        for entry in container.walk() {
            entries += 1;
            budget(entries <= limits.maximum_package_entries)?;
            budget(entry.path().components().count() <= 128)?;
            if entry.is_stream() {
                expanded = expanded
                    .checked_add(entry.len())
                    .ok_or(Code::SizeExceeded)?;
                budget(expanded <= limits.maximum_expanded_bytes as u64)?;
                streams.push(entry.path().to_owned());
            }
        }
        let mut buffer = [0; 32768];
        for path in streams {
            let mut input = container.open_stream(path).map_err(invalid)?;
            while input.read(&mut buffer).map_err(invalid)? > 0 {}
        }
        if extension == "doc" {
            require(
                container.is_stream("/WordDocument")
                    && !container.is_stream("/Workbook")
                    && !container.is_stream("/Book"),
            )?;
            word(&mut container, limits)
        } else {
            require(
                !container.is_stream("/WordDocument")
                    && (container.is_stream("/Workbook") || container.is_stream("/Book")),
            )?;
            let workbook = if container.is_stream("/Workbook") {
                "/Workbook"
            } else {
                "/Book"
            };
            biff(container.open_stream(workbook).map_err(invalid)?, limits)?;
            spreadsheet(
                calamine::Xls::new(Cursor::new(bytes)).map_err(invalid)?,
                limits,
            )
        }
    } else {
        require(bytes.starts_with(b"PK\x03\x04"))?;
        package(bytes, extension, limits)?;
        if extension == "xlsx" {
            spreadsheet(
                calamine::Xlsx::new(Cursor::new(bytes)).map_err(invalid)?,
                limits,
            )
        } else {
            Ok(())
        }
    }
}
fn spreadsheet<RS: Read + Seek, R: calamine::Reader<RS>>(
    mut book: R,
    limits: &VerificationLimits,
) -> BlobResult<()> {
    let sheets = book.sheet_names().to_vec();
    require(!sheets.is_empty())?;
    budget(sheets.len() <= limits.maximum_package_entries)?;
    let mut cells = 0usize;
    let mut text = 0usize;
    for sheet in sheets {
        let range = book.worksheet_range(&sheet).map_err(invalid)?;
        let (rows, columns) = range.get_size();
        cells = cells
            .checked_add(rows.checked_mul(columns).ok_or(Code::SizeExceeded)?)
            .ok_or(Code::SizeExceeded)?;
        budget(cells <= limits.maximum_spreadsheet_cells)?;
        for row in range.rows() {
            for value in row {
                if let calamine::Data::String(value) = value {
                    text = text
                        .checked_add(value.chars().count())
                        .ok_or(Code::SizeExceeded)?;
                    budget(text <= limits.maximum_text_characters)?;
                }
            }
        }
    }
    Ok(())
}
fn biff(mut input: impl Read + Seek, limits: &VerificationLimits) -> BlobResult<()> {
    let mut header = [0; 4];
    let mut count = 0usize;
    let mut first = true;
    let mut sheet = false;
    let mut saw_sheet = false;
    let mut max_row = 0usize;
    let mut max_column = 0usize;
    loop {
        match input.read_exact(&mut header) {
            Ok(()) => (),
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(invalid(error)),
        }
        let id = u16::from_le_bytes(header[..2].try_into().unwrap());
        let size = u16::from_le_bytes(header[2..].try_into().unwrap()) as usize;
        if id == 0 && size == 0 {
            let mut rest = [0; 4096];
            loop {
                let n = input.read(&mut rest).map_err(invalid)?;
                if n == 0 {
                    break;
                }
                require(rest[..n].iter().all(|b| *b == 0))?;
            }
            break;
        }
        count += 1;
        budget(
            count
                <= limits
                    .maximum_spreadsheet_cells
                    .saturating_mul(4)
                    .saturating_add(limits.maximum_package_entries),
        )?;
        let mut body = vec![0; size];
        input.read_exact(&mut body).map_err(invalid)?;
        if first {
            require(
                id == 0x0809
                    && size >= 4
                    && u16le(&body, 0)? == 0x0600
                    && u16le(&body, 2)? == 0x0005,
            )?;
            first = false;
        }
        if id == 0x002f {
            return Err(Code::FormatNotAllowed.into());
        } // FILEPASS: encrypted workbook.
        if id == 0x0809 {
            require(size >= 4)?;
            sheet = u16le(&body, 2)? == 0x0010;
            saw_sheet |= sheet;
            max_row = 0;
            max_column = 0;
        }
        if sheet && id == 0x0200 {
            require(size >= 12)?;
            let first_row = u32le(&body, 0)? as usize;
            let last_row = u32le(&body, 4)? as usize;
            let first_col = u16le(&body, 8)? as usize;
            let last_col = u16le(&body, 10)? as usize;
            require(last_row >= first_row && last_col >= first_col)?;
            budget(
                (last_row - first_row)
                    .checked_mul(last_col - first_col)
                    .is_some_and(|n| n <= limits.maximum_spreadsheet_cells),
            )?;
        }
        if sheet
            && matches!(
                id,
                0x0203 | 0x00fd | 0x0204 | 0x027e | 0x00bd | 0x0006 | 0x0205 | 0x0201 | 0x00be
            )
        {
            require(size >= 4)?;
            max_row = max_row.max(u16le(&body, 0)? as usize + 1);
            max_column = max_column.max(u16le(&body, 2)? as usize + 1);
            budget(
                max_row
                    .checked_mul(max_column)
                    .is_some_and(|n| n <= limits.maximum_spreadsheet_cells),
            )?;
        }
    }
    require(!first && saw_sheet)
}
fn word(
    container: &mut cfb::CompoundFile<Cursor<&[u8]>>,
    limits: &VerificationLimits,
) -> BlobResult<()> {
    // MS-DOC 2.5: navigate FibBase/FibRgW/FibRgLw/FibRgFcLcb, then CLX/PlcPcd.
    // Read the bounded indexes and actual text spans; never copy the whole CFB file.
    let word_size = container.entry("/WordDocument").map_err(invalid)?.len();
    let mut word = container.open_stream("/WordDocument").map_err(invalid)?;
    let mut base = [0; 34];
    word.read_exact(&mut base).map_err(invalid)?;
    require(
        u16le(&base, 0)? == 0xa5ec
            && matches!(u16le(&base, 2)?, 0x00c1 | 0x00d9 | 0x0101 | 0x010c | 0x0112),
    )?;
    let flags = u16le(&base, 10)?;
    require(flags & 0x8100 == 0)?;
    let table_name = if flags & 0x0200 == 0 {
        "/0Table"
    } else {
        "/1Table"
    };
    let csw = u16le(&base, 32)? as u64;
    budget(csw <= 4096)?;
    let cslw_position = 34 + csw * 2;
    word.seek(SeekFrom::Start(cslw_position)).map_err(invalid)?;
    let cslw = read_u16(&mut word)? as usize;
    require(cslw >= 11)?;
    budget(cslw <= 4096)?;
    let mut longs = vec![0; cslw * 4];
    word.read_exact(&mut longs).map_err(invalid)?;
    let total = (3..=10).try_fold(0u64, |sum, index| {
        u32le(&longs, index * 4).map(|n| sum + n as u64)
    })?;
    budget(total <= limits.maximum_text_characters as u64)?;
    let pairs = read_u16(&mut word)? as usize;
    require(pairs >= 34)?;
    budget(pairs <= 4096)?;
    let mut offsets = vec![0; pairs * 8];
    word.read_exact(&mut offsets).map_err(invalid)?;
    let clx_at = u32le(&offsets, 33 * 8)? as u64;
    let clx_len = u32le(&offsets, 33 * 8 + 4)? as u64;
    drop(word);
    let table_size = container.entry(table_name).map_err(invalid)?.len();
    require(
        clx_at
            .checked_add(clx_len)
            .is_some_and(|end| end <= table_size),
    )?;
    let mut table = container.open_stream(table_name).map_err(invalid)?;
    table.seek(SeekFrom::Start(clx_at)).map_err(invalid)?;
    let end = clx_at + clx_len;
    let mut pieces = Vec::new();
    let mut properties = 0usize;
    while table.stream_position().map_err(invalid)? < end {
        let mut tag = [0];
        table.read_exact(&mut tag).map_err(invalid)?;
        if tag[0] == 1 {
            properties += 1;
            budget(properties <= limits.maximum_package_entries)?;
            let length = read_u16(&mut table)? as u64;
            require(
                table
                    .stream_position()
                    .map_err(invalid)?
                    .checked_add(length)
                    .is_some_and(|next| next <= end),
            )?;
            table
                .seek(SeekFrom::Current(length as i64))
                .map_err(invalid)?;
        } else if tag[0] == 2 {
            let mut length = [0; 4];
            table.read_exact(&mut length).map_err(invalid)?;
            let length = u32::from_le_bytes(length) as usize;
            require(length >= 4 && (length - 4).is_multiple_of(12))?;
            let n = (length - 4) / 12;
            budget(n <= limits.maximum_package_entries)?;
            require(
                table
                    .stream_position()
                    .map_err(invalid)?
                    .checked_add(length as u64)
                    == Some(end),
            )?;
            let mut plc = vec![0; length];
            table.read_exact(&mut plc).map_err(invalid)?;
            require(u32le(&plc, 0)? == 0)?;
            let last = u32le(&plc, n * 4)? as u64;
            require(last >= total)?;
            budget(last <= limits.maximum_text_characters as u64)?;
            for index in 0..n {
                let first = u32le(&plc, index * 4)?;
                let next = u32le(&plc, (index + 1) * 4)?;
                require(next >= first)?;
                let fc = u32le(&plc, (n + 1) * 4 + index * 8 + 2)?;
                require(fc & 0x80000000 == 0)?;
                let compressed = fc & 0x40000000 != 0;
                let raw = (fc & 0x3fffffff) as u64;
                if compressed {
                    require(raw.is_multiple_of(2))?;
                }
                let at = if compressed { raw / 2 } else { raw };
                let size = (next - first) as u64 * if compressed { 1 } else { 2 };
                require(at.checked_add(size).is_some_and(|end| end <= word_size))?;
                pieces.push((at, size));
            }
        } else {
            return Err(Code::FormatNotAllowed.into());
        }
    }
    require(!pieces.is_empty())?;
    drop(table);
    let mut word = container.open_stream("/WordDocument").map_err(invalid)?;
    let mut buffer = [0; 32768];
    for (at, mut length) in pieces {
        word.seek(SeekFrom::Start(at)).map_err(invalid)?;
        while length > 0 {
            let n = (length as usize).min(buffer.len());
            word.read_exact(&mut buffer[..n]).map_err(invalid)?;
            length -= n as u64;
        }
    }
    Ok(())
}
fn package(bytes: &[u8], extension: &str, limits: &VerificationLimits) -> BlobResult<()> {
    let central_entries = zip_directory_entries(bytes, limits)?;
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).map_err(invalid)?;
    // zip's name index coalesces duplicate entries; inspect the original directory
    // before that view can hide duplicates or contradictory local metadata.
    require(archive.len() == central_entries)?;
    require(!archive.is_empty())?;
    budget(archive.len() <= limits.maximum_package_entries)?;
    let mut names = BTreeSet::new();
    let mut expanded = 0u64;
    let mut word = false;
    let mut excel = false;
    let mut declared_word = false;
    let mut declared_excel = false;
    let mut types = false;
    let mut actual_expanded = 0usize;
    for index in 0..archive.len() {
        let mut entry = archive.by_index(index).map_err(invalid)?;
        let name = entry.name().map_err(invalid)?.into_owned();
        require(
            name.len() <= 4096
                && !name.contains(['\\', '\0', ':'])
                && !name.starts_with('/')
                && name.split('/').all(|part| part != ".." && part != ".")
                && names.insert(name.clone())
                && !entry.encrypted(),
        )?;
        require(entry.enclosed_name().is_some())?;
        expanded = expanded
            .checked_add(entry.size())
            .ok_or(Code::SizeExceeded)?;
        budget(expanded <= limits.maximum_expanded_bytes as u64)?;
        let mut buffer = [0; 32768];
        let mut entry_bytes = 0u64;
        loop {
            let n = entry.read(&mut buffer).map_err(invalid)?;
            if n == 0 {
                break;
            }
            actual_expanded = actual_expanded.checked_add(n).ok_or(Code::SizeExceeded)?;
            budget(actual_expanded <= limits.maximum_expanded_bytes)?;
            entry_bytes += n as u64;
        }
        require(entry_bytes == entry.size())?;
    }
    // Immutable mapped input: the first pass bounded actual inflation/CRC for every
    // member, including non-XML payloads and false central-directory size claims.
    let mut package_text = 0usize;
    for name in names {
        if name.ends_with(".xml") || name.ends_with(".rels") {
            let mut entry = archive.by_name(&name).map_err(invalid)?;
            let role = if name == "[Content_Types].xml" {
                types = true;
                "types"
            } else if name == "word/document.xml" {
                word = true;
                "word"
            } else if name == "xl/workbook.xml" {
                excel = true;
                "excel"
            } else if name.starts_with("xl/worksheets/") && name.ends_with(".xml") {
                "sheet"
            } else {
                "other"
            };
            let declared = xml(BufReader::new(&mut entry), role, limits)?;
            declared_word |= declared.0;
            declared_excel |= declared.1;
            package_text = package_text
                .checked_add(declared.2)
                .ok_or(Code::SizeExceeded)?;
            budget(package_text <= limits.maximum_text_characters)?;
        }
    }
    require(
        types
            && if extension == "docx" {
                word && !excel && declared_word && !declared_excel
            } else {
                excel && !word && declared_excel && !declared_word
            },
    )
}
fn xml(
    input: impl BufRead,
    role: &str,
    limits: &VerificationLimits,
) -> BlobResult<(bool, bool, usize)> {
    use quick_xml::{NsReader, events::Event, name::ResolveResult};
    let mut reader = NsReader::from_reader(input);
    let mut buffer = Vec::new();
    let mut depth = 0usize;
    let mut roots = 0usize;
    let mut text = 0usize;
    let mut rows = 0usize;
    let mut columns = 0usize;
    let mut elements = 0usize;
    let mut word = false;
    let mut excel = false;
    let mut body = false;
    let mut declarations = 0usize;
    let mut overrides = BTreeSet::new();
    loop {
        let decoder = reader.decoder();
        let (namespace, event) = reader
            .read_resolved_event_into(&mut buffer)
            .map_err(invalid)?;
        match event {
            Event::Start(ref element) | Event::Empty(ref element) => {
                require(!matches!(namespace, ResolveResult::Unknown(_)))?;
                elements += 1;
                budget(
                    elements
                        <= limits
                            .maximum_spreadsheet_cells
                            .saturating_mul(4)
                            .saturating_add(limits.maximum_package_entries),
                )?;
                if depth == 0 {
                    roots += 1;
                    require(roots == 1)?;
                    let local = element.local_name();
                    let expected = match role {
                        "types" => Some((
                            b"Types".as_slice(),
                            b"http://schemas.openxmlformats.org/package/2006/content-types"
                                .as_slice(),
                        )),
                        "word" => Some((b"document".as_slice(), WORD_NS)),
                        "excel" => Some((b"workbook".as_slice(), EXCEL_NS)),
                        "sheet" => Some((b"worksheet".as_slice(), EXCEL_NS)),
                        _ => None,
                    };
                    if let Some((name, ns)) = expected {
                        require(
                            local.as_ref() == name
                                && matches!(&namespace,ResolveResult::Bound(found) if found.as_ref()==ns),
                        )?;
                    }
                }
                let mut part = None;
                let mut content_type = None;
                for attribute in element.attributes().with_checks(true) {
                    let attribute = attribute.map_err(invalid)?;
                    let name = attribute.key.as_ref();
                    let value = attribute
                        .decoded_and_normalized_value(quick_xml::XmlVersion::Implicit1_0, decoder)
                        .map_err(invalid)?;
                    budget(value.len() <= limits.maximum_expanded_bytes)?;
                    if role == "types" && element.local_name().as_ref() == b"Override" {
                        if name == b"PartName" {
                            part = Some(value.into_owned());
                        } else if name == b"ContentType" {
                            content_type = Some(value.into_owned());
                        }
                    } else if role == "sheet"
                        && name == b"r"
                        && element.local_name().as_ref() == b"c"
                    {
                        let (column, row) = cell(&value)?;
                        rows = rows.max(row);
                        columns = columns.max(column);
                        budget(
                            rows.checked_mul(columns)
                                .is_some_and(|n| n <= limits.maximum_spreadsheet_cells),
                        )?;
                    } else if role == "sheet"
                        && name == b"ref"
                        && element.local_name().as_ref() == b"dimension"
                    {
                        let (a, b) = value.split_once(':').unwrap_or((&value, &value));
                        let (ac, ar) = cell(a)?;
                        let (bc, br) = cell(b)?;
                        require(bc >= ac && br >= ar)?;
                        budget(
                            (bc - ac + 1)
                                .checked_mul(br - ar + 1)
                                .is_some_and(|n| n <= limits.maximum_spreadsheet_cells),
                        )?;
                    }
                }
                if let (Some(part), Some(kind)) = (part, content_type) {
                    require(overrides.insert(part.clone()))?;
                    word |= part == "/word/document.xml"
                        && kind
                            == "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml";
                    excel |= part == "/xl/workbook.xml"
                        && kind
                            == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml";
                }
                body |= role == "word"
                    && depth == 1
                    && element.local_name().as_ref() == b"body"
                    && matches!(&namespace,ResolveResult::Bound(ns) if ns.as_ref()==WORD_NS);
                if matches!(event, Event::Start(_)) {
                    depth += 1;
                    budget(depth <= 128)?;
                }
            }
            Event::End(_) => {
                require(depth > 0)?;
                depth -= 1;
            }
            Event::Text(value) => {
                let content = value.xml10_content().map_err(invalid)?;
                require(depth > 0 || content.trim().is_empty())?;
                text = text
                    .checked_add(content.chars().count())
                    .ok_or(Code::SizeExceeded)?;
                budget(text <= limits.maximum_text_characters)?;
            }
            Event::CData(value) => {
                require(depth > 0)?;
                text = text.checked_add(value.len()).ok_or(Code::SizeExceeded)?;
                budget(text <= limits.maximum_text_characters)?;
            }
            Event::DocType(_) => return Err(Code::FormatNotAllowed.into()),
            Event::GeneralRef(value) => {
                let name = value.decode().map_err(invalid)?;
                require(
                    matches!(name.as_ref(), "amp" | "lt" | "gt" | "quot" | "apos")
                        || value
                            .resolve_char_ref()
                            .map_err(invalid)?
                            .is_some_and(|c| matches!(c, '\t' | '\n' | '\r') || c >= ' '),
                )?;
                text += 1;
                budget(text <= limits.maximum_text_characters)?;
            }
            Event::Eof => {
                require(depth == 0 && roots == 1 && (role != "word" || body))?;
                break;
            }
            Event::Decl(declaration) => {
                declarations += 1;
                require(
                    declarations == 1
                        && depth == 0
                        && roots == 0
                        && declaration.version().map_err(invalid)?.as_ref() == b"1.0",
                )?;
            }
            _ => (),
        }
        buffer.clear();
    }
    Ok((word, excel, text))
}
fn cell(value: &str) -> BlobResult<(usize, usize)> {
    let index = value
        .bytes()
        .position(|b| b.is_ascii_digit())
        .ok_or(Code::FormatNotAllowed)?;
    require(index > 0)?;
    let mut column = 0usize;
    for b in value[..index].bytes() {
        require(b.is_ascii_uppercase())?;
        column = column
            .checked_mul(26)
            .and_then(|n| n.checked_add((b - b'A' + 1) as usize))
            .ok_or(Code::SizeExceeded)?;
    }
    require(value[index..].bytes().all(|b| b.is_ascii_digit()))?;
    let row = value[index..].parse::<usize>().map_err(invalid)?;
    require(row > 0 && column > 0)?;
    Ok((column, row))
}
fn u16le(bytes: &[u8], at: usize) -> BlobResult<u16> {
    Ok(u16::from_le_bytes(
        bytes
            .get(at..at + 2)
            .ok_or(Code::FormatNotAllowed)?
            .try_into()
            .unwrap(),
    ))
}
fn u32le(bytes: &[u8], at: usize) -> BlobResult<u32> {
    Ok(u32::from_le_bytes(
        bytes
            .get(at..at + 4)
            .ok_or(Code::FormatNotAllowed)?
            .try_into()
            .unwrap(),
    ))
}
fn read_u16(input: &mut impl Read) -> BlobResult<u16> {
    let mut bytes = [0; 2];
    input.read_exact(&mut bytes).map_err(invalid)?;
    Ok(u16::from_le_bytes(bytes))
}

fn zip_directory_entries(bytes: &[u8], limits: &VerificationLimits) -> BlobResult<usize> {
    let minimum = bytes.len().saturating_sub(65557);
    let end = (minimum..bytes.len().saturating_sub(21))
        .rev()
        .find(|at| {
            bytes.get(*at..*at + 4) == Some(b"PK\x05\x06")
                && u16le(bytes, *at + 20).is_ok_and(|n| *at + 22 + n as usize == bytes.len())
        })
        .ok_or(Code::FormatNotAllowed)?;
    require(end + 22 + u16le(bytes, end + 20)? as usize == bytes.len())?;
    require(u16le(bytes, end + 4)? == 0 && u16le(bytes, end + 6)? == 0)?;
    let mut count = u16le(bytes, end + 10)? as u64;
    require(u16le(bytes, end + 8)? as u64 == count)?;
    let mut size = u32le(bytes, end + 12)? as u64;
    let mut offset = u32le(bytes, end + 16)? as u64;
    if count == 65535 || size == u32::MAX as u64 || offset == u32::MAX as u64 {
        require(end >= 20 && bytes.get(end - 20..end - 16) == Some(b"PK\x06\x07"))?;
        require(u32le(bytes, end - 16)? == 0 && u32le(bytes, end - 4)? == 1)?;
        let record = u64le(bytes, end - 12)?;
        require(record <= bytes.len().saturating_sub(56) as u64)?;
        let record = record as usize;
        require(
            bytes.get(record..record + 4) == Some(b"PK\x06\x06")
                && u64le(bytes, record + 4)? >= 44
                && u32le(bytes, record + 16)? == 0
                && u32le(bytes, record + 20)? == 0,
        )?;
        count = u64le(bytes, record + 32)?;
        require(u64le(bytes, record + 24)? == count)?;
        size = u64le(bytes, record + 40)?;
        offset = u64le(bytes, record + 48)?;
    }
    budget(count <= limits.maximum_package_entries as u64)?;
    require(count > 0 && offset.checked_add(size).is_some_and(|n| n <= end as u64))?;
    let finish = (offset + size) as usize;
    let mut at = offset as usize;
    let mut names = BTreeSet::new();
    for _ in 0..count {
        require(
            bytes.get(at..at + 4) == Some(b"PK\x01\x02")
                && at.checked_add(46).is_some_and(|n| n <= finish),
        )?;
        let name = u16le(bytes, at + 28)? as usize;
        let extra = u16le(bytes, at + 30)? as usize;
        let comment = u16le(bytes, at + 32)? as usize;
        require(
            name > 0
                && at
                    .checked_add(46 + name + extra + comment)
                    .is_some_and(|n| n <= finish),
        )?;
        require(names.insert(bytes[at + 46..at + 46 + name].to_vec()))?;
        at += 46 + name + extra + comment;
    }
    require(at == finish)?;
    Ok(count as usize)
}
fn u64le(bytes: &[u8], at: usize) -> BlobResult<u64> {
    Ok(u64::from_le_bytes(
        bytes
            .get(at..at + 8)
            .ok_or(Code::FormatNotAllowed)?
            .try_into()
            .unwrap(),
    ))
}
