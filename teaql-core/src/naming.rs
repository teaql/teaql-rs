pub fn default_table_name(entity_name: &str) -> String {
    let mut out = String::with_capacity(entity_name.len() + 5);
    for (index, ch) in entity_name.chars().enumerate() {
        match ch.is_ascii_uppercase() {
            true => {
                if index > 0 {
                    out.push('_');
                }
                out.push(ch.to_ascii_lowercase());
            }
            false => out.push(ch),
        }
    }
    out.push_str("_data");
    out
}
