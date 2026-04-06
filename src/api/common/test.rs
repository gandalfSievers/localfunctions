use super::*;

// -- X-Ray Trace ID tests ----------------------------------------------------

#[test]
fn generate_xray_trace_id_has_correct_format() {
    let trace_id = generate_xray_trace_id();

    // Must start with "Root=1-"
    assert!(trace_id.starts_with("Root=1-"), "trace_id: {}", trace_id);
    // Must contain ";Parent=" and ";Sampled=1"
    assert!(trace_id.contains(";Parent="), "trace_id: {}", trace_id);
    assert!(trace_id.ends_with(";Sampled=1"), "trace_id: {}", trace_id);

    // Parse out the components
    let root_part = trace_id.split(";Parent=").next().unwrap();
    let root_value = root_part.strip_prefix("Root=1-").unwrap();
    let parts: Vec<&str> = root_value.split('-').collect();
    assert_eq!(parts.len(), 2, "Root should have timestamp-id: {}", root_value);

    // Timestamp: 8 hex chars
    assert_eq!(parts[0].len(), 8, "timestamp hex: {}", parts[0]);
    assert!(u32::from_str_radix(parts[0], 16).is_ok());

    // Root ID: 24 hex chars (96 bits)
    assert_eq!(parts[1].len(), 24, "root id hex: {}", parts[1]);
    assert!(u128::from_str_radix(parts[1], 16).is_ok());

    // Parent ID: 16 hex chars (64 bits)
    let parent_part = trace_id
        .split(";Parent=")
        .nth(1)
        .unwrap()
        .split(";Sampled=")
        .next()
        .unwrap();
    assert_eq!(parent_part.len(), 16, "parent id hex: {}", parent_part);
    assert!(u64::from_str_radix(parent_part, 16).is_ok());
}

#[test]
fn generate_xray_trace_id_is_unique() {
    let id1 = generate_xray_trace_id();
    let id2 = generate_xray_trace_id();
    assert_ne!(id1, id2);
}

// -- Qualifier parsing tests -------------------------------------------------

#[test]
fn parse_qualifier_no_qualifier() {
    let (name, qual) = parse_qualifier("my-func", None);
    assert_eq!(name, "my-func");
    assert_eq!(qual, None);
}

#[test]
fn parse_qualifier_inline_colon() {
    let (name, qual) = parse_qualifier("my-func:PROD", None);
    assert_eq!(name, "my-func");
    assert_eq!(qual, Some("PROD".into()));
}

#[test]
fn parse_qualifier_query_param() {
    let (name, qual) = parse_qualifier("my-func", Some("STAGING"));
    assert_eq!(name, "my-func");
    assert_eq!(qual, Some("STAGING".into()));
}

#[test]
fn parse_qualifier_query_param_overrides_inline() {
    let (name, qual) = parse_qualifier("my-func:DEV", Some("PROD"));
    assert_eq!(name, "my-func");
    assert_eq!(qual, Some("PROD".into()));
}

#[test]
fn parse_qualifier_dollar_latest() {
    let (name, qual) = parse_qualifier("my-func:$LATEST", None);
    assert_eq!(name, "my-func");
    assert_eq!(qual, Some("$LATEST".into()));
}

#[test]
fn validate_qualifier_accepts_none() {
    assert!(validate_qualifier(&None, "my-func").is_ok());
}

#[test]
fn validate_qualifier_accepts_latest() {
    assert!(validate_qualifier(&Some("$LATEST".into()), "my-func").is_ok());
}

#[test]
fn validate_qualifier_rejects_unknown() {
    let result = validate_qualifier(&Some("PROD".into()), "my-func");
    assert!(result.is_err());
}

// -- Client context validation tests -----------------------------------------

#[test]
fn validate_client_context_accepts_valid() {
    use base64::Engine;
    let json = r#"{"env":"test"}"#;
    let encoded = base64::engine::general_purpose::STANDARD.encode(json);
    assert!(validate_client_context(&encoded).is_ok());
}

#[test]
fn validate_client_context_rejects_invalid_base64() {
    let result = validate_client_context("not-valid-base64!!!");
    assert!(result.is_err());
}

#[test]
fn validate_client_context_rejects_non_json() {
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode("not json");
    let result = validate_client_context(&encoded);
    assert!(result.is_err());
}

#[test]
fn validate_client_context_rejects_oversized() {
    use base64::Engine;
    // Create a JSON payload that exceeds MAX_CLIENT_CONTEXT_BYTES when decoded
    let big_value = "x".repeat(MAX_CLIENT_CONTEXT_BYTES + 100);
    let json = format!(r#"{{"data":"{}"}}"#, big_value);
    let encoded = base64::engine::general_purpose::STANDARD.encode(&json);
    let result = validate_client_context(&encoded);
    assert!(result.is_err());
}

// -- epoch_days_to_ymd tests -------------------------------------------------

#[test]
fn epoch_days_to_ymd_known_date() {
    // 2024-01-01 = day 19723 since epoch
    let (y, m, d) = epoch_days_to_ymd(19723);
    assert_eq!(y, 2024);
    assert_eq!(m, 1);
    assert_eq!(d, 1);
}

#[test]
fn epoch_days_to_ymd_unix_epoch() {
    // Day 0 = 1970-01-01
    let (y, m, d) = epoch_days_to_ymd(0);
    assert_eq!(y, 1970);
    assert_eq!(m, 1);
    assert_eq!(d, 1);
}
