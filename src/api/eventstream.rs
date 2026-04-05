//! AWS Event Stream binary encoding for Lambda response streaming.
//!
//! Implements the minimal subset of the AWS event stream protocol needed
//! to encode `PayloadChunk`, `InvokeComplete`, and error events for the
//! `InvokeWithResponseStream` API.
//!
//! Wire format per message:
//! ```text
//! [total_len: 4B][headers_len: 4B][prelude_crc: 4B]
//! [headers: variable][payload: variable]
//! [message_crc: 4B]
//! ```
//! All integers are big-endian. CRCs use CRC-32C (Castagnoli).

use bytes::{BufMut, BytesMut};

/// AWS event stream header value type for UTF-8 strings.
const HEADER_TYPE_STRING: u8 = 7;

/// Encode a single event stream message from headers and payload.
fn encode_message(headers: &[(&str, &str)], payload: &[u8]) -> Vec<u8> {
    // Encode headers into a temporary buffer.
    let mut headers_buf = BytesMut::new();
    for (name, value) in headers {
        headers_buf.put_u8(name.len() as u8);
        headers_buf.put_slice(name.as_bytes());
        headers_buf.put_u8(HEADER_TYPE_STRING);
        headers_buf.put_u16(value.len() as u16);
        headers_buf.put_slice(value.as_bytes());
    }

    let headers_len = headers_buf.len() as u32;
    // 4 (total_len) + 4 (headers_len) + 4 (prelude_crc) + headers + payload + 4 (message_crc)
    let total_len = 12 + headers_len + payload.len() as u32 + 4;

    let mut msg = BytesMut::with_capacity(total_len as usize);

    // Prelude: total length + headers length
    msg.put_u32(total_len);
    msg.put_u32(headers_len);

    // Prelude CRC (covers the first 8 bytes)
    let prelude_crc = crc32c::crc32c(&msg[..8]);
    msg.put_u32(prelude_crc);

    // Headers
    msg.put_slice(&headers_buf);

    // Payload
    msg.put_slice(payload);

    // Message CRC (covers everything before this field)
    let message_crc = crc32c::crc32c(&msg);
    msg.put_u32(message_crc);

    msg.to_vec()
}

/// Encode a `PayloadChunk` event containing response data.
pub fn encode_payload_chunk(data: &[u8]) -> Vec<u8> {
    encode_message(
        &[
            (":event-type", "PayloadChunk"),
            (":content-type", "application/octet-stream"),
            (":message-type", "event"),
        ],
        data,
    )
}

/// Encode an `InvokeComplete` event signalling the end of a streaming response.
pub fn encode_invoke_complete(error_code: &str, error_details: &str, log_result: &str) -> Vec<u8> {
    let body = serde_json::json!({
        "ErrorCode": error_code,
        "ErrorDetails": error_details,
        "LogResult": log_result,
    });
    let payload = serde_json::to_vec(&body).unwrap_or_default();

    encode_message(
        &[
            (":event-type", "InvokeComplete"),
            (":content-type", "application/json"),
            (":message-type", "event"),
        ],
        &payload,
    )
}

/// Encode an error event for mid-stream failures.
pub fn encode_error(error_code: &str, error_message: &str) -> Vec<u8> {
    encode_message(
        &[
            (":error-code", error_code),
            (":error-message", error_message),
            (":message-type", "error"),
        ],
        &[],
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Parse the prelude of an event stream message and verify CRCs.
    fn parse_and_verify(msg: &[u8]) -> (u32, u32) {
        assert!(msg.len() >= 16, "message too short");

        let total_len = u32::from_be_bytes(msg[0..4].try_into().unwrap());
        let headers_len = u32::from_be_bytes(msg[4..8].try_into().unwrap());
        let prelude_crc = u32::from_be_bytes(msg[8..12].try_into().unwrap());

        assert_eq!(msg.len(), total_len as usize, "total length mismatch");

        // Verify prelude CRC
        let computed_prelude_crc = crc32c::crc32c(&msg[..8]);
        assert_eq!(prelude_crc, computed_prelude_crc, "prelude CRC mismatch");

        // Verify message CRC
        let message_crc_offset = total_len as usize - 4;
        let message_crc =
            u32::from_be_bytes(msg[message_crc_offset..].try_into().unwrap());
        let computed_message_crc = crc32c::crc32c(&msg[..message_crc_offset]);
        assert_eq!(message_crc, computed_message_crc, "message CRC mismatch");

        (total_len, headers_len)
    }

    /// Extract header key-value pairs from an encoded message.
    fn extract_headers(msg: &[u8]) -> Vec<(String, String)> {
        let headers_len = u32::from_be_bytes(msg[4..8].try_into().unwrap()) as usize;
        let headers_start = 12;
        let headers_end = headers_start + headers_len;
        let mut pos = headers_start;
        let mut headers = Vec::new();

        while pos < headers_end {
            let name_len = msg[pos] as usize;
            pos += 1;
            let name = std::str::from_utf8(&msg[pos..pos + name_len])
                .unwrap()
                .to_string();
            pos += name_len;
            let _value_type = msg[pos];
            pos += 1;
            let value_len = u16::from_be_bytes(msg[pos..pos + 2].try_into().unwrap()) as usize;
            pos += 2;
            let value = std::str::from_utf8(&msg[pos..pos + value_len])
                .unwrap()
                .to_string();
            pos += value_len;
            headers.push((name, value));
        }

        headers
    }

    #[test]
    fn payload_chunk_encodes_valid_event_stream() {
        let data = b"hello streaming world";
        let msg = encode_payload_chunk(data);

        let (total_len, _headers_len) = parse_and_verify(&msg);
        assert!(total_len > 16);

        let headers = extract_headers(&msg);
        assert!(headers.contains(&(":event-type".into(), "PayloadChunk".into())));
        assert!(headers.contains(&(":content-type".into(), "application/octet-stream".into())));
        assert!(headers.contains(&(":message-type".into(), "event".into())));

        // Verify payload is present after headers
        let headers_len = u32::from_be_bytes(msg[4..8].try_into().unwrap()) as usize;
        let payload_start = 12 + headers_len;
        let payload_end = total_len as usize - 4;
        assert_eq!(&msg[payload_start..payload_end], data);
    }

    #[test]
    fn invoke_complete_encodes_valid_event_stream() {
        let msg = encode_invoke_complete("", "", "");

        parse_and_verify(&msg);

        let headers = extract_headers(&msg);
        assert!(headers.contains(&(":event-type".into(), "InvokeComplete".into())));
        assert!(headers.contains(&(":content-type".into(), "application/json".into())));
        assert!(headers.contains(&(":message-type".into(), "event".into())));

        // Verify JSON payload
        let headers_len = u32::from_be_bytes(msg[4..8].try_into().unwrap()) as usize;
        let total_len = u32::from_be_bytes(msg[0..4].try_into().unwrap()) as usize;
        let payload = &msg[12 + headers_len..total_len - 4];
        let json: serde_json::Value = serde_json::from_slice(payload).unwrap();
        assert_eq!(json["ErrorCode"], "");
        assert_eq!(json["ErrorDetails"], "");
        assert_eq!(json["LogResult"], "");
    }

    #[test]
    fn invoke_complete_with_error_info() {
        let msg = encode_invoke_complete("FunctionError", "something broke", "base64logs==");

        parse_and_verify(&msg);

        let headers_len = u32::from_be_bytes(msg[4..8].try_into().unwrap()) as usize;
        let total_len = u32::from_be_bytes(msg[0..4].try_into().unwrap()) as usize;
        let payload = &msg[12 + headers_len..total_len - 4];
        let json: serde_json::Value = serde_json::from_slice(payload).unwrap();
        assert_eq!(json["ErrorCode"], "FunctionError");
        assert_eq!(json["ErrorDetails"], "something broke");
        assert_eq!(json["LogResult"], "base64logs==");
    }

    #[test]
    fn error_event_encodes_valid_event_stream() {
        let msg = encode_error("ServiceException", "container crashed");

        parse_and_verify(&msg);

        let headers = extract_headers(&msg);
        assert!(headers.contains(&(":error-code".into(), "ServiceException".into())));
        assert!(headers.contains(&(":error-message".into(), "container crashed".into())));
        assert!(headers.contains(&(":message-type".into(), "error".into())));

        // Error events have no payload
        let headers_len = u32::from_be_bytes(msg[4..8].try_into().unwrap()) as usize;
        let total_len = u32::from_be_bytes(msg[0..4].try_into().unwrap()) as usize;
        let payload_len = total_len - 12 - headers_len - 4;
        assert_eq!(payload_len, 0);
    }

    #[test]
    fn empty_payload_chunk() {
        let msg = encode_payload_chunk(b"");
        parse_and_verify(&msg);

        let headers_len = u32::from_be_bytes(msg[4..8].try_into().unwrap()) as usize;
        let total_len = u32::from_be_bytes(msg[0..4].try_into().unwrap()) as usize;
        let payload_len = total_len - 12 - headers_len - 4;
        assert_eq!(payload_len, 0);
    }

    #[test]
    fn large_payload_chunk() {
        let data = vec![0xAB; 1024 * 1024]; // 1 MB
        let msg = encode_payload_chunk(&data);
        parse_and_verify(&msg);

        let headers_len = u32::from_be_bytes(msg[4..8].try_into().unwrap()) as usize;
        let total_len = u32::from_be_bytes(msg[0..4].try_into().unwrap()) as usize;
        let payload = &msg[12 + headers_len..total_len - 4];
        assert_eq!(payload.len(), 1024 * 1024);
    }
}
