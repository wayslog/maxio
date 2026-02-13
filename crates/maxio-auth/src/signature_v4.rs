use hmac::{Hmac, Mac};
use http::HeaderMap;
use percent_encoding::{AsciiSet, CONTROLS, utf8_percent_encode};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

const AWS_URI_ENCODE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b'/')
    .add(b':')
    .add(b';')
    .add(b'=')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b']');

pub fn get_signing_key(secret_key: &str, date: &str, region: &str) -> Vec<u8> {
    let date_key = hmac_sha256(format!("AWS4{secret_key}").as_bytes(), date.as_bytes());
    let region_key = hmac_sha256(&date_key, region.as_bytes());
    let service_key = hmac_sha256(&region_key, b"s3");
    hmac_sha256(&service_key, b"aws4_request")
}

pub fn get_canonical_request(
    method: &str,
    uri: &str,
    query_string: &str,
    canonical_headers: &str,
    signed_headers: &str,
    payload_hash: &str,
) -> String {
    format!(
        "{method}\n{uri}\n{query_string}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    )
}

pub fn get_string_to_sign(canonical_request: &str, date_time: &str, scope: &str) -> String {
    let canonical_hash = sha256_hex(canonical_request.as_bytes());
    format!("AWS4-HMAC-SHA256\n{date_time}\n{scope}\n{canonical_hash}")
}

pub fn get_signature(signing_key: &[u8], string_to_sign: &str) -> String {
    hex::encode(hmac_sha256(signing_key, string_to_sign.as_bytes()))
}

pub fn verify_signature(
    secret_key: &str,
    method: &str,
    uri: &str,
    query_string: &str,
    headers: &HeaderMap,
    signed_header_names: &[String],
    payload_hash: &str,
    date_time: &str,
    date: &str,
    region: &str,
    signature: &str,
) -> bool {
    let canonical_headers = match canonical_headers(headers, signed_header_names) {
        Some(v) => v,
        None => return false,
    };

    let canonical_uri = canonical_uri(uri);
    let canonical_query = canonical_query_string(query_string);
    let signed_headers = signed_header_names
        .iter()
        .map(|h| h.to_ascii_lowercase())
        .collect::<Vec<_>>();

    let mut sorted_signed_headers = signed_headers;
    sorted_signed_headers.sort();
    let signed_headers = sorted_signed_headers.join(";");

    let canonical_request = get_canonical_request(
        method,
        &canonical_uri,
        &canonical_query,
        &canonical_headers,
        &signed_headers,
        payload_hash,
    );

    let scope = format!("{date}/{region}/s3/aws4_request");
    let string_to_sign = get_string_to_sign(&canonical_request, date_time, &scope);
    let signing_key = get_signing_key(secret_key, date, region);
    let computed = get_signature(&signing_key, &string_to_sign);
    constant_time_eq(computed.as_bytes(), signature.as_bytes())
}

pub fn canonical_uri(path: &str) -> String {
    if path.is_empty() {
        return "/".to_string();
    }

    let starts_with_slash = path.starts_with('/');
    let ends_with_slash = path.ends_with('/');
    let encoded_segments = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(percent_encode)
        .collect::<Vec<_>>();

    let mut out = String::new();
    if starts_with_slash {
        out.push('/');
    }
    out.push_str(&encoded_segments.join("/"));
    if ends_with_slash && !out.ends_with('/') {
        out.push('/');
    }
    if out.is_empty() { "/".to_string() } else { out }
}

pub fn canonical_query_string(query_string: &str) -> String {
    let mut params = query_string
        .split('&')
        .filter(|pair| !pair.is_empty())
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (percent_encode(name), percent_encode(value))
        })
        .collect::<Vec<_>>();

    params.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    params
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = match HmacSha256::new_from_slice(key) {
        Ok(mac) => mac,
        Err(_) => return Vec::new(),
    };
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

fn canonical_headers(headers: &HeaderMap, signed_header_names: &[String]) -> Option<String> {
    let mut names = signed_header_names
        .iter()
        .map(|name| name.to_ascii_lowercase())
        .collect::<Vec<_>>();
    names.sort();

    let mut out = String::new();
    for name in names {
        let value = headers.get(name.as_str())?;
        let value = value.to_str().ok()?;
        out.push_str(&name);
        out.push(':');
        out.push_str(&normalize_header_value(value));
        out.push('\n');
    }
    Some(out)
}

fn normalize_header_value(value: &str) -> String {
    let mut out = String::new();
    let mut in_whitespace = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_whitespace() {
            in_whitespace = true;
            continue;
        }
        if in_whitespace && !out.is_empty() {
            out.push(' ');
        }
        in_whitespace = false;
        out.push(ch);
    }
    out
}

fn percent_encode(value: &str) -> String {
    utf8_percent_encode(value, AWS_URI_ENCODE_SET).to_string()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let mut diff = a.len() ^ b.len();
    let max_len = a.len().max(b.len());

    for i in 0..max_len {
        let left = *a.get(i).unwrap_or(&0);
        let right = *b.get(i).unwrap_or(&0);
        diff |= usize::from(left ^ right);
    }

    diff == 0
}
