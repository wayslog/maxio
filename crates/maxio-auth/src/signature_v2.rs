use hmac::{Hmac, Mac};
use http::HeaderMap;
use sha1::Sha1;

type HmacSha1 = Hmac<Sha1>;

pub fn is_signature_v2(auth_header: &str) -> bool {
    auth_header.starts_with("AWS ")
}

#[derive(Debug, Clone)]
pub struct ParsedV2Auth {
    pub access_key: String,
    pub signature: String,
}

pub fn parse_v2_auth(auth_header: &str) -> Option<ParsedV2Auth> {
    let rest = auth_header.strip_prefix("AWS ")?;
    let (access_key, signature) = rest.split_once(':')?;
    if access_key.is_empty() || signature.is_empty() {
        return None;
    }
    Some(ParsedV2Auth {
        access_key: access_key.to_string(),
        signature: signature.to_string(),
    })
}

pub fn verify_signature_v2(
    secret_key: &str,
    method: &str,
    path: &str,
    headers: &HeaderMap,
    parsed: &ParsedV2Auth,
) -> bool {
    let content_md5 = header_value(headers, "content-md5");
    let content_type = header_value(headers, "content-type");

    let date = header_value(headers, "x-amz-date");
    let date = if date.is_empty() {
        header_value(headers, "date")
    } else {
        date
    };

    let amz_headers = canonicalized_amz_headers(headers);
    let resource = canonicalized_resource(path);

    let string_to_sign =
        format!("{method}\n{content_md5}\n{content_type}\n{date}\n{amz_headers}{resource}");

    let computed = sign_v2(secret_key, &string_to_sign);
    constant_time_eq(computed.as_bytes(), parsed.signature.as_bytes())
}

fn sign_v2(secret_key: &str, string_to_sign: &str) -> String {
    use base64::Engine;
    let mut mac = match HmacSha1::new_from_slice(secret_key.as_bytes()) {
        Ok(mac) => mac,
        Err(_) => return String::new(),
    };
    mac.update(string_to_sign.as_bytes());
    let result = mac.finalize().into_bytes();
    base64::engine::general_purpose::STANDARD.encode(result)
}

fn header_value(headers: &HeaderMap, name: &str) -> String {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string()
}

fn canonicalized_amz_headers(headers: &HeaderMap) -> String {
    let mut amz_headers: Vec<(String, String)> = Vec::new();

    for (name, value) in headers.iter() {
        let name_lower = name.as_str().to_ascii_lowercase();
        if name_lower.starts_with("x-amz-") {
            let val = value.to_str().unwrap_or("").trim().to_string();
            amz_headers.push((name_lower, val));
        }
    }

    amz_headers.sort_by(|a, b| a.0.cmp(&b.0));

    let mut result = String::new();
    for (name, value) in amz_headers {
        result.push_str(&name);
        result.push(':');
        result.push_str(&value);
        result.push('\n');
    }
    result
}

fn canonicalized_resource(path: &str) -> String {
    if path.is_empty() {
        "/".to_string()
    } else {
        path.to_string()
    }
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
