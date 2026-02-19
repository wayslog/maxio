use http::HeaderMap;

use crate::signature_v4;

/// Parsed presigned URL V4 query parameters
#[derive(Debug, Clone)]
pub struct PresignedV4Params {
    pub access_key: String,
    pub date: String,
    pub region: String,
    pub service: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
    pub date_time: String,
    pub expires: String,
}

/// Check if a query string contains presigned V4 parameters
pub fn is_presigned_v4(query: &str) -> bool {
    query.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256")
}

/// Parse presigned V4 query parameters
pub fn parse_presigned_v4(query: &str) -> Option<PresignedV4Params> {
    let mut algorithm = None;
    let mut credential = None;
    let mut date = None;
    let mut expires = None;
    let mut signed_headers = None;
    let mut signature = None;

    for pair in query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        match key {
            "X-Amz-Algorithm" => algorithm = Some(value),
            "X-Amz-Credential" => credential = Some(value),
            "X-Amz-Date" => date = Some(value),
            "X-Amz-Expires" => expires = Some(value),
            "X-Amz-SignedHeaders" => signed_headers = Some(value),
            "X-Amz-Signature" => signature = Some(value),
            _ => {}
        }
    }

    if algorithm? != "AWS4-HMAC-SHA256" {
        return None;
    }

    let credential = credential?;
    let signature = signature?.to_string();
    let date_time = date?.to_string();
    let expires = expires.unwrap_or("86400").to_string();
    let signed_headers_str = signed_headers?;

    // URL-decode credential (may contain %2F for /)
    let credential = credential.replace("%2F", "/").replace("%2f", "/");

    let scope: Vec<&str> = credential.split('/').collect();
    if scope.len() != 5 || scope[4] != "aws4_request" {
        return None;
    }

    let signed_headers = signed_headers_str.replace("%3B", ";").replace("%3b", ";");
    let signed_headers: Vec<String> = signed_headers
        .split(';')
        .filter(|h| !h.is_empty())
        .map(|h| h.to_ascii_lowercase())
        .collect();

    Some(PresignedV4Params {
        access_key: scope[0].to_string(),
        date: scope[1].to_string(),
        region: scope[2].to_string(),
        service: scope[3].to_string(),
        signed_headers,
        signature,
        date_time,
        expires,
    })
}

/// Verify a presigned V4 URL signature.
/// For presigned URLs, the canonical request uses "UNSIGNED-PAYLOAD" as the payload hash,
/// and the query string includes all X-Amz-* params EXCEPT X-Amz-Signature.
pub fn verify_presigned_v4(
    secret_key: &str,
    method: &str,
    uri: &str,
    query_string: &str,
    headers: &HeaderMap,
    params: &PresignedV4Params,
) -> bool {
    // Build canonical query string WITHOUT X-Amz-Signature.
    // For presigned URLs, we must use the raw query params as-is (already encoded),
    // sorted by key, without double-encoding.
    let filtered_query = build_presigned_canonical_query(query_string);

    let canonical_headers = build_presigned_canonical_headers(headers, &params.signed_headers);
    let Some(canonical_headers) = canonical_headers else {
        return false;
    };

    let canonical_uri = signature_v4::canonical_uri(uri);

    let mut sorted_signed = params.signed_headers.clone();
    sorted_signed.sort();
    let signed_headers_str = sorted_signed.join(";");

    let canonical_request = signature_v4::get_canonical_request(
        method,
        &canonical_uri,
        &filtered_query,
        &canonical_headers,
        &signed_headers_str,
        "UNSIGNED-PAYLOAD",
    );

    let scope = format!("{}/{}/s3/aws4_request", params.date, params.region);
    let string_to_sign =
        signature_v4::get_string_to_sign(&canonical_request, &params.date_time, &scope);
    let signing_key = signature_v4::get_signing_key(secret_key, &params.date, &params.region);
    let computed = signature_v4::get_signature(&signing_key, &string_to_sign);

    constant_time_eq(computed.as_bytes(), params.signature.as_bytes())
}

fn build_presigned_canonical_query(query_string: &str) -> String {
    let mut params: Vec<(&str, &str)> = query_string
        .split('&')
        .filter(|pair| !pair.is_empty() && !pair.starts_with("X-Amz-Signature="))
        .map(|pair| {
            let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
            (name, value)
        })
        .collect();

    params.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));

    params
        .into_iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn build_presigned_canonical_headers(
    headers: &HeaderMap,
    signed_header_names: &[String],
) -> Option<String> {
    let mut names: Vec<String> = signed_header_names
        .iter()
        .map(|n| n.to_ascii_lowercase())
        .collect();
    names.sort();

    let mut out = String::new();
    for name in names {
        let value = headers.get(name.as_str())?;
        let value = value.to_str().ok()?;
        out.push_str(&name);
        out.push(':');
        out.push_str(value.trim());
        out.push('\n');
    }
    Some(out)
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
