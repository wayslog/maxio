use chrono::Utc;
use maxio_auth::signature_v4::{
    canonical_query_string, canonical_uri, get_canonical_request, get_signature, get_signing_key,
    get_string_to_sign,
};
use maxio_common::error::{MaxioError, Result};
use sha2::{Digest, Sha256};
use url::Url;

use super::types::{ReplicateObjectInfo, ReplicationTarget};

#[derive(Debug, Clone)]
pub struct ReplicationWorker {
    client: reqwest::Client,
}

impl ReplicationWorker {
    pub fn new(client: reqwest::Client) -> Self {
        Self { client }
    }

    pub async fn replicate_object(
        &self,
        info: &ReplicateObjectInfo,
        target: &ReplicationTarget,
    ) -> Result<()> {
        let mut object_url =
            build_target_object_url(&target.endpoint, &target.bucket, &info.object)?;
        if let Some(version_id) = info.version_id.as_deref()
            && !version_id.is_empty()
        {
            object_url
                .query_pairs_mut()
                .append_pair("versionId", version_id);
        }
        let host = host_header_value(&object_url)?;

        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let short_date = now.format("%Y%m%d").to_string();
        let region = if target.region.is_empty() {
            "us-east-1"
        } else {
            target.region.as_str()
        };

        let payload_hash = sha256_hex(&info.body);
        let canonical_uri = canonical_uri(object_url.path());
        let canonical_query = object_url
            .query()
            .map(canonical_query_string)
            .unwrap_or_default();

        let mut canonical_header_pairs = vec![
            ("host".to_string(), host.clone()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
        ];
        if let Some(token) = target.session_token.as_deref()
            && !token.is_empty()
        {
            canonical_header_pairs.push(("x-amz-security-token".to_string(), token.to_string()));
        }
        canonical_header_pairs.sort_by(|left, right| left.0.cmp(&right.0));

        let canonical_headers = canonical_header_pairs
            .iter()
            .map(|(key, value)| format!("{key}:{value}\n"))
            .collect::<String>();
        let signed_headers = canonical_header_pairs
            .iter()
            .map(|(key, _)| key.as_str())
            .collect::<Vec<_>>()
            .join(";");

        let canonical_request = get_canonical_request(
            "PUT",
            &canonical_uri,
            &canonical_query,
            &canonical_headers,
            &signed_headers,
            &payload_hash,
        );

        let scope = format!("{short_date}/{region}/s3/aws4_request");
        let string_to_sign = get_string_to_sign(&canonical_request, &amz_date, &scope);
        let signing_key = get_signing_key(&target.secret_key, &short_date, region);
        let signature = get_signature(&signing_key, &string_to_sign);

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            target.access_key, scope, signed_headers, signature
        );

        let mut request = self
            .client
            .put(object_url.clone())
            .header("Host", host)
            .header("x-amz-date", amz_date)
            .header("x-amz-content-sha256", payload_hash)
            .header("Authorization", authorization)
            .body(info.body.clone());

        if let Some(content_type) = info.content_type.as_deref()
            && !content_type.is_empty()
        {
            request = request.header(reqwest::header::CONTENT_TYPE, content_type);
        }
        if let Some(token) = target.session_token.as_deref()
            && !token.is_empty()
        {
            request = request.header("x-amz-security-token", token);
        }

        let response = request.send().await.map_err(|err| {
            MaxioError::InternalError(format!(
                "replication request failed for target {}: {err}",
                target.arn
            ))
        })?;

        if !response.status().is_success() {
            return Err(MaxioError::InternalError(format!(
                "replication PUT failed for target {} with status {}",
                target.arn,
                response.status()
            )));
        }

        Ok(())
    }
}

fn build_target_object_url(endpoint: &str, bucket: &str, object: &str) -> Result<Url> {
    let endpoint = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
        endpoint.to_string()
    } else {
        format!("http://{endpoint}")
    };

    let mut url = Url::parse(&endpoint).map_err(|err| {
        MaxioError::InvalidArgument(format!("invalid replication endpoint: {err}"))
    })?;

    {
        let mut segments = url.path_segments_mut().map_err(|_| {
            MaxioError::InvalidArgument("replication endpoint cannot be a base URL".to_string())
        })?;
        segments.push(bucket);
        for segment in object.split('/') {
            if !segment.is_empty() {
                segments.push(segment);
            }
        }
    }

    Ok(url)
}

fn host_header_value(url: &Url) -> Result<String> {
    let host = url.host_str().ok_or_else(|| {
        MaxioError::InvalidArgument("replication endpoint has no host".to_string())
    })?;

    let value = match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    Ok(value)
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}
