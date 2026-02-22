use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use bytes::Bytes;
use chrono::Utc;
use reqwest::Client;
use sha2::Sha256;

use super::config::{TierConfig, TierTarget};
use maxio_common::error::{MaxioError, Result};

pub struct TierClient {
    config: TierConfig,
    client: Client,
}

impl TierClient {
    pub fn new(config: TierConfig) -> Result<Self> {
        let client = Client::builder()
            .build()
            .map_err(|e| MaxioError::InternalError(format!("Failed to create tier client: {e}")))?;

        Ok(Self { config, client })
    }

    pub fn tier_name(&self) -> &str {
        &self.config.name
    }

    pub async fn put_object(&self, key: &str, data: Bytes) -> Result<()> {
        match &self.config.target {
            TierTarget::S3 {
                endpoint,
                bucket,
                region: _,
                access_key: _,
                secret_key: _,
                use_ssl,
            } => {
                let scheme = if *use_ssl { "https" } else { "http" };
                let url = format!(
                    "{}://{}/{}/{}{}",
                    scheme, endpoint, bucket, self.config.prefix, key
                );

                let _response = self
                    .client
                    .put(&url)
                    .body(data.to_vec())
                    .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
                    .send()
                    .await
                    .map_err(|e| MaxioError::InternalError(format!("Tier PUT failed: {e}")))?;

                Ok(())
            }
            TierTarget::Azure {
                account_name,
                account_key,
                container,
                endpoint,
            } => {
                self.azure_put_object(account_name, account_key, container, endpoint.as_deref(), key, data)
                    .await
            }
            TierTarget::Gcs {
                bucket,
                credentials_json,
                prefix,
            } => {
                self.gcs_put_object(bucket, credentials_json, prefix, key, data)
                    .await
            }
        }
    }

    pub async fn get_object(&self, key: &str) -> Result<Bytes> {
        match &self.config.target {
            TierTarget::S3 {
                endpoint,
                bucket,
                use_ssl,
                ..
            } => {
                let scheme = if *use_ssl { "https" } else { "http" };
                let url = format!(
                    "{}://{}/{}/{}{}",
                    scheme, endpoint, bucket, self.config.prefix, key
                );

                let response = self
                    .client
                    .get(&url)
                    .send()
                    .await
                    .map_err(|e| MaxioError::InternalError(format!("Tier GET failed: {e}")))?;

                if !response.status().is_success() {
                    return Err(MaxioError::ObjectNotFound {
                        bucket: bucket.clone(),
                        key: key.to_string(),
                    });
                }

                let bytes = response
                    .bytes()
                    .await
                    .map_err(|e| MaxioError::InternalError(format!("Failed to read tier response: {e}")))?;

                Ok(bytes)
            }
            TierTarget::Azure {
                account_name,
                account_key,
                container,
                endpoint,
            } => {
                self.azure_get_object(account_name, account_key, container, endpoint.as_deref(), key)
                    .await
            }
            TierTarget::Gcs {
                bucket,
                credentials_json,
                prefix,
            } => {
                self.gcs_get_object(bucket, credentials_json, prefix, key)
                    .await
            }
        }
    }

    pub async fn delete_object(&self, key: &str) -> Result<()> {
        match &self.config.target {
            TierTarget::S3 {
                endpoint,
                bucket,
                use_ssl,
                ..
            } => {
                let scheme = if *use_ssl { "https" } else { "http" };
                let url = format!(
                    "{}://{}/{}/{}{}",
                    scheme, endpoint, bucket, self.config.prefix, key
                );

                let _response = self
                    .client
                    .delete(&url)
                    .send()
                    .await
                    .map_err(|e| MaxioError::InternalError(format!("Tier DELETE failed: {e}")))?;

                Ok(())
            }
            TierTarget::Azure {
                account_name,
                account_key,
                container,
                endpoint,
            } => {
                self.azure_delete_object(account_name, account_key, container, endpoint.as_deref(), key)
                    .await
            }
            TierTarget::Gcs {
                bucket,
                credentials_json,
                prefix,
            } => {
                self.gcs_delete_object(bucket, credentials_json, prefix, key)
                    .await
            }
        }
    }

    async fn azure_put_object(
        &self,
        account_name: &str,
        account_key: &str,
        container: &str,
        endpoint: Option<&str>,
        key: &str,
        data: Bytes,
    ) -> Result<()> {
        let blob_name = format!("{}{}", self.config.prefix, key);
        let default_host = format!("{}.blob.core.windows.net", account_name);
        let host = endpoint.unwrap_or(&default_host);
        let url = format!("https://{}/{}/{}", host, container, blob_name);

        let date = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
        let content_length = data.len();

        let string_to_sign = format!(
            "PUT\n\n\n{}\n\napplication/octet-stream\n\n\n\n\n\nx-ms-blob-type:BlockBlob\nx-ms-date:{}\nx-ms-version:2020-10-02\n/{}/{}/{}",
            content_length, date, account_name, container, blob_name
        );

        let signature = self.azure_sign(account_key, &string_to_sign)?;
        let auth_header = format!("SharedKey {}:{}", account_name, signature);

        let response = self
            .client
            .put(&url)
            .header("Authorization", auth_header)
            .header("x-ms-date", &date)
            .header("x-ms-version", "2020-10-02")
            .header("x-ms-blob-type", "BlockBlob")
            .header("Content-Type", "application/octet-stream")
            .header("Content-Length", content_length.to_string())
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| MaxioError::InternalError(format!("Azure PUT failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(MaxioError::InternalError(format!(
                "Azure PUT failed with status {}: {}",
                status, body
            )));
        }

        Ok(())
    }

    async fn azure_get_object(
        &self,
        account_name: &str,
        account_key: &str,
        container: &str,
        endpoint: Option<&str>,
        key: &str,
    ) -> Result<Bytes> {
        let blob_name = format!("{}{}", self.config.prefix, key);
        let default_host = format!("{}.blob.core.windows.net", account_name);
        let host = endpoint.unwrap_or(&default_host);
        let url = format!("https://{}/{}/{}", host, container, blob_name);

        let date = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();

        let string_to_sign = format!(
            "GET\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{}\nx-ms-version:2020-10-02\n/{}/{}/{}",
            date, account_name, container, blob_name
        );

        let signature = self.azure_sign(account_key, &string_to_sign)?;
        let auth_header = format!("SharedKey {}:{}", account_name, signature);

        let response = self
            .client
            .get(&url)
            .header("Authorization", auth_header)
            .header("x-ms-date", &date)
            .header("x-ms-version", "2020-10-02")
            .send()
            .await
            .map_err(|e| MaxioError::InternalError(format!("Azure GET failed: {e}")))?;

        if !response.status().is_success() {
            return Err(MaxioError::ObjectNotFound {
                bucket: container.to_string(),
                key: key.to_string(),
            });
        }

        response
            .bytes()
            .await
            .map_err(|e| MaxioError::InternalError(format!("Failed to read Azure response: {e}")))
    }

    async fn azure_delete_object(
        &self,
        account_name: &str,
        account_key: &str,
        container: &str,
        endpoint: Option<&str>,
        key: &str,
    ) -> Result<()> {
        let blob_name = format!("{}{}", self.config.prefix, key);
        let default_host = format!("{}.blob.core.windows.net", account_name);
        let host = endpoint.unwrap_or(&default_host);
        let url = format!("https://{}/{}/{}", host, container, blob_name);

        let date = Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();

        let string_to_sign = format!(
            "DELETE\n\n\n\n\n\n\n\n\n\n\nx-ms-date:{}\nx-ms-version:2020-10-02\n/{}/{}/{}",
            date, account_name, container, blob_name
        );

        let signature = self.azure_sign(account_key, &string_to_sign)?;
        let auth_header = format!("SharedKey {}:{}", account_name, signature);

        let _response = self
            .client
            .delete(&url)
            .header("Authorization", auth_header)
            .header("x-ms-date", &date)
            .header("x-ms-version", "2020-10-02")
            .send()
            .await
            .map_err(|e| MaxioError::InternalError(format!("Azure DELETE failed: {e}")))?;

        Ok(())
    }

    fn azure_sign(&self, account_key: &str, string_to_sign: &str) -> Result<String> {
        use hmac::{Hmac, Mac};

        let key_bytes = BASE64
            .decode(account_key)
            .map_err(|e| MaxioError::InternalError(format!("Invalid Azure account key: {e}")))?;

        let mut mac = Hmac::<Sha256>::new_from_slice(&key_bytes)
            .map_err(|e| MaxioError::InternalError(format!("HMAC error: {e}")))?;
        mac.update(string_to_sign.as_bytes());

        Ok(BASE64.encode(mac.finalize().into_bytes()))
    }

    async fn gcs_put_object(
        &self,
        bucket: &str,
        _credentials_json: &str,
        prefix: &str,
        key: &str,
        data: Bytes,
    ) -> Result<()> {
        let object_name = format!("{}{}{}", prefix, self.config.prefix, key);
        let encoded_name = urlencoding::encode(&object_name);
        let url = format!(
            "https://storage.googleapis.com/upload/storage/v1/b/{}/o?uploadType=media&name={}",
            bucket, encoded_name
        );

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/octet-stream")
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| MaxioError::InternalError(format!("GCS PUT failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(MaxioError::InternalError(format!(
                "GCS PUT failed with status {}: {}",
                status, body
            )));
        }

        Ok(())
    }

    async fn gcs_get_object(
        &self,
        bucket: &str,
        _credentials_json: &str,
        prefix: &str,
        key: &str,
    ) -> Result<Bytes> {
        let object_name = format!("{}{}{}", prefix, self.config.prefix, key);
        let encoded_name = urlencoding::encode(&object_name);
        let url = format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o/{}?alt=media",
            bucket, encoded_name
        );

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .map_err(|e| MaxioError::InternalError(format!("GCS GET failed: {e}")))?;

        if !response.status().is_success() {
            return Err(MaxioError::ObjectNotFound {
                bucket: bucket.to_string(),
                key: key.to_string(),
            });
        }

        response
            .bytes()
            .await
            .map_err(|e| MaxioError::InternalError(format!("Failed to read GCS response: {e}")))
    }

    async fn gcs_delete_object(
        &self,
        bucket: &str,
        _credentials_json: &str,
        prefix: &str,
        key: &str,
    ) -> Result<()> {
        let object_name = format!("{}{}{}", prefix, self.config.prefix, key);
        let encoded_name = urlencoding::encode(&object_name);
        let url = format!(
            "https://storage.googleapis.com/storage/v1/b/{}/o/{}",
            bucket, encoded_name
        );

        let _response = self
            .client
            .delete(&url)
            .send()
            .await
            .map_err(|e| MaxioError::InternalError(format!("GCS DELETE failed: {e}")))?;

        Ok(())
    }
}
