use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use rand::Rng;
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::config::KmsConfig;
use crate::error::{KmsError, Result};

const NONCE_SIZE: usize = 12;
const DEK_SIZE: usize = 32;

pub struct KmsClient {
    config: KmsConfig,
    client: Client,
}

#[derive(Debug, Serialize)]
struct GenerateKeyRequest {
    context: String,
}

#[derive(Debug, Deserialize)]
struct GenerateKeyResponse {
    plaintext: String,
    ciphertext: String,
}

#[derive(Debug, Serialize)]
struct DecryptRequest {
    ciphertext: String,
    context: String,
}

#[derive(Debug, Deserialize)]
struct DecryptResponse {
    plaintext: String,
}

#[derive(Debug, Clone)]
pub struct DataEncryptionKey {
    pub plaintext: [u8; DEK_SIZE],
    pub ciphertext: Vec<u8>,
}

impl KmsClient {
    pub fn new(config: KmsConfig) -> Result<Self> {
        if !config.is_configured() {
            return Err(KmsError::NotConfigured);
        }

        let client = Client::builder()
            .danger_accept_invalid_certs(config.tls_skip_verify)
            .build()
            .map_err(|e| KmsError::ConnectionFailed(e.to_string()))?;

        Ok(Self { config, client })
    }

    pub async fn generate_dek(&self, context: &str) -> Result<DataEncryptionKey> {
        let url = format!("{}/v1/key/generate/{}", self.config.endpoint, self.config.key_id);
        
        let request = GenerateKeyRequest {
            context: BASE64.encode(context.as_bytes()),
        };

        let mut req = self.client.post(&url).json(&request);
        
        if let Some(creds) = &self.config.credentials {
            req = req.basic_auth(&creds.access_key, Some(&creds.secret_key));
        }

        let response = req
            .send()
            .await
            .map_err(|e| KmsError::ConnectionFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(KmsError::EncryptionFailed(format!(
                "KMS returned status: {}",
                response.status()
            )));
        }

        let resp: GenerateKeyResponse = response
            .json()
            .await
            .map_err(|e| KmsError::InvalidResponse(e.to_string()))?;

        let plaintext_bytes = BASE64
            .decode(&resp.plaintext)
            .map_err(|e| KmsError::InvalidResponse(format!("invalid plaintext: {e}")))?;

        let ciphertext_bytes = BASE64
            .decode(&resp.ciphertext)
            .map_err(|e| KmsError::InvalidResponse(format!("invalid ciphertext: {e}")))?;

        if plaintext_bytes.len() != DEK_SIZE {
            return Err(KmsError::InvalidResponse(format!(
                "invalid DEK size: expected {}, got {}",
                DEK_SIZE,
                plaintext_bytes.len()
            )));
        }

        let mut plaintext = [0u8; DEK_SIZE];
        plaintext.copy_from_slice(&plaintext_bytes);

        Ok(DataEncryptionKey {
            plaintext,
            ciphertext: ciphertext_bytes,
        })
    }

    pub async fn decrypt_dek(&self, ciphertext: &[u8], context: &str) -> Result<[u8; DEK_SIZE]> {
        let url = format!("{}/v1/key/decrypt/{}", self.config.endpoint, self.config.key_id);

        let request = DecryptRequest {
            ciphertext: BASE64.encode(ciphertext),
            context: BASE64.encode(context.as_bytes()),
        };

        let mut req = self.client.post(&url).json(&request);

        if let Some(creds) = &self.config.credentials {
            req = req.basic_auth(&creds.access_key, Some(&creds.secret_key));
        }

        let response = req
            .send()
            .await
            .map_err(|e| KmsError::ConnectionFailed(e.to_string()))?;

        if !response.status().is_success() {
            return Err(KmsError::DecryptionFailed(format!(
                "KMS returned status: {}",
                response.status()
            )));
        }

        let resp: DecryptResponse = response
            .json()
            .await
            .map_err(|e| KmsError::InvalidResponse(e.to_string()))?;

        let plaintext_bytes = BASE64
            .decode(&resp.plaintext)
            .map_err(|e| KmsError::InvalidResponse(format!("invalid plaintext: {e}")))?;

        if plaintext_bytes.len() != DEK_SIZE {
            return Err(KmsError::InvalidResponse(format!(
                "invalid DEK size: expected {}, got {}",
                DEK_SIZE,
                plaintext_bytes.len()
            )));
        }

        let mut plaintext = [0u8; DEK_SIZE];
        plaintext.copy_from_slice(&plaintext_bytes);

        Ok(plaintext)
    }

    pub fn encrypt_with_dek(dek: &[u8; DEK_SIZE], plaintext: &[u8]) -> Result<Vec<u8>> {
        let cipher = Aes256Gcm::new_from_slice(dek)
            .map_err(|_| KmsError::EncryptionFailed("invalid key".to_string()))?;

        let mut nonce_bytes = [0u8; NONCE_SIZE];
        rand::rng().fill(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, plaintext)
            .map_err(|_| KmsError::EncryptionFailed("encryption failed".to_string()))?;

        let mut output = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
        output.extend_from_slice(&nonce_bytes);
        output.extend_from_slice(&ciphertext);

        Ok(output)
    }

    pub fn decrypt_with_dek(dek: &[u8; DEK_SIZE], ciphertext: &[u8]) -> Result<Vec<u8>> {
        if ciphertext.len() < NONCE_SIZE {
            return Err(KmsError::DecryptionFailed("ciphertext too short".to_string()));
        }

        let cipher = Aes256Gcm::new_from_slice(dek)
            .map_err(|_| KmsError::DecryptionFailed("invalid key".to_string()))?;

        let (nonce_bytes, encrypted) = ciphertext.split_at(NONCE_SIZE);
        let nonce = Nonce::from_slice(nonce_bytes);

        cipher
            .decrypt(nonce, encrypted)
            .map_err(|_| KmsError::DecryptionFailed("decryption failed".to_string()))
    }
}
