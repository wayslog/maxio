use hkdf::Hkdf;
use rand::{rngs::OsRng, RngCore};
use sha2::{Digest, Sha256};

use crate::{CryptoError, Result};

const MASTER_KEY_SIZE: usize = 32;
const HKDF_SALT: &[u8] = b"maxio-sse-v1";

#[derive(Debug, Clone)]
pub struct MasterKey {
    key: [u8; MASTER_KEY_SIZE],
}

impl MasterKey {
    pub fn generate() -> Self {
        let mut key = [0_u8; MASTER_KEY_SIZE];
        OsRng.fill_bytes(&mut key);
        Self { key }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != MASTER_KEY_SIZE {
            return Err(CryptoError::InvalidKeyLength(bytes.len()));
        }

        let mut key = [0_u8; MASTER_KEY_SIZE];
        key.copy_from_slice(bytes);
        Ok(Self { key })
    }

    pub fn as_bytes(&self) -> &[u8; MASTER_KEY_SIZE] {
        &self.key
    }

    pub fn derive_object_key(&self, bucket: &str, key: &str, version_id: Option<&str>) -> [u8; 32] {
        let mut output = [0_u8; 32];
        let info = format!(
            "bucket={bucket};key={key};version={}",
            version_id.unwrap_or("null")
        );
        let hk = Hkdf::<Sha256>::new(Some(HKDF_SALT), &self.key);

        if hk.expand(info.as_bytes(), &mut output).is_err() {
            let mut fallback = Sha256::new();
            fallback.update(self.key);
            fallback.update(info.as_bytes());
            let digest = fallback.finalize();
            output.copy_from_slice(&digest[..32]);
        }

        output
    }
}
