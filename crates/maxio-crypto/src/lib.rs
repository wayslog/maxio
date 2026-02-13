pub mod cipher;
pub mod key;

pub use key::MasterKey;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("invalid key length: expected 32 bytes, got {0}")]
    InvalidKeyLength(usize),
    #[error("invalid ciphertext: {0}")]
    InvalidCiphertext(&'static str),
    #[error("encryption failure")]
    Encrypt,
    #[error("decryption failure")]
    Decrypt,
    #[error("key derivation failure")]
    KeyDerivation,
}

pub type Result<T> = std::result::Result<T, CryptoError>;
