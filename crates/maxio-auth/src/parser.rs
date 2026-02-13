use thiserror::Error;

pub type Result<T> = std::result::Result<T, ParseError>;

#[derive(Debug, Clone)]
pub struct ParsedAuthHeader {
    pub access_key: String,
    pub date: String,
    pub region: String,
    pub service: String,
    pub signed_headers: Vec<String>,
    pub signature: String,
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("unsupported authorization algorithm")]
    UnsupportedAlgorithm,
    #[error("missing {0} field")]
    MissingField(&'static str),
    #[error("invalid credential scope")]
    InvalidCredentialScope,
    #[error("invalid authorization header format")]
    InvalidFormat,
}

pub fn parse_auth_header(auth_header: &str) -> Result<ParsedAuthHeader> {
    let prefix = "AWS4-HMAC-SHA256 ";
    let parts = auth_header
        .strip_prefix(prefix)
        .ok_or(ParseError::UnsupportedAlgorithm)?;

    let mut credential = None;
    let mut signed_headers = None;
    let mut signature = None;

    for part in parts.split(',').map(str::trim) {
        let (key, value) = part.split_once('=').ok_or(ParseError::InvalidFormat)?;
        match key {
            "Credential" => credential = Some(value.trim().to_string()),
            "SignedHeaders" => signed_headers = Some(value.trim().to_string()),
            "Signature" => signature = Some(value.trim().to_string()),
            _ => {}
        }
    }

    let credential = credential.ok_or(ParseError::MissingField("Credential"))?;
    let signature = signature.ok_or(ParseError::MissingField("Signature"))?;
    let signed_headers = signed_headers.ok_or(ParseError::MissingField("SignedHeaders"))?;

    let scope: Vec<&str> = credential.split('/').collect();
    if scope.len() != 5 || scope[4] != "aws4_request" {
        return Err(ParseError::InvalidCredentialScope);
    }

    let signed_headers = signed_headers
        .split(';')
        .map(str::trim)
        .filter(|h| !h.is_empty())
        .map(|h| h.to_ascii_lowercase())
        .collect::<Vec<_>>();

    if signed_headers.is_empty() {
        return Err(ParseError::MissingField("SignedHeaders"));
    }

    Ok(ParsedAuthHeader {
        access_key: scope[0].to_string(),
        date: scope[1].to_string(),
        region: scope[2].to_string(),
        service: scope[3].to_string(),
        signed_headers,
        signature,
    })
}
