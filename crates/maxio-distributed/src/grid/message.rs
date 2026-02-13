use serde::{Deserialize, Serialize};

use crate::errors::{GridError, Result};

pub type MuxId = u32;
pub type Seq = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Op {
    Connect,
    Request,
    Response,
    Ping,
    Pong,
    Merged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Flags(pub u8);

impl Flags {
    pub const NONE: Self = Self(0);
    pub const CRC: Self = Self(1 << 0);
    pub const EOF: Self = Self(1 << 1);
    pub const STATELESS: Self = Self(1 << 2);
    pub const SUBROUTE: Self = Self(1 << 3);

    pub fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    pub fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

impl Default for Flags {
    fn default() -> Self {
        Self::NONE
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub mux_id: MuxId,
    pub seq: Seq,
    pub handler: u8,
    pub op: Op,
    pub flags: Flags,
    pub payload: Vec<u8>,
}

impl Message {
    pub fn new(
        mux_id: MuxId,
        seq: Seq,
        handler: u8,
        op: Op,
        flags: Flags,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            mux_id,
            seq,
            handler,
            op,
            flags,
            payload,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        rmp_serde::to_vec(self).map_err(GridError::Encode)
    }

    pub fn decode(raw: &[u8]) -> Result<Self> {
        rmp_serde::from_slice(raw).map_err(GridError::Decode)
    }

    pub fn with_subroute(mut self, subroute: &str) -> Result<Self> {
        let subroute_bytes = subroute.as_bytes();
        let len = u16::try_from(subroute_bytes.len()).map_err(|_| GridError::SubrouteTooLong {
            len: subroute_bytes.len(),
        })?;

        let mut out = Vec::with_capacity(2 + subroute_bytes.len() + self.payload.len());
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(subroute_bytes);
        out.extend_from_slice(&self.payload);
        self.payload = out;
        self.flags.insert(Flags::SUBROUTE);
        Ok(self)
    }

    pub fn extract_subroute(&self) -> Result<(Option<String>, &[u8])> {
        if !self.flags.contains(Flags::SUBROUTE) {
            return Ok((None, &self.payload));
        }

        if self.payload.len() < 2 {
            return Err(GridError::InvalidSubroutePayload);
        }

        let route_len = u16::from_be_bytes([self.payload[0], self.payload[1]]) as usize;
        if self.payload.len() < 2 + route_len {
            return Err(GridError::InvalidSubroutePayload);
        }

        let route = std::str::from_utf8(&self.payload[2..2 + route_len])
            .map_err(GridError::Utf8)?
            .to_string();
        let body = &self.payload[2 + route_len..];
        Ok((Some(route), body))
    }
}
