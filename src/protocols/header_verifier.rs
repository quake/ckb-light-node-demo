use ckb_chain_spec::consensus::Consensus;
use ckb_types::{core::HeaderView, packed::Byte32};

pub trait HeaderProvider {
    fn get_header(&self, hash: Byte32) -> Option<HeaderView>;
}

pub struct HeaderVerifier<'a, T> {
    consensus: &'a Consensus,
    header_provider: &'a T,
}

impl<'a, T> HeaderVerifier<'a, T> {
    pub fn new(consensus: &'a Consensus, header_provider: &'a T) -> Self {
        Self {
            consensus,
            header_provider,
        }
    }
}

impl<'a, T: HeaderProvider> HeaderVerifier<'a, T> {
    pub fn verify(&self, header: &HeaderView) -> Result<(), HeaderVerificationError> {
        self.verify_version(header)
            .and(self.verify_pow(header))
            .and(self.verify_number(header))
            .and(self.verify_timestamp(header))
    }

    fn verify_version(&self, header: &HeaderView) -> Result<(), HeaderVerificationError> {
        if header.version() == self.consensus.block_version() {
            Ok(())
        } else {
            Err(HeaderVerificationError::Version)
        }
    }

    fn verify_pow(&self, header: &HeaderView) -> Result<(), HeaderVerificationError> {
        if self.consensus.pow_engine().verify(&header.data()) {
            Ok(())
        } else {
            Err(HeaderVerificationError::Pow)
        }
    }

    fn verify_number(&self, header: &HeaderView) -> Result<(), HeaderVerificationError> {
        match self.header_provider.get_header(header.parent_hash()) {
            Some(parent) => {
                if parent.number() + 1 == header.number() {
                    Ok(())
                } else {
                    Err(HeaderVerificationError::Number)
                }
            }
            None => Err(HeaderVerificationError::UnknownParent),
        }
    }

    fn verify_timestamp(&self, header: &HeaderView) -> Result<(), HeaderVerificationError> {
        let median_time_block_count = self.consensus.median_time_block_count();
        let mut timestamps = Vec::with_capacity(median_time_block_count);
        let mut parent_hash = header.parent_hash();
        for _ in 0..median_time_block_count {
            match self.header_provider.get_header(parent_hash) {
                Some(parent) => {
                    timestamps.push(parent.timestamp());
                    parent_hash = parent.parent_hash();
                }
                None => {
                    break;
                }
            }
        }
        timestamps.sort();
        let median_time = timestamps[timestamps.len() >> 1];
        if header.timestamp() > median_time {
            Ok(())
        } else {
            Err(HeaderVerificationError::Timestamp)
        }
    }
}

#[derive(Debug)]
pub enum HeaderVerificationError {
    Version,
    Pow,
    Number,
    Timestamp,
    UnknownParent,
}

impl std::error::Error for HeaderVerificationError {}

impl std::fmt::Display for HeaderVerificationError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            HeaderVerificationError::Version => write!(f, "invalid version"),
            HeaderVerificationError::Pow => write!(f, "invalid nonce"),
            HeaderVerificationError::Number => write!(f, "invalid block number"),
            HeaderVerificationError::Timestamp => write!(f, "invalid block timestamp"),
            HeaderVerificationError::UnknownParent => write!(f, "cannot find parent block"),
        }
    }
}
