//! The access list and the authorization list of a transaction, as the archive service gives
//! them.
//!
//! The service stores both in a binary column and sends the column as a hex string. The
//! bytes are what `bincode` (version 1, its default options) writes for the service's own
//! types (`hypersync-format` 0.7.1, read in `hypersync-client` 1.4.1's `arrow_reader`):
//!
//! ```text
//! Vec<T>          u64 little-endian count, then the items
//! Option<T>       one byte, 0 (none) or 1 (some), then the value
//! address, hash,  as TEXT: u64 little-endian length, then that many ASCII characters,
//! quantity        "0x" and hex digits
//!
//! AccessList      { address: Option<address>, storage_keys: Option<Vec<hash>> }
//! Authorization   { chain_id, address, nonce, y_parity, r, s }   (address; the rest quantities)
//! ```
//!
//! Nothing here is trusted and nothing is allocated from a length before it is checked
//! against what is left of the input. What is decoded goes into the rebuilt transaction, and
//! the rebuilt header's hash decides whether it was right.

use alloy_eips::eip2930::{AccessList, AccessListItem};
use alloy_eips::eip7702::{Authorization, SignedAuthorization};
use alloy_primitives::{Address, B256, U256};

/// Decodes an access list. An item without storage keys has none; an item without an address
/// is not an access list entry and is refused.
///
/// # Errors
///
/// Returns what in the bytes is not the layout above.
pub(super) fn access_list(bytes: &[u8]) -> Result<AccessList, String> {
    let mut input = Input(bytes);
    let count = input.count("the number of entries")?;
    let mut items = Vec::with_capacity(count);
    for entry in 0..count {
        let at = |what: &str| format!("entry {entry}: {what}");
        if !input.some().ok_or_else(|| at("no address tag"))? {
            return Err(at("it has no address"));
        }
        let address = input.parse::<Address>().ok_or_else(|| at("its address"))?;
        let storage_keys = if input.some().ok_or_else(|| at("no storage keys tag"))? {
            let keys = input.count(&at("the number of storage keys"))?;
            let mut storage_keys = Vec::with_capacity(keys);
            for key in 0..keys {
                let key = input
                    .parse::<B256>()
                    .ok_or_else(|| at(&format!("storage key {key}")))?;
                storage_keys.push(key);
            }
            storage_keys
        } else {
            Vec::new()
        };
        items.push(AccessListItem {
            address,
            storage_keys,
        });
    }
    input.end()?;
    Ok(AccessList(items))
}

/// Decodes an EIP-7702 authorization list.
///
/// # Errors
///
/// Returns what in the bytes is not the layout above.
pub(super) fn authorization_list(bytes: &[u8]) -> Result<Vec<SignedAuthorization>, String> {
    let mut input = Input(bytes);
    let count = input.count("the number of authorizations")?;
    let mut list = Vec::with_capacity(count);
    for entry in 0..count {
        let at = |what: &str| format!("authorization {entry}: its {what}");
        let chain_id = input.quantity().ok_or_else(|| at("chain id"))?;
        let address = input.parse::<Address>().ok_or_else(|| at("address"))?;
        let nonce = input.quantity().and_then(|nonce| u64::try_from(nonce).ok());
        let nonce = nonce.ok_or_else(|| at("nonce"))?;
        let y_parity = input
            .quantity()
            .and_then(|parity| u8::try_from(parity).ok());
        let y_parity = y_parity.ok_or_else(|| at("y parity"))?;
        let r = input.quantity().ok_or_else(|| at("r"))?;
        let s = input.quantity().ok_or_else(|| at("s"))?;
        let inner = Authorization {
            chain_id,
            address,
            nonce,
        };
        list.push(SignedAuthorization::new_unchecked(inner, y_parity, r, s));
    }
    input.end()?;
    Ok(list)
}

/// Encodes an access list in the service's layout: what a block filled from the chain's RPC
/// gives its rows, so they are rebuilt like the service's. The inverse of [`access_list`].
pub(crate) fn encode_access_list(list: &AccessList) -> Vec<u8> {
    fn length(out: &mut Vec<u8>, length: usize) {
        let length = u64::try_from(length).unwrap_or(u64::MAX);
        out.extend_from_slice(&length.to_le_bytes());
    }
    fn text(out: &mut Vec<u8>, text: &str) {
        length(out, text.len());
        out.extend_from_slice(text.as_bytes());
    }
    let mut out = Vec::new();
    length(&mut out, list.len());
    for item in list.iter() {
        out.push(1);
        text(&mut out, &format!("{:#x}", item.address));
        out.push(1);
        length(&mut out, item.storage_keys.len());
        for key in &item.storage_keys {
            text(&mut out, &format!("{key:#x}"));
        }
    }
    out
}

/// What is left of the bytes being decoded.
struct Input<'a>(&'a [u8]);

impl<'a> Input<'a> {
    /// Takes the next `length` bytes, if there are that many.
    fn take(&mut self, length: usize) -> Option<&'a [u8]> {
        let (taken, rest) = self.0.split_at_checked(length)?;
        self.0 = rest;
        Some(taken)
    }

    /// Takes a length: a little-endian `u64` that is no more than the bytes left, since
    /// everything a length counts takes at least one byte.
    fn length(&mut self) -> Option<usize> {
        let bytes: [u8; 8] = self.take(8)?.try_into().ok()?;
        let length = usize::try_from(u64::from_le_bytes(bytes)).ok()?;
        (length <= self.0.len()).then_some(length)
    }

    /// Takes the count of a `Vec`.
    fn count(&mut self, what: &str) -> Result<usize, String> {
        self.length()
            .ok_or_else(|| format!("{what} is missing or larger than the data"))
    }

    /// Takes the tag of an `Option`: whether a value follows.
    fn some(&mut self) -> Option<bool> {
        match self.take(1)? {
            [0] => Some(false),
            [1] => Some(true),
            _ => None,
        }
    }

    /// Takes a text value.
    fn text(&mut self) -> Option<&'a str> {
        let length = self.length()?;
        std::str::from_utf8(self.take(length)?).ok()
    }

    /// Takes a text value that is a fixed-size hex value: an address or a hash.
    fn parse<T: std::str::FromStr>(&mut self) -> Option<T> {
        self.text()?.parse().ok()
    }

    /// Takes a text value that is a quantity: "0x" and hex digits, none for zero.
    fn quantity(&mut self) -> Option<U256> {
        let digits = self.text()?.strip_prefix("0x")?;
        if digits.is_empty() {
            return Some(U256::ZERO);
        }
        U256::from_str_radix(digits, 16).ok()
    }

    /// Checks that nothing is left.
    fn end(&self) -> Result<(), String> {
        if self.0.is_empty() {
            Ok(())
        } else {
            Err(format!("{} bytes follow the last entry", self.0.len()))
        }
    }
}
