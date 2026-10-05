//! A user deposit as its L1 log has it: the `OptimismPortal`'s `TransactionDeposited(from, to,
//! version, opaqueData)`, whose opaque data packs the mint, the value, the gas limit, whether it
//! creates a contract, and the calldata
//! (<https://specs.optimism.io/protocol/deposits.html#deposit-contract>). The archive service's
//! rows lack a user deposit's `mint`, which the transaction's hash covers: it is taken from
//! here, and the row's other fields are checked against the log, which also proves the logs
//! pair with the right deposits. Only version 0 exists; the header hash proves the rest.

use alloy_primitives::{Address, B256, Bytes, U128, U256};

use super::DepositRow;
use crate::source::DepositLog;

/// What a user deposit's log says of it.
#[derive(Debug)]
pub(super) struct UserDeposit {
    from: Address,
    /// `None` for a contract creation.
    to: Option<Address>,
    pub(super) mint: U128,
    value: U256,
    gas: u64,
    input: Bytes,
}

impl UserDeposit {
    /// Decodes `log`; `None` if it is not a deposit log of this layout.
    pub(super) fn of(log: &DepositLog) -> Option<Self> {
        let address = |topic: Option<B256>| topic.map(|topic| Address::from_word(topic));
        // The log's data is `abi.encode(opaqueData)`: its offset, its length, then its bytes.
        let data = &log.data;
        let word = |at: usize| Some(U256::from_be_slice(data.get(at..at.checked_add(32)?)?));
        let at = usize::try_from(word(0)?).ok()?;
        let length = usize::try_from(word(at)?).ok()?;
        let start = at.checked_add(32)?;
        let opaque = data.get(start..start.checked_add(length)?)?;
        // `abi.encodePacked(mint, value, gasLimit, isCreation, data)`.
        let is_creation = *opaque.get(72)? != 0;
        Some(Self {
            from: address(log.topic1)?,
            to: if is_creation {
                None
            } else {
                Some(address(log.topic2)?)
            },
            mint: U128::try_from(U256::from_be_slice(opaque.get(..32)?)).ok()?,
            value: U256::from_be_slice(opaque.get(32..64)?),
            gas: u64::from_be_bytes(opaque.get(64..72)?.try_into().ok()?),
            input: Bytes::copy_from_slice(opaque.get(73..)?),
        })
    }

    /// The first field of `row` that is not what this log says, named for the error; a field
    /// the row lacks is not compared (the mint is filled from here).
    pub(super) fn differs(&self, row: &DepositRow) -> Option<&'static str> {
        [
            (
                "user deposit `from` (not its L1 log's)",
                row.from.is_some_and(|from| from != self.from),
            ),
            ("user deposit `to` (not its L1 log's)", row.to != self.to),
            (
                "user deposit `mint` (not its L1 log's)",
                row.mint.is_some_and(|mint| mint != self.mint),
            ),
            (
                "user deposit `value` (not its L1 log's)",
                row.value != self.value,
            ),
            ("user deposit `gas` (not its L1 log's)", row.gas != self.gas),
            (
                "user deposit `input` (not its L1 log's)",
                row.input != self.input,
            ),
        ]
        .into_iter()
        .find_map(|(field, differs)| differs.then_some(field))
    }
}
