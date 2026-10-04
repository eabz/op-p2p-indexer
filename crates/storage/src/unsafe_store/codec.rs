//! Conversion between domain types and what Redis holds: block hashes of JSON fields
//! (docs/storage.md section 3.1) and the events the scripts reply with (section 3.3).

use std::collections::HashMap;

use alloy_consensus::{BlockBody, Header};
use alloy_eips::eip4895::Withdrawals;
use alloy_primitives::{Address, BlockHash};
use op_alloy_consensus::{OpBlock, OpReceiptEnvelope, OpTxEnvelope};
use op_indexer_primitives::{BlockRef, BlockSource, DecodedBlock, Reorg, UnsafeEvent};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::{ParseError, StorageError, Store};

/// Field of a transaction object holding its sender.
const FROM: &str = "from";

/// The JSON fields of a stored block.
pub(super) struct EncodedBlock {
    pub(super) header: String,
    pub(super) transactions: String,
    pub(super) tx_count: usize,
    /// Empty until the block has receipts.
    pub(super) receipts: String,
    pub(super) source: &'static str,
}

/// Encodes a block's fields as JSON in alloy's serde form, each transaction with its `from`.
///
/// Expects a block that passed [`validate_block`](crate::validate::validate_block), which rejects what the scripts or the
/// committed store could not hold.
pub(super) fn encode_block(block: &DecodedBlock) -> Result<EncodedBlock, StorageError> {
    let transactions = &block.block.body.transactions;
    let transactions = transactions
        .iter()
        .zip(&block.senders)
        .map(|(transaction, sender)| {
            let mut value = serde_json::to_value(transaction)?;
            if let Value::Object(fields) = &mut value {
                fields.insert(FROM.to_owned(), serde_json::to_value(sender)?);
            }
            Ok(value)
        })
        .collect::<Result<Vec<_>, serde_json::Error>>()
        .map_err(encode_error("transactions"))?;

    Ok(EncodedBlock {
        header: to_json("header", &block.block.header)?,
        tx_count: transactions.len(),
        transactions: to_json("transactions", &transactions)?,
        receipts: match &block.receipts {
            Some(receipts) => encode_receipts(receipts)?,
            None => String::new(),
        },
        source: match block.source {
            BlockSource::Gossip => "gossip",
            BlockSource::L1 => "l1",
            BlockSource::Import => "import",
            BlockSource::Sync => "sync",
        },
    })
}

pub(super) fn encode_receipts(receipts: &[OpReceiptEnvelope]) -> Result<String, StorageError> {
    to_json("receipts", &receipts)
}

/// Decodes the fields of a stored block. Any failure means the store holds data this binary
/// did not write, which is fatal.
pub(super) fn decode_block(
    hash: BlockHash,
    fields: &HashMap<String, String>,
) -> Result<DecodedBlock, StorageError> {
    let block = Some(hash);
    let header: Header = from_json("header", field(fields, "header", block)?, block)?;
    // Each transaction object carries its sender as `from`. It stays in the object when the
    // transaction is decoded: a deposit has the field anyway, the other types ignore it.
    let objects: Vec<Value> =
        from_json("transactions", field(fields, "transactions", block)?, block)?;
    let mut transactions = Vec::with_capacity(objects.len());
    let mut senders = Vec::with_capacity(objects.len());
    for object in objects {
        let sender = object.get(FROM).ok_or(StorageError::MissingField {
            store: Store::Unsafe,
            field: "transaction sender",
            block,
        })?;
        senders
            .push(Address::deserialize(sender).map_err(decode_error("transaction sender", block))?);
        transactions
            .push(OpTxEnvelope::deserialize(object).map_err(decode_error("transactions", block))?);
    }
    let receipts = fields
        .get("receipts")
        .map(|receipts| from_json("receipts", receipts, block))
        .transpose()?;
    let source = match field(fields, "source", block)? {
        "gossip" => BlockSource::Gossip,
        "l1" => BlockSource::L1,
        "import" => BlockSource::Import,
        "sync" => BlockSource::Sync,
        _ => return Err(invalid("block source", block, None)),
    };

    // Only the header and the transactions are stored. OP Stack blocks have no ommers, and
    // their withdrawals list is empty from the fork that added the withdrawals root.
    let body = BlockBody {
        transactions,
        ommers: Vec::new(),
        withdrawals: header.withdrawals_root.map(|_| Withdrawals::default()),
    };
    Ok(DecodedBlock {
        block: OpBlock::new(header, body),
        hash,
        senders,
        receipts,
        source,
    })
}

/// Decodes the events a script replied with, each a map of the fields it wrote to the stream.
pub(super) fn decode_events(
    events: &[HashMap<String, String>],
) -> Result<Vec<UnsafeEvent>, StorageError> {
    events.iter().map(decode_event).collect()
}

/// Reads a block reference from the `number` and `hash` fields of a Redis hash or an event.
pub(super) fn block_ref(
    fields: &HashMap<String, String>,
    number: &'static str,
    hash: &'static str,
) -> Result<BlockRef, StorageError> {
    Ok(BlockRef {
        number: field(fields, number, None)?
            .parse()
            .map_err(|err| invalid("block number", None, Some(ParseError::from(err))))?,
        hash: parse_hash(field(fields, hash, None)?)?,
    })
}

/// Decodes one event: every type the scripts write to the stream (section 3.3).
fn decode_event(fields: &HashMap<String, String>) -> Result<UnsafeEvent, StorageError> {
    match field(fields, "type", None)? {
        "head" => Ok(UnsafeEvent::NewHead {
            head: block_ref(fields, "number", "hash")?,
            gap: field(fields, "gap", None)? == "1",
        }),
        "fill" => Ok(UnsafeEvent::Filled(block_ref(fields, "number", "hash")?)),
        "reorg" => Ok(UnsafeEvent::Reorg(Reorg {
            common_ancestor: fields
                .contains_key("ancestor_hash")
                .then(|| block_ref(fields, "ancestor_number", "ancestor_hash"))
                .transpose()?,
            old_head: block_ref(fields, "old_head_number", "old_head_hash")?,
            new_head: block_ref(fields, "new_head_number", "new_head_hash")?,
            replaced: field(fields, "replaced", None)?
                .split(',')
                .map(parse_hash)
                .collect::<Result<_, _>>()?,
        })),
        "receipts" => Ok(UnsafeEvent::Receipts(block_ref(fields, "number", "hash")?)),
        "pruned" => Ok(UnsafeEvent::Pruned {
            up_to: block_ref(fields, "number", "hash")?,
        }),
        _ => Err(invalid("event type", None, None)),
    }
}

fn parse_hash(hash: &str) -> Result<BlockHash, StorageError> {
    hash.parse::<BlockHash>()
        .map_err(|err| invalid("block hash", None, Some(ParseError::from(err))))
}

/// Returns the field `name` of `block`'s hash, or of an event when `block` is `None`.
fn field<'a>(
    fields: &'a HashMap<String, String>,
    name: &'static str,
    block: Option<BlockHash>,
) -> Result<&'a str, StorageError> {
    fields
        .get(name)
        .map(String::as_str)
        .ok_or(StorageError::MissingField {
            store: Store::Unsafe,
            field: name,
            block,
        })
}

fn invalid(
    what: &'static str,
    block: Option<BlockHash>,
    source: Option<ParseError>,
) -> StorageError {
    StorageError::InvalidData {
        store: Store::Unsafe,
        what,
        block,
        source,
    }
}

fn to_json<T: serde::Serialize>(what: &'static str, value: &T) -> Result<String, StorageError> {
    serde_json::to_string(value).map_err(encode_error(what))
}

fn from_json<T: DeserializeOwned>(
    what: &'static str,
    json: &str,
    block: Option<BlockHash>,
) -> Result<T, StorageError> {
    serde_json::from_str(json).map_err(decode_error(what, block))
}

fn encode_error(what: &'static str) -> impl Fn(serde_json::Error) -> StorageError {
    move |source| StorageError::Encode { what, source }
}

fn decode_error(
    what: &'static str,
    block: Option<BlockHash>,
) -> impl Fn(serde_json::Error) -> StorageError {
    move |source| StorageError::Decode {
        what,
        block,
        source,
    }
}
