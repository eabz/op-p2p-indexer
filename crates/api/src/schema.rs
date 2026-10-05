//! Arrow schemas shared by directory responses and server batches.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::ticket::Table;

impl Table {
    /// Returns the wire schema for this table.
    #[must_use]
    pub fn schema(self) -> SchemaRef {
        let fields = match self {
            Self::Blocks => vec![
                Field::new("number", DataType::UInt64, false),
                Field::new("hash", DataType::FixedSizeBinary(32), false),
                Field::new("parent_hash", DataType::FixedSizeBinary(32), false),
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                    false,
                ),
                Field::new("fee_recipient", DataType::FixedSizeBinary(20), false),
                Field::new("state_root", DataType::FixedSizeBinary(32), false),
                Field::new("transactions_root", DataType::FixedSizeBinary(32), false),
                Field::new("receipts_root", DataType::FixedSizeBinary(32), false),
                Field::new("logs_bloom", DataType::FixedSizeBinary(256), false),
                Field::new("prev_randao", DataType::FixedSizeBinary(32), false),
                Field::new("gas_limit", DataType::UInt64, false),
                Field::new("gas_used", DataType::UInt64, false),
                Field::new("base_fee_per_gas", DataType::UInt64, true),
                Field::new("extra_data", DataType::Binary, false),
                Field::new("tx_count", DataType::UInt32, false),
                Field::new("withdrawals_root", DataType::FixedSizeBinary(32), true),
                Field::new("blob_gas_used", DataType::UInt64, true),
                Field::new("excess_blob_gas", DataType::UInt64, true),
                Field::new(
                    "parent_beacon_block_root",
                    DataType::FixedSizeBinary(32),
                    true,
                ),
                Field::new("requests_hash", DataType::FixedSizeBinary(32), true),
                Field::new("has_receipts", DataType::Boolean, false),
                Field::new("status", DataType::Utf8, false),
            ],
            Self::Transactions => vec![
                Field::new("block_number", DataType::UInt64, false),
                Field::new("block_hash", DataType::FixedSizeBinary(32), false),
                Field::new(
                    "block_timestamp",
                    DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                    false,
                ),
                Field::new("tx_index", DataType::UInt32, false),
                Field::new("hash", DataType::FixedSizeBinary(32), false),
                Field::new("tx_type", DataType::UInt8, false),
                Field::new("sender", DataType::FixedSizeBinary(20), false),
                Field::new("to", DataType::FixedSizeBinary(20), true),
                Field::new("nonce", DataType::UInt64, true),
                Field::new("value", DataType::FixedSizeBinary(32), false),
                Field::new("gas_limit", DataType::UInt64, false),
                Field::new("gas_price", DataType::FixedSizeBinary(32), true),
                Field::new("max_fee_per_gas", DataType::FixedSizeBinary(32), true),
                Field::new(
                    "max_priority_fee_per_gas",
                    DataType::FixedSizeBinary(32),
                    true,
                ),
                Field::new("input", DataType::Binary, false),
                Field::new("source_hash", DataType::FixedSizeBinary(32), true),
                Field::new("mint", DataType::FixedSizeBinary(32), true),
                Field::new("is_system_tx", DataType::Boolean, true),
                Field::new("encoded", DataType::Binary, false),
            ],
            Self::Receipts => vec![
                Field::new("block_number", DataType::UInt64, false),
                Field::new("block_hash", DataType::FixedSizeBinary(32), false),
                Field::new(
                    "block_timestamp",
                    DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                    false,
                ),
                Field::new("tx_index", DataType::UInt32, false),
                Field::new("tx_hash", DataType::FixedSizeBinary(32), false),
                Field::new("success", DataType::Boolean, false),
                Field::new("cumulative_gas_used", DataType::UInt64, false),
                Field::new("gas_used", DataType::UInt64, false),
                Field::new("logs_count", DataType::UInt32, false),
                Field::new("deposit_nonce", DataType::UInt64, true),
                Field::new("deposit_receipt_version", DataType::UInt64, true),
            ],
            Self::Logs => vec![
                Field::new("block_number", DataType::UInt64, false),
                Field::new("block_hash", DataType::FixedSizeBinary(32), false),
                Field::new(
                    "block_timestamp",
                    DataType::Timestamp(TimeUnit::Second, Some("UTC".into())),
                    false,
                ),
                Field::new("log_index", DataType::UInt32, false),
                Field::new("tx_index", DataType::UInt32, false),
                Field::new("tx_hash", DataType::FixedSizeBinary(32), false),
                Field::new("address", DataType::FixedSizeBinary(20), false),
                Field::new("topic0", DataType::FixedSizeBinary(32), true),
                Field::new("topic1", DataType::FixedSizeBinary(32), true),
                Field::new("topic2", DataType::FixedSizeBinary(32), true),
                Field::new("topic3", DataType::FixedSizeBinary(32), true),
                Field::new("data", DataType::Binary, false),
            ],
        };
        Arc::new(Schema::new(fields))
    }
}
