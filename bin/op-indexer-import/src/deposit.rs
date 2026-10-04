//! Deposit reconstruction: rebuilds the deposit transactions (type `0x7E`) of a block from
//! Bedrock on, which the archive service reports without their source hash, mint and
//! system-transaction flag, and gives the fields their receipts need.
//!
//! Every rebuilt transaction is encoded and hashed, and the hash must equal the one reported:
//! a transaction that does not match is an error, never stored. The encoded bytes are returned
//! so the caller stores exactly what was checked.
//!
//! Does not fetch anything and does not check the transactions root or the receipts root: the
//! caller does, over the bytes returned here.
//!
//! # Rules
//!
//! From the OP Stack specification; the section is named where the rule is applied.
//!
//! - **The transaction** is `0x7E || rlp([sourceHash, from, to, mint, value, gas, isSystemTx,
//!   data])` ([the deposited transaction type]).
//! - **The first transaction of a block is the L1-attributes deposit** ([kinds of deposited
//!   transactions]): from the depositor account to the L1 block predeploy, `mint` and `value`
//!   zero, `isSystemTx` true before Regolith and false from it on ([L1 attributes deposited
//!   transaction], [Regolith]). Its source hash is `keccak256(bytes32(1), keccak256(l1BlockHash,
//!   bytes32(seqNumber)))` ([source hash computation]), with the L1 block hash and sequence
//!   number read from its own calldata: [Bedrock's `setL1BlockValues`][L1 attributes calldata]
//!   (ABI words), or the packed form of [Ecotone], [Isthmus] and [Jovian].
//! - **User deposits follow, only in the first block of an epoch** (sequence number 0), one
//!   per `TransactionDeposited` event of the deposit contract in the L1 origin block, in log
//!   order ([user-deposited transactions], [deriving the transaction list]). Source hash:
//!   `keccak256(bytes32(0), keccak256(l1BlockHash, bytes32(l1LogIndex)))`, the log index
//!   counted over all logs of the L1 block. `from` and `to` are the event's indexed fields;
//!   `mint`, `value`, `gas`, the creation flag and `data` are packed in its `opaqueData`.
//!   `isSystemTx` is false.
//! - **Deposit receipts** ([deposit receipt], [Regolith], [Canyon]): from Regolith the receipt
//!   records the sender's nonce before the transaction (`depositNonce`); from Canyon it also
//!   carries the version 1, and both are part of the hashed receipt. Before Canyon neither is
//!   hashed.
//!
//! - **Network upgrade deposits** close the deposits of a fork's activation block, after the
//!   user deposits ([Ecotone][ecotone upgrade], [Fjord][fjord upgrade], [Isthmus][isthmus
//!   upgrade], [Jovian][jovian upgrade], [Karst][karst upgrade]; Canyon, Delta, Granite and
//!   Holocene have none). Each
//!   is a fixed transaction with `mint` zero, `isSystemTx` false and the source hash
//!   `keccak256(bytes32(2), keccak256(intent))`, where the intent is a string the fork's
//!   specification gives ([source hash computation]). Sender, target, gas and data are
//!   reported, so only the intent has to be known: [`UPGRADE_INTENTS`] and [`KARST_INTENTS`]
//!   list them, and a
//!   deposit is an upgrade deposit if one of them makes it hash to its reported hash. No
//!   bytecode is kept here. (`kona-hardforks` ships these transactions, but it is built on
//!   op-alloy 0.18 and has no Jovian; it was used to cross-check the intents up to Isthmus.)
//!
//! The layout of `opaqueData` is not in the specification text. It is that of the reference
//! deposit contract the specification names (`OptimismPortal.depositTransaction`):
//! `abi.encodePacked(uint256 mint, uint256 value, uint64 gasLimit, bool isCreation, bytes
//! data)`, in the event `TransactionDeposited(address indexed from, address indexed to, uint256
//! indexed version, bytes opaqueData)` with version 0.
//!
//! [the deposited transaction type]: https://specs.optimism.io/protocol/deposits.html#the-deposited-transaction-type
//! [source hash computation]: https://specs.optimism.io/protocol/deposits.html#source-hash-computation
//! [kinds of deposited transactions]: https://specs.optimism.io/protocol/deposits.html#kinds-of-deposited-transactions
//! [deposit receipt]: https://specs.optimism.io/protocol/deposits.html#deposit-receipt
//! [L1 attributes deposited transaction]: https://specs.optimism.io/protocol/deposits.html#l1-attributes-deposited-transaction
//! [L1 attributes calldata]: https://specs.optimism.io/protocol/deposits.html#l1-attributes-deposited-transaction-calldata
//! [user-deposited transactions]: https://specs.optimism.io/protocol/deposits.html#user-deposited-transactions
//! [deriving the transaction list]: https://specs.optimism.io/protocol/derivation.html#deriving-the-transaction-list
//! [Regolith]: https://specs.optimism.io/protocol/regolith/overview.html
//! [Canyon]: https://specs.optimism.io/protocol/canyon/overview.html
//! [Ecotone]: https://specs.optimism.io/protocol/ecotone/l1-attributes.html
//! [Isthmus]: https://specs.optimism.io/protocol/isthmus/l1-attributes.html
//! [Jovian]: https://specs.optimism.io/protocol/jovian/l1-attributes.html
//! [ecotone upgrade]: https://specs.optimism.io/protocol/ecotone/derivation.html
//! [fjord upgrade]: https://specs.optimism.io/protocol/fjord/derivation.html
//! [isthmus upgrade]: https://specs.optimism.io/protocol/isthmus/derivation.html
//! [jovian upgrade]: https://specs.optimism.io/protocol/jovian/derivation.html
//! [karst upgrade]: https://specs.optimism.io/protocol/karst/derivation.html

use alloy_primitives::{Address, B256, Bytes, TxKind, U256, address, keccak256};
use op_alloy_consensus::{
    DEPOSIT_TX_TYPE_ID, L1InfoDepositSource, TxDeposit, UpgradeDepositSource, UserDepositSource,
};

/// The account L1-attributes deposits are sent from.
const L1_ATTRIBUTES_DEPOSITOR: Address = address!("0xdeaddeaddeaddeaddeaddeaddeaddeaddead0001");
/// The L1 block predeploy, which L1-attributes deposits call.
const L1_BLOCK_PREDEPLOY: Address = address!("0x4200000000000000000000000000000000000015");

/// `setL1BlockValues(uint64,uint64,uint256,bytes32,uint64,bytes32,uint256,uint256)`: Bedrock
/// to Delta.
const SET_L1_BLOCK_VALUES: [u8; 4] = [0x01, 0x5d, 0x8e, 0xb9];
/// `setL1BlockValuesEcotone()`.
const SET_L1_BLOCK_VALUES_ECOTONE: [u8; 4] = [0x44, 0x0a, 0x5e, 0x20];
/// `setL1BlockValuesIsthmus()`.
const SET_L1_BLOCK_VALUES_ISTHMUS: [u8; 4] = [0x09, 0x89, 0x99, 0xbe];
/// `setL1BlockValuesJovian()`.
const SET_L1_BLOCK_VALUES_JOVIAN: [u8; 4] = [0x3d, 0xb6, 0xbe, 0x2b];

/// Where the L1 block number is in the L1-attributes calldata, as a big-endian `u64`. The
/// same in the ABI form (the low bytes of the first word) and the packed form.
const L1_NUMBER_AT: usize = 28;
/// Where the L1 block hash is in the L1-attributes calldata: the fourth ABI word of the
/// Bedrock form, and the same bytes of the packed form.
const L1_HASH_AT: usize = 100;
/// The fifth ABI word of the Bedrock form: the sequence number.
const BEDROCK_SEQUENCE_WORD_AT: usize = 132;
/// Where the sequence number is in the packed form, as a big-endian `u64`.
const PACKED_SEQUENCE_AT: usize = 12;

/// The intents of every network upgrade deposit up to Jovian, by fork, as the specification of each fork's
/// activation block gives them. A deposit is matched by hash, so their order does not matter
/// and a wrong entry can only fail to match.
const UPGRADE_INTENTS: [&str; 22] = [
    "Ecotone: L1 Block Deployment",
    "Ecotone: Gas Price Oracle Deployment",
    "Ecotone: L1 Block Proxy Update",
    "Ecotone: Gas Price Oracle Proxy Update",
    "Ecotone: Gas Price Oracle Set Ecotone",
    "Ecotone: beacon block roots contract deployment",
    "Fjord: Gas Price Oracle Deployment",
    "Fjord: Gas Price Oracle Proxy Update",
    "Fjord: Gas Price Oracle Set Fjord",
    "Isthmus: L1 Block Deployment",
    "Isthmus: Gas Price Oracle Deployment",
    "Isthmus: Operator Fee Vault Deployment",
    "Isthmus: L1 Block Proxy Update",
    "Isthmus: Gas Price Oracle Proxy Update",
    "Isthmus: Operator Fee Vault Proxy Update",
    "Isthmus: Gas Price Oracle Set Isthmus",
    "Isthmus: EIP-2935 Contract Deployment",
    "Jovian: L1Block Deployment",
    "Jovian: L1Block Proxy Update",
    "Jovian: GasPriceOracle Deployment",
    "Jovian: GasPriceOracle Proxy Update",
    "Jovian: Gas Price Oracle Set Jovian",
];

/// The intents of Karst's network upgrade deposits, in the order of its bundle
/// (`op-core/nuts/bundles/karst_nut_bundle.json` in the Optimism monorepo, at the commit its
/// `fork_lock.toml` names). From Karst on, the intent hashed is qualified with the fork and the
/// transaction's position: `"Karst <index>: <intent>"`.
const KARST_INTENTS: [&str; 31] = [
    "ConditionalDeployer Deployment",
    "Upgrade ConditionalDeployer Implementation",
    "Deploy StorageSetter Implementation",
    "Deploy L2CrossDomainMessenger Implementation",
    "Deploy GasPriceOracle Implementation",
    "Deploy L2StandardBridge Implementation",
    "Deploy SequencerFeeVault Implementation",
    "Deploy OptimismMintableERC20Factory Implementation",
    "Deploy L2ERC721Bridge Implementation",
    "Deploy L1Block Implementation",
    "Deploy L2ToL1MessagePasser Implementation",
    "Deploy OptimismMintableERC721Factory Implementation",
    "Deploy L2ProxyAdmin Implementation",
    "Deploy BaseFeeVault Implementation",
    "Deploy L1FeeVault Implementation",
    "Deploy OperatorFeeVault Implementation",
    "Deploy SchemaRegistry Implementation",
    "Deploy EAS Implementation",
    "Deploy ConditionalDeployer Implementation",
    "Deploy L2DevFeatureFlags Implementation",
    "Deploy CrossL2Inbox Implementation",
    "Deploy L2ToL2CrossDomainMessenger Implementation",
    "Deploy SuperchainETHBridge Implementation",
    "Deploy ETHLiquidity Implementation",
    "Deploy L1BlockCGT Implementation",
    "Deploy L2ToL1MessagePasserCGT Implementation",
    "Deploy LiquidityController Implementation",
    "Deploy NativeAssetLiquidity Implementation",
    "Upgrade L2ProxyAdmin Implementation",
    "Deploy L2ContractsManager Implementation",
    "L2ProxyAdmin Upgrade Predeploys",
];

/// Bytes of `opaqueData` before the transaction's `data`: mint, value, gas limit, creation flag.
const OPAQUE_FIXED_BYTES: usize = 32 + 32 + 8 + 1;
/// An ABI word.
const WORD: usize = 32;

/// The fork activations that change deposits, as Unix timestamps; `None` for a fork the chain
/// has not scheduled. They come from the importer's chain configuration.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DepositForks {
    pub(crate) regolith_time: Option<u64>,
    pub(crate) canyon_time: Option<u64>,
}

/// A deposit transaction as the archive service reports it.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ReportedDeposit<'a> {
    pub(crate) hash: B256,
    pub(crate) from: Address,
    /// `None` for a contract creation.
    pub(crate) to: Option<Address>,
    pub(crate) value: U256,
    pub(crate) gas: u64,
    pub(crate) input: &'a Bytes,
    /// The sender's nonce before the transaction, as nodes report it from Regolith on.
    pub(crate) nonce: Option<u64>,
}

/// The L1 block an L2 block was derived from, as its L1-attributes deposit states it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct L1Origin {
    pub(crate) number: u64,
    pub(crate) hash: B256,
    /// The L2 block's position in its epoch; 0 for the block that holds the user deposits.
    pub(crate) sequence_number: u64,
}

/// One `TransactionDeposited` log of the deposit contract.
#[derive(Debug, Clone)]
pub(crate) struct DepositEvent {
    /// The log's index among all logs of its L1 block.
    pub(crate) log_index: u64,
    /// Topic 1.
    pub(crate) from: Address,
    /// Topic 2.
    pub(crate) to: Address,
    /// Topic 3.
    pub(crate) version: U256,
    /// The log's data, unchanged: the ABI encoding of the event's `bytes opaqueData`.
    pub(crate) data: Bytes,
}

/// What reconstruction needs from L1.
pub(crate) trait L1DepositSource {
    /// Why a lookup failed.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Returns every `TransactionDeposited` log the chain's deposit contract emitted in L1
    /// block `origin.number`, in log order. Logs of reverted transactions do not exist, so
    /// none are to be filtered.
    ///
    /// # Errors
    ///
    /// Must fail if that block is not known or its hash is not `origin.hash`: logs of another
    /// block at that height would give deposits that do not exist.
    fn deposits(&self, origin: &L1Origin) -> Result<Vec<DepositEvent>, Self::Error>;
}

/// A rebuilt deposit transaction, checked against its reported hash.
#[derive(Debug, Clone)]
pub(crate) struct RebuiltDeposit {
    pub(crate) tx: TxDeposit,
    /// The transaction's EIP-2718 encoding: the bytes whose keccak is the reported hash.
    pub(crate) encoded: Vec<u8>,
    /// The receipt's deposit nonce: set from Regolith on.
    pub(crate) deposit_nonce: Option<u64>,
    /// The receipt's deposit receipt version: 1 from Canyon on.
    pub(crate) deposit_receipt_version: Option<u64>,
}

/// A deposit that could not be rebuilt.
#[derive(Debug, thiserror::Error)]
#[error("block {block}, transaction {index}: {rule}")]
pub(crate) struct DepositError {
    pub(crate) block: u64,
    /// The transaction's index in the block.
    pub(crate) index: usize,
    #[source]
    pub(crate) rule: Rule,
}

/// The rule a deposit, or what is known about it, does not satisfy.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Rule {
    #[error("the block does not start with a deposit, so it has no L1-attributes transaction")]
    NoL1Attributes,
    #[error("the L1-attributes deposit is from {0}, not the depositor account")]
    L1AttributesSender(Address),
    #[error("the L1-attributes deposit does not call the L1 block predeploy")]
    L1AttributesTarget,
    #[error("the L1-attributes deposit carries a value")]
    L1AttributesValue,
    #[error(
        "the L1-attributes calldata ({length} bytes, starting {start}) is not a known \
         setL1BlockValues form; a fork that changes it needs its form added"
    )]
    L1AttributesCalldata {
        length: usize,
        /// The first bytes of the calldata: its selector, if it is that long.
        start: Bytes,
    },
    #[error("the L1 deposit events of block {number} could not be read")]
    L1Lookup {
        /// The L1 origin block.
        number: u64,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error(
        "the deposit follows the user deposits, so it is a network upgrade deposit, and no \
         known upgrade intent makes it hash to the reported {reported}; if a fork activates at \
         this block (time {block_timestamp}), the intent table needs that fork's intents"
    )]
    UpgradeDeposit {
        /// The hash to match.
        reported: B256,
        /// The block's timestamp: a fork's activation time if the fork is not known here.
        block_timestamp: u64,
    },
    #[error("the L1 origin has {events} deposit events but the block has {deposits} user deposits")]
    MissingDeposits { events: usize, deposits: usize },
    #[error("the deposit event has version {0}, and only version 0 is known")]
    EventVersion(U256),
    #[error("the deposit event's data is not an ABI-encoded opaqueData")]
    EventData,
    #[error("the deposit event mints more than fits 128 bits")]
    MintRange,
    #[error("the deposit event gives another `{0}` than the reported transaction")]
    EventMismatch(&'static str),
    #[error("the rebuilt {kind} hashes to {rebuilt}, not to the reported {reported}")]
    Hash {
        /// Which rule set built the transaction.
        kind: &'static str,
        reported: B256,
        rebuilt: B256,
    },
    #[error("the deposit's nonce is not reported, and from Canyon its receipt needs it")]
    MissingNonce,
}

/// Rebuilds the deposits of one block. `deposits` are the block's deposit transactions, which
/// are its first ones, in order; the result has one entry for each, in the same order.
///
/// `l1` is asked only for a block that holds user deposits: the first block of an epoch.
/// CPU work (two keccaks per deposit) apart from that lookup.
///
/// # Errors
///
/// Returns [`DepositError`], naming the block, the transaction and the [`Rule`], if a
/// transaction cannot be rebuilt from what is known, or if it is rebuilt and does not hash to
/// its reported hash.
pub(crate) fn rebuild_deposits<S: L1DepositSource>(
    forks: &DepositForks,
    block_number: u64,
    block_timestamp: u64,
    deposits: &[ReportedDeposit<'_>],
    l1: &S,
) -> Result<Vec<RebuiltDeposit>, DepositError> {
    let at = |index: usize| {
        move |rule: Rule| DepositError {
            block: block_number,
            index,
            rule,
        }
    };
    let active = |fork: Option<u64>| fork.is_some_and(|time| block_timestamp >= time);
    let (regolith, canyon) = (active(forks.regolith_time), active(forks.canyon_time));
    let receipt = |reported: &ReportedDeposit<'_>| receipt_fields(reported, regolith, canyon);

    let Some((attributes, users)) = deposits.split_first() else {
        return Err(at(0)(Rule::NoL1Attributes));
    };
    let origin = l1_origin(attributes).map_err(at(0))?;
    let mut rebuilt = Vec::with_capacity(deposits.len());
    let tx = l1_attributes(attributes, &origin, regolith);
    let (nonce, version) = receipt(attributes).map_err(at(0))?;
    rebuilt.push(checked(tx, attributes, "L1-attributes deposit", nonce, version).map_err(at(0))?);
    if users.is_empty() {
        return Ok(rebuilt);
    }

    // User deposits exist only in the first block of an epoch.
    let events = if origin.sequence_number == 0 {
        l1.deposits(&origin).map_err(|source| {
            at(1)(Rule::L1Lookup {
                number: origin.number,
                source: Box::new(source),
            })
        })?
    } else {
        Vec::new()
    };
    if events.len() > users.len() {
        return Err(at(deposits.len())(Rule::MissingDeposits {
            events: events.len(),
            deposits: users.len(),
        }));
    }
    for (position, reported) in users.iter().enumerate() {
        let index = position.saturating_add(1);
        let (nonce, version) = receipt(reported).map_err(at(index))?;
        // The deposits past the origin's events are the upgrade deposits of a fork's activation.
        let deposit = match events.get(position) {
            Some(event) => {
                let tx = user_deposit(reported, event, &origin).map_err(at(index))?;
                checked(tx, reported, "user deposit", nonce, version)
            }
            None => upgrade_deposit(reported, block_timestamp, nonce, version),
        };
        rebuilt.push(deposit.map_err(at(index))?);
    }
    Ok(rebuilt)
}

/// Reads the L1 origin from the calldata of an L1-attributes deposit, after checking that the
/// transaction is one: from the depositor account, to the L1 block predeploy, without value.
fn l1_origin(reported: &ReportedDeposit<'_>) -> Result<L1Origin, Rule> {
    if reported.from != L1_ATTRIBUTES_DEPOSITOR {
        return Err(Rule::L1AttributesSender(reported.from));
    }
    if reported.to != Some(L1_BLOCK_PREDEPLOY) {
        return Err(Rule::L1AttributesTarget);
    }
    if !reported.value.is_zero() {
        return Err(Rule::L1AttributesValue);
    }
    let input = reported.input.as_ref();
    let unknown = || Rule::L1AttributesCalldata {
        length: input.len(),
        start: Bytes::copy_from_slice(input.get(..4).unwrap_or(input)),
    };
    // The form is told by the selector, not by the fork: a fork's activation block still uses
    // the form before it.
    let sequence_number = match (field::<4>(input, 0).ok_or_else(unknown)?, input.len()) {
        // Eight ABI words; the sequence number is the fifth, a `uint64`.
        (SET_L1_BLOCK_VALUES, 260) => {
            let word = field::<WORD>(input, BEDROCK_SEQUENCE_WORD_AT).ok_or_else(unknown)?;
            u64::try_from(U256::from_be_bytes(word)).map_err(|_overflow| unknown())?
        }
        (SET_L1_BLOCK_VALUES_ECOTONE, 164)
        | (SET_L1_BLOCK_VALUES_ISTHMUS, 176)
        | (SET_L1_BLOCK_VALUES_JOVIAN, 178) => {
            u64::from_be_bytes(field(input, PACKED_SEQUENCE_AT).ok_or_else(unknown)?)
        }
        _ => return Err(unknown()),
    };
    Ok(L1Origin {
        number: u64::from_be_bytes(field(input, L1_NUMBER_AT).ok_or_else(unknown)?),
        hash: B256::from(field::<WORD>(input, L1_HASH_AT).ok_or_else(unknown)?),
        sequence_number,
    })
}

/// The L1-attributes deposit: what is reported, plus the three fields the rules give.
fn l1_attributes(reported: &ReportedDeposit<'_>, origin: &L1Origin, regolith: bool) -> TxDeposit {
    TxDeposit {
        source_hash: L1InfoDepositSource::new(origin.hash, origin.sequence_number).source_hash(),
        from: L1_ATTRIBUTES_DEPOSITOR,
        to: TxKind::Call(L1_BLOCK_PREDEPLOY),
        mint: 0,
        value: U256::ZERO,
        gas_limit: reported.gas,
        is_system_transaction: !regolith,
        input: reported.input.clone(),
    }
}

/// A user deposit, built from its L1 event alone and compared with what is reported, so a
/// wrong pairing of events and transactions is named rather than shown as a hash mismatch.
fn user_deposit(
    reported: &ReportedDeposit<'_>,
    event: &DepositEvent,
    origin: &L1Origin,
) -> Result<TxDeposit, Rule> {
    // Version 0 is the only version of the event.
    if !event.version.is_zero() {
        return Err(Rule::EventVersion(event.version));
    }
    let opaque = opaque_data(&event.data).ok_or(Rule::EventData)?;
    let fixed = (
        field::<WORD>(opaque, 0),
        field::<WORD>(opaque, WORD),
        field::<8>(opaque, 2 * WORD),
        opaque.get(OPAQUE_FIXED_BYTES - 1),
        opaque.get(OPAQUE_FIXED_BYTES..),
    );
    let (Some(mint), Some(value), Some(gas), Some(&creation), Some(data)) = fixed else {
        return Err(Rule::EventData);
    };
    let mint = u128::try_from(U256::from_be_bytes(mint)).map_err(|_overflow| Rule::MintRange)?;
    let to = (creation == 0).then_some(event.to);
    let tx = TxDeposit {
        source_hash: UserDepositSource::new(origin.hash, event.log_index).source_hash(),
        from: event.from,
        to: to.into(),
        mint,
        value: U256::from_be_bytes(value),
        gas_limit: u64::from_be_bytes(gas),
        is_system_transaction: false,
        // The reported calldata, which is compared with the event's below: no copy.
        input: reported.input.clone(),
    };
    let differs = [
        ("from", tx.from != reported.from),
        ("to", to != reported.to),
        ("value", tx.value != reported.value),
        ("gas", tx.gas_limit != reported.gas),
        ("input", data != reported.input.as_ref()),
    ];
    match differs.into_iter().find(|(_, differs)| *differs) {
        Some((name, _)) => Err(Rule::EventMismatch(name)),
        None => Ok(tx),
    }
}

/// A network upgrade deposit: what is reported, no mint, and the source hash of the one known
/// intent that makes it hash to its reported hash.
fn upgrade_deposit(
    reported: &ReportedDeposit<'_>,
    block_timestamp: u64,
    deposit_nonce: Option<u64>,
    deposit_receipt_version: Option<u64>,
) -> Result<RebuiltDeposit, Rule> {
    let plain = UPGRADE_INTENTS.iter().map(|intent| (*intent).to_owned());
    let karst = KARST_INTENTS
        .iter()
        .enumerate()
        .map(|(index, intent)| format!("Karst {index}: {intent}"));
    let base = TxDeposit {
        source_hash: B256::ZERO,
        from: reported.from,
        to: reported.to.into(),
        mint: 0,
        value: reported.value,
        gas_limit: reported.gas,
        is_system_transaction: false,
        input: reported.input.clone(),
    };
    plain
        .chain(karst)
        .find_map(|intent| {
            let tx = TxDeposit {
                source_hash: UpgradeDepositSource::new(intent).source_hash(),
                ..base.clone()
            };
            let kind = "network upgrade deposit";
            checked(tx, reported, kind, deposit_nonce, deposit_receipt_version).ok()
        })
        .ok_or(Rule::UpgradeDeposit {
            reported: reported.hash,
            block_timestamp,
        })
}

/// The deposit nonce and deposit receipt version of a deposit's receipt.
fn receipt_fields(
    reported: &ReportedDeposit<'_>,
    regolith: bool,
    canyon: bool,
) -> Result<(Option<u64>, Option<u64>), Rule> {
    if canyon && reported.nonce.is_none() {
        return Err(Rule::MissingNonce);
    }
    let nonce = if regolith { reported.nonce } else { None };
    Ok((nonce, canyon.then_some(1)))
}

/// Encodes `tx` and checks that it hashes to the reported hash.
fn checked(
    tx: TxDeposit,
    reported: &ReportedDeposit<'_>,
    kind: &'static str,
    deposit_nonce: Option<u64>,
    deposit_receipt_version: Option<u64>,
) -> Result<RebuiltDeposit, Rule> {
    let mut encoded = Vec::with_capacity(tx.eip2718_encoded_length());
    encoded.push(DEPOSIT_TX_TYPE_ID);
    tx.rlp_encode(&mut encoded);
    let rebuilt = keccak256(&encoded);
    if rebuilt != reported.hash {
        return Err(Rule::Hash {
            kind,
            reported: reported.hash,
            rebuilt,
        });
    }
    Ok(RebuiltDeposit {
        tx,
        encoded,
        deposit_nonce,
        deposit_receipt_version,
    })
}

/// The `opaqueData` bytes inside a `TransactionDeposited` log's data: the ABI encoding of one
/// `bytes` value, which is an offset word (32), a length word, and the bytes padded to a word.
fn opaque_data(data: &[u8]) -> Option<&[u8]> {
    let offset = U256::from_be_bytes(field::<WORD>(data, 0)?);
    let length = usize::try_from(U256::from_be_bytes(field::<WORD>(data, WORD)?)).ok()?;
    let end = length.checked_add(2 * WORD)?;
    (offset == U256::from(WORD) && data.len() == end.checked_next_multiple_of(WORD)?)
        .then(|| data.get(2 * WORD..end))?
}

/// The `N` bytes of `bytes` starting at `at`, if it is that long.
fn field<const N: usize>(bytes: &[u8], at: usize) -> Option<[u8; N]> {
    bytes.get(at..at.checked_add(N)?)?.try_into().ok()
}
