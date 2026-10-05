//! Flight table identifiers and range tickets shared by consumers and servers.
use alloy_primitives::BlockNumber;
use arrow_flight::{FlightDescriptor, Ticket};
use tonic::Status;
/// Maximum blocks in one Flight response.
pub const MAX_FLIGHT_BLOCKS: u64 = 100_000;
/// A table Flight serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Table {
    /// One row per block: the header.
    Blocks,
    /// One row per transaction.
    Transactions,
    /// One row per receipt.
    Receipts,
    /// One row per log.
    Logs,
}

impl Table {
    /// Every table, in the order they are listed.
    pub const ALL: [Self; 4] = [Self::Blocks, Self::Transactions, Self::Receipts, Self::Logs];

    /// The table's name, in descriptors and tickets.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Blocks => "blocks",
            Self::Transactions => "transactions",
            Self::Receipts => "receipts",
            Self::Logs => "logs",
        }
    }

    /// The table named `name`.
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|table| table.name() == name)
    }
}
/// How far a range may reach.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cap {
    /// Up to the finalized head.
    Finalized,
    /// Up to the safe head.
    Safe,
    /// Up to the unsafe head: blocks above the archive come from the unsafe store, and may
    /// still be reorged.
    Any,
}

impl Cap {
    const ALL: [Self; 3] = [Self::Finalized, Self::Safe, Self::Any];

    /// The cap's name in a ticket.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Finalized => "finalized",
            Self::Safe => "safe",
            Self::Any => "any",
        }
    }
}

/// What a ticket or a descriptor asks for: a table, an inclusive block range and a cap.
#[derive(Debug, Clone, Copy)]
pub struct Query {
    /// The table.
    pub table: Table,
    /// `None` for the lowest block held.
    pub from: Option<BlockNumber>,
    /// The last block, inclusive.
    pub to: BlockNumber,
    /// How far the range may reach.
    pub cap: Cap,
}

/// A request that names something not served.
fn not_served(what: &str, all: impl Iterator<Item = &'static str>) -> Status {
    Status::invalid_argument(format!("{what}: {}", all.collect::<Vec<_>>().join(", ")))
}

impl Query {
    /// The whole of `table`, at any status.
    pub const fn whole(table: Table) -> Self {
        Self {
            table,
            from: None,
            to: BlockNumber::MAX,
            cap: Cap::Any,
        }
    }

    /// Reads `table:from:to[:cap]`.
    ///
    /// # Errors
    ///
    /// `INVALID_ARGUMENT` if the text is not a ticket, or names a table or cap not served.
    pub fn parse(text: &[u8]) -> Result<Self, Status> {
        let invalid = || Status::invalid_argument("a ticket is `table:from:to[:cap]`");
        let text = std::str::from_utf8(text).map_err(|_not_text| invalid())?;
        let mut parts = text.split(':');
        let table = parts
            .next()
            .and_then(Table::parse)
            .ok_or_else(|| not_served("tables", Table::ALL.into_iter().map(Table::name)))?;
        let mut number = || {
            parts
                .next()
                .and_then(|part| part.parse::<BlockNumber>().ok())
                .ok_or_else(invalid)
        };
        let (from, to) = (number()?, number()?);
        if to < from {
            return Err(Status::invalid_argument("`to` is below `from`"));
        }
        let cap = match parts.next() {
            None => Cap::Any,
            Some(name) => Cap::ALL
                .into_iter()
                .find(|cap| cap.name() == name)
                .ok_or_else(|| not_served("caps", Cap::ALL.into_iter().map(Cap::name)))?,
        };
        if parts.next().is_some() {
            return Err(invalid());
        }
        Ok(Self {
            table,
            from: Some(from),
            to,
            cap,
        })
    }

    /// The ticket of the query: `table:from:to:cap`, `from` 0 when it names none.
    #[must_use]
    pub fn ticket(self) -> Ticket {
        let text = format!(
            "{}:{}:{}:{}",
            self.table.name(),
            self.from.unwrap_or(0),
            self.to,
            self.cap.name()
        );
        Ticket {
            ticket: text.into(),
        }
    }
}

impl TryFrom<&FlightDescriptor> for Query {
    type Error = Status;

    /// A path of one table (all of it), or a ticket's text as the command.
    fn try_from(descriptor: &FlightDescriptor) -> Result<Self, Status> {
        match descriptor.path.as_slice() {
            [table] => Table::parse(table)
                .map(Self::whole)
                .ok_or_else(|| not_served("tables", Table::ALL.into_iter().map(Table::name))),
            [] => Self::parse(&descriptor.cmd),
            _ => Err(Status::invalid_argument("a path names one table")),
        }
    }
}
