//! The known-forks horizon: the activation time of a fork this build does not know, once
//! enough peers agree on it. Information only: it warns, and never refuses a block or stops
//! the node.
//!
//! Execution peers announce their next fork as `next` in their fork id ([EIP-2124]). A `next`
//! this build does not know means a fork is coming that it cannot follow. Once
//! [`AGREEING_HOSTS`] distinct hosts announce the same unknown time in their eth status, that
//! time is the horizon, and the operator is told to upgrade before it. Only completed status
//! exchanges on our chain count. A host is an IPv4 address or an IPv6 /48 (a free tunnel hands
//! out a /48), and each host counts for one time only, its latest; times that have passed are
//! forgotten. Node records only warn: anyone can write one.
//!
//! [EIP-2124]: https://eips.ethereum.org/EIPS/eip-2124

use std::collections::HashMap;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use tracing::{info, warn};

use crate::session::{host, unix_now};
use crate::warn_limit::WarnLimit;

/// Distinct hosts that must announce the same unknown fork time before it is the horizon.
const AGREEING_HOSTS: usize = 3;
/// Unknown fork times farther ahead than this are ignored: forks are scheduled weeks ahead,
/// not years.
const MAX_AHEAD: Duration = Duration::from_hours(24 * 365);
/// Hosts whose announcement is remembered; past it, after forgetting passed times, a new
/// host is ignored. Each host completed an eth status on our chain.
const MAX_HOSTS: usize = 1024;
/// Shortest time between two reminders that a horizon is set.
const REMIND_INTERVAL: Duration = Duration::from_mins(10);

/// Tracks unknown fork times announced by peers, and warns about the horizon they set.
#[derive(Debug)]
pub(crate) struct Horizon {
    /// The network's name, for the warning.
    network: &'static str,
    /// The newest known fork time of this build, for the log.
    newest_known: Option<u64>,
    state: Mutex<State>,
    reminded: WarnLimit,
}

#[derive(Debug, Default)]
struct State {
    /// The unknown fork time each host announced last.
    announced: HashMap<IpAddr, u64>,
    /// The earliest time [`AGREEING_HOSTS`] hosts announce now, if any.
    horizon: Option<u64>,
}

impl Horizon {
    /// A horizon not set yet; `newest_known` is this build's last known fork time.
    pub(crate) fn new(network: &'static str, newest_known: Option<u64>) -> Self {
        Self {
            network,
            newest_known,
            state: Mutex::new(State::default()),
            reminded: WarnLimit::default(),
        }
    }

    /// Counts `next`, an unknown fork time announced in the eth status of a peer at `ip`.
    pub(crate) fn announced(&self, ip: IpAddr, next: u64) {
        let now = unix_now();
        if next <= now || next > now.saturating_add(MAX_AHEAD.as_secs()) {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.announced.retain(|_, time| *time > now);
        let site = site(ip);
        if !state.announced.contains_key(&site) && state.announced.len() >= MAX_HOSTS {
            return;
        }
        state.announced.insert(site, next);
        let mut agreeing: HashMap<u64, usize> = HashMap::new();
        for time in state.announced.values() {
            *agreeing.entry(*time).or_default() += 1;
        }
        let horizon = agreeing
            .into_iter()
            .filter(|(_, hosts)| *hosts >= AGREEING_HOSTS)
            .map(|(time, _)| time)
            .min();
        if horizon == state.horizon {
            return;
        }
        state.horizon = horizon;
        drop(state);
        if let Some(fork_time) = horizon {
            warn_upgrade(self.network, fork_time);
        }
    }

    /// Logs the horizon: at startup once, then a reminder at most every ten minutes while it
    /// is set.
    pub(crate) fn log(&self, startup: bool) {
        let horizon = self
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .horizon;
        match horizon {
            None if startup => info!(
                newest_known_fork = self.newest_known,
                "no unknown hardfork announced by execution peers"
            ),
            Some(fork_time) if self.reminded.allow(REMIND_INTERVAL).is_some() => {
                warn_upgrade(self.network, fork_time);
            }
            _ => {}
        }
    }
}

fn warn_upgrade(network: &str, fork_time: u64) {
    warn!(
        network,
        fork_time,
        "execution peers announce a hardfork at this time that this build does not know: \
         upgrade before then"
    );
}

/// The host a peer counts as: its IPv4 address, or its IPv6 /48.
fn site(ip: IpAddr) -> IpAddr {
    match host(ip) {
        IpAddr::V6(v6) => {
            let [a, b, c, ..] = v6.segments();
            IpAddr::V6(Ipv6Addr::new(a, b, c, 0, 0, 0, 0, 0))
        }
        v4 @ IpAddr::V4(_) => v4,
    }
}
