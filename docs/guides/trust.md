# Trust and finality

Verification helps you detect corrupted or inconsistent data. It does not make this indexer an execution node, and each check answers a different question.

| Check or label | What it establishes | What it does not establish |
|---|---|---|
| Sequencer signature | Payload came from the configured chain signer. | Correct EVM execution or L1 settlement. |
| Block hashes and transaction/receipt roots | Data matches its header commitments. | Correct state transitions. |
| Unsafe | The node currently follows this block. | Permanence; the chain can reorganize. |
| Safe | With L1 tracking, a matching bonded claim was found on L1. | Full batch derivation or resolution of disputes. |
| Finalized | The L1 block containing the matching claim is finalized. | Independent execution validation of the L2 claim. |

L1 tracking starts from a trusted recent beacon checkpoint. Choose and refresh that checkpoint through your own trusted process. The live profile can operate without L1 tracking, but it then cannot provide the same safe/finalized coverage.

The project still needs stronger handling of a safe head that contradicts the unsafe chain and of respected dispute-game selection/claim retention. These limits affect settlement-sensitive applications. They are [readiness gates](readiness.md), not guarantees hidden behind a finality label.

Historical imports use external archive data, can fill missing fields through RPC, and are verified against the importer's configured anchors. An anchor is a trust input: record where it came from. A successful transport, checksum or Flight query is not a substitute for this verification.

For the implementation detail, read [L1 verification](../l1.md), [storage and fork choice](../storage.md) and [import verification](../import.md).
