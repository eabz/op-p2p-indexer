# Follow live updates

The gRPC service delivers history followed by live chain events. It is useful when your database must stay current after a backfill. [Download its schema](../examples/stream.proto) to generate a client.

With `grpcurl` installed, query the local node's heads using the supplied schema. Server reflection is not required:

```bash
grpcurl -plaintext -import-path . -proto stream.proto \
  -d '{}' 127.0.0.1:50051 opindexer.v1.Stream/GetHeads
```

Read the next live events:

```bash
grpcurl -plaintext -max-msg-sz 67108864 -import-path . -proto stream.proto \
  -d '{"fromHead":true,"payload":"PAYLOAD_DECODED"}' \
  127.0.0.1:50051 opindexer.v1.Stream/Subscribe
```

These commands require the downloaded `stream.proto` in your current directory. For a configured key, add an `authorization: Bearer …` header. Plaintext is appropriate for loopback here; use TLS termination or a private tunnel for remote access.

## Build a consumer that can recover

Store `(chain_id, block_number, block_hash)` with every block. Keep your persisted cursor in the same transaction as your output, so a crashed consumer never acknowledges unwritten rows. Restart `Subscribe` with `fromNumber` at the last durable block and deduplicate by hash; the protocol has no client acknowledgment or durable resume token.

| Event | Consumer action |
|---|---|
| Block | Check parent hash and write block/transactions with its current status. |
| Receipts | Attach receipts/logs by block hash. Absence in a prior block was not an empty result. |
| Reorg | Invalidate the removed canonical blocks from the supplied height and undo derived records; replacement blocks follow. |
| Heads | Update the interpretation of previously received blocks at or below the heads. |

After a reconnect, reconcile your stored hashes with the node before treating the resumed range as canonical. A server change can have different coverage; retrying against another server alone does not establish continuity. Failover during a read is not yet proven; see [known limits](readiness.md).

Raise your client's receive limit to 64 MiB, bound work queues and surface stalled progress. Late receipts and reorganizations must be part of the application model, including for data initially read through Flight. See [stream semantics](../stream.md) and [trust boundaries](trust.md).
