# Read your first dataset

Use Python 3.10+ with `pyarrow` on the node's host. The API example below targets the v0.1.12 protocol and reads a small inclusive range from a direct node, not a balancer.

```bash
python3 -m venv .venv
.venv/bin/pip install pyarrow
```

Find a currently available block number with `GetHeads` as described in [live updates](live.md), or use a recently received height in node logs. Replace `START` and `END` below with ten available blocks (`END = START + 9`). Genesis is not available on a newly started live node.

```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://127.0.0.1:50051")
start, end = START, END
ticket = flight.Ticket(f"blocks:{start}:{end}:any".encode())
reader = client.do_get(ticket, options=flight.FlightCallOptions(timeout=30))
for chunk in reader:
    if chunk.data is not None:
        print(chunk.data.to_pydict())
```

For a directly runnable example with numeric arguments, [download first_query.py](../examples/first_query.py):

```bash
.venv/bin/python first_query.py --from-block 40000000 --to-block 40000009
```

Those numbers are illustrative: choose a range your node actually holds. The script checks contiguous block heights and prints batch summaries. It does not validate cryptography or finality. `--endpoint` selects another node, `--cap finalized` requests finalized data, and `--api-key-file` supplies a configured client key from a private file.

## Interpret the result

`any` includes the unsafe chain. Such blocks can be reorganized; do not treat this query as settled accounting. Use a safe/finalized cap only when the node tracks those heads, and read [what those labels mean](trust.md).

Hash and address columns are binary; convert them to hex for display. Amounts represented as 32-byte values are big-endian integers. Timestamps are Unix seconds. Check `has_receipts`: receipts not yet fetched are different from an empty list of receipts.

The same ticket shape supports `transactions`, `receipts` and `logs`. Ranges are inclusive and bounded by the API; split large reads into small jobs and stream record batches instead of collecting an entire chain in memory. A balancer uses `GetFlightInfo` to plan endpoints; a direct `DoGet` example is not a fleet client.

## When a read fails

| Result | Next step |
|---|---|
| Connection refused | Confirm process, configured port and local bind address. |
| Unauthenticated | Configure the matching bearer key; use a private key file. |
| Out of range | Choose held blocks; check that the requested cap exists. |
| Timeout or resource exhausted | Reduce range/concurrency and inspect server load before retrying. |
| Missing receipts | Follow receipt events or read again after receipts arrive. |

Do not skip failed ranges silently. Persist checkpoints only after writing output successfully. See [the API reference](../stream.md) for schemas, metadata and limits.
