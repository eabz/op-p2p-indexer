"""Read a bounded block range from an op-p2p-indexer node with Arrow Flight."""
import argparse
from pathlib import Path
import pyarrow.flight as flight


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoint', default='grpc://127.0.0.1:50051')
    parser.add_argument('--from-block', type=int, required=True)
    parser.add_argument('--to-block', type=int, required=True)
    parser.add_argument('--cap', choices=('any', 'safe', 'finalized'), default='any')
    parser.add_argument('--api-key-file', type=Path)
    args = parser.parse_args()
    if not 0 <= args.from_block <= args.to_block or args.to_block - args.from_block >= 100_000:
        parser.error('choose an inclusive range of 1 to 100,000 non-negative blocks')
    headers = []
    if args.api_key_file:
        headers.append((b'authorization', b'Bearer ' + args.api_key_file.read_bytes().strip()))
    options = flight.FlightCallOptions(timeout=30, headers=headers)
    client = flight.FlightClient(args.endpoint)
    ticket = flight.Ticket(f'blocks:{args.from_block}:{args.to_block}:{args.cap}'.encode())
    expected = args.from_block
    for chunk in client.do_get(ticket, options=options):
        if chunk.data is None:
            continue
        numbers = chunk.data.column('number').to_pylist()
        if numbers != list(range(expected, expected + len(numbers))):
            raise RuntimeError(f'non-contiguous block output at {expected}; checkpoint not advanced')
        expected += len(numbers)
        print(f'rows={len(numbers)} next_block={expected} bytes={chunk.data.nbytes}')
    if expected != args.to_block + 1:
        raise RuntimeError(f'incomplete range: expected through {args.to_block}, received through {expected - 1}')
    print('Range complete. The selected cap determines its finality; any may reorganize.')


if __name__ == '__main__':
    main()
