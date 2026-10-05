"""Exercise the published query against a local Flight fixture, and compile its proto."""
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import pyarrow as pa
import pyarrow.flight as flight

ROOT = Path(__file__).resolve().parent


class Fixture(flight.FlightServerBase):
    def do_get(self, context, ticket):
        table, first, last, cap = ticket.ticket.decode().split(':')
        assert table == 'blocks' and cap == 'any'
        numbers = list(range(int(first), int(last) + 1))
        # Two ranges deliberately return invalid output to check client diagnostics.
        if int(first) == 20:
            numbers.pop(2)
        elif int(first) == 30:
            numbers.pop()
        batch = pa.record_batch([pa.array(numbers, type=pa.uint64())], names=['number'])
        return flight.RecordBatchStream(pa.Table.from_batches([batch]))


def main():
    docs = ROOT / '.site/docs'
    assert (docs / 'examples/first_query.py').read_bytes() == (ROOT.parent / 'docs/examples/first_query.py').read_bytes()
    server = Fixture('grpc://127.0.0.1:0')
    thread = threading.Thread(target=server.serve, daemon=True)
    thread.start()
    try:
        for start, error in ((10, None), (20, 'non-contiguous'), (30, 'incomplete range')):
            result = subprocess.run([
                sys.executable, str(docs / 'examples/first_query.py'),
                '--endpoint', f'grpc://127.0.0.1:{server.port}',
                '--from-block', str(start), '--to-block', str(start + 9),
            ], capture_output=True, text=True, timeout=15)
            if error:
                assert result.returncode != 0 and error in result.stderr, result.stderr
            else:
                assert result.returncode == 0 and 'Range complete' in result.stdout, result.stderr
    finally:
        server.shutdown()
        thread.join(timeout=5)
    with tempfile.TemporaryDirectory() as output:
        subprocess.run([
            sys.executable, '-m', 'grpc_tools.protoc',
            f'--proto_path={docs / "examples"}', f'--python_out={output}',
            f'--grpc_python_out={output}', str(docs / 'examples/stream.proto'),
        ], check=True)
    print('Published example: complete range succeeds; gaps/truncation fail; released proto compiles')


if __name__ == '__main__':
    main()
