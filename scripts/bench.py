#!/usr/bin/env python3
"""Arrow Flight bench: reads a block range of one table through the balancer, in parallel.

Asks the balancer (`GetFlightInfo`) to plan the range into jobs, each naming up to three
servers, then runs the jobs across `--processes` processes with `--threads` threads each, so one
Python process (its GIL, its gRPC client) is not what limits the read. A job reads its ticket
from its first location; on UNAVAILABLE or RESOURCE_EXHAUSTED (a server down or full) it moves
to the next, and after the last starts again from the first, up to `--retries` rounds. Other
errors fail the job.

Prints progress every few seconds, then: jobs done and failed, MB/s and rows/s, retries, time
to first batch (median and p95), and per server its jobs and MB/s. MB are the Arrow bytes
received, decoded (compressed IPC is counted after decompression).

    KEY=<api key> scripts/bench.py --balancer grpc://balancer:50060 \\
        --table logs --from 120000000 --to 120100000 --processes 4 --threads 8

Run it from two machines at once to tell a client limit from a server limit: if the sum of
the two runs' MB/s is about twice one run's, the client was the limit. See docs/serving.md
section 7. Needs pyarrow (`pip install pyarrow`).
"""

import argparse
import multiprocessing
import os
import queue
import sys
import threading
import time
from collections import defaultdict

import pyarrow.flight as flight

# What a failover is for: UNAVAILABLE (the server is down or shutting down) or
# RESOURCE_EXHAUSTED (at its limit). pyarrow raises these as several exception types, so the
# status is matched in the message too.
RETRYABLE = ("unavailable", "resource_exhausted", "resource exhausted")


def retryable(err):
    if isinstance(err, flight.FlightUnavailableError):
        return True
    text = str(err).lower()
    return any(word in text for word in RETRYABLE)


def options(key, compression):
    headers = [(b"authorization", b"Bearer " + key.encode())]
    if compression != "none":
        headers.append((b"op-indexer-compression", compression.encode()))
    return flight.FlightCallOptions(headers=headers)


def record(index, server, retries, rows=0, nbytes=0, ttfb=None, seconds=0.0, error=None):
    """One job's result."""
    return {
        "index": index,
        "server": server,
        "rows": rows,
        "bytes": nbytes,
        "ttfb": ttfb,
        "seconds": seconds,
        "retries": retries,
        "error": error,
    }


def read_job(clients, job, call, retries):
    """Reads one job; returns its result record."""
    index, ticket, locations = job
    failovers = 0
    error = None
    for attempt in range(retries + 1):
        for location in locations:
            started = time.monotonic()
            try:
                client = clients.get(location)
                if client is None:
                    client = flight.connect(location)
                    clients[location] = client
                rows = 0
                nbytes = 0
                ttfb = None
                for chunk in client.do_get(flight.Ticket(ticket), call):
                    if ttfb is None:
                        ttfb = time.monotonic() - started
                    rows += chunk.data.num_rows
                    nbytes += chunk.data.nbytes
                seconds = time.monotonic() - started
                return record(index, location, failovers, rows, nbytes,
                              ttfb if ttfb is not None else seconds, seconds)
            except Exception as err:  # noqa: BLE001: a job's failure is reported, not raised
                error = describe(err)
                if not retryable(err):
                    return record(index, location, failovers, error=error)
                failovers += 1
        time.sleep(min(2.0, 0.2 * (attempt + 1)))
    return record(index, locations[-1] if locations else "-", failovers, error=error)


def describe(err):
    """`Type: first line of the message`, whatever the message (it may be empty)."""
    lines = str(err).strip().splitlines()
    return "{}: {}".format(type(err).__name__, lines[0] if lines else repr(err))


def worker(jobs, results, threads, key, compression, retries):
    """One process: `threads` threads taking jobs until each gets a stop marker.

    Each job is announced ("started", index, pid) before it is read and reported ("done",
    record) after, whatever happens in it, so the parent knows which jobs a process that dies
    took with it."""
    call = options(key, compression)
    pid = os.getpid()

    def run():
        clients = {}
        while True:
            job = jobs.get()
            if job is None:
                return
            index = job[0]
            results.put(("started", index, pid))
            try:
                result = read_job(clients, job, call, retries)
            except BaseException as err:  # noqa: BLE001: reported as the job's failure
                result = record(index, "-", 0, error=describe(err))
            results.put(("done", result))

    pool = [threading.Thread(target=run, daemon=True) for _ in range(threads)]
    for thread in pool:
        thread.start()
    for thread in pool:
        thread.join()


def plan(balancer, key, table, start, end, cap):
    """The balancer's jobs for the range: (index, ticket bytes, locations)."""
    client = flight.connect(balancer)
    command = "{}:{}:{}:{}".format(table, start, end, cap).encode()
    info = client.get_flight_info(flight.FlightDescriptor.for_command(command), options(key, "none"))
    return [
        (index, endpoint.ticket.ticket, [location.uri.decode() for location in endpoint.locations])
        for index, endpoint in enumerate(info.endpoints)
    ]


def percentile(values, share):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    return ordered[min(len(ordered) - 1, int(share * len(ordered)))]


def ended(exitcode):
    """A process's exit code in words: a negative one is the signal that ended it."""
    if exitcode is None:
        return "still running"
    if exitcode < 0:
        return "killed by signal {}".format(-exitcode)
    return "exit code {}".format(exitcode)


def mb(nbytes):
    return nbytes / 1e6


def totals(results):
    """Bytes, rows and retries of `results`."""
    return (
        sum(result["bytes"] for result in results),
        sum(result["rows"] for result in results),
        sum(result["retries"] for result in results),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--balancer", required=True, help="grpc://host:port of the balancer")
    parser.add_argument("--table", required=True, choices=["blocks", "transactions", "receipts", "logs"])
    parser.add_argument("--from", dest="start", type=int, required=True, help="first block")
    parser.add_argument("--to", dest="end", type=int, required=True, help="last block")
    parser.add_argument("--cap", default="finalized", choices=["finalized", "safe", "any"])
    parser.add_argument("--processes", type=int, default=4)
    parser.add_argument("--threads", type=int, default=4, help="threads per process")
    parser.add_argument("--compression", default="none", choices=["none", "lz4", "zstd"])
    parser.add_argument("--retries", type=int, default=3, help="rounds over a job's locations")
    parser.add_argument("--progress", type=float, default=5.0, help="seconds between lines")
    args = parser.parse_args()

    key = os.environ.get("KEY")
    if not key:
        sys.exit("set KEY to an API key the servers accept")

    jobs = plan(args.balancer, key, args.table, args.start, args.end, args.cap)
    if not jobs:
        sys.exit("the balancer planned no jobs for this range")
    print(
        "{} jobs for {} {}..{} ({}), {} processes x {} threads, compression {}".format(
            len(jobs), args.table, args.start, args.end, args.cap,
            args.processes, args.threads, args.compression,
        ),
        flush=True,
    )

    # gRPC does not survive fork: every process starts fresh.
    context = multiprocessing.get_context("spawn")
    job_queue = context.Queue()
    results = context.Queue()
    for job in jobs:
        job_queue.put(job)
    for _ in range(args.processes * args.threads):
        job_queue.put(None)
    started = time.monotonic()
    processes = [
        context.Process(
            target=worker,
            args=(job_queue, results, args.threads, key, args.compression, args.retries),
            daemon=True,
        )
        for _ in range(args.processes)
    ]
    for process in processes:
        process.start()

    by_index = {}
    in_flight = {}  # job index -> pid of the process reading it
    reported = set()  # pids whose end was reported
    last_line = started
    while len(by_index) < len(jobs):
        try:
            message = results.get(timeout=1.0)
        except queue.Empty:
            # Nothing for a second: whatever a dead process sent has been read, so the jobs it
            # still held are lost with it.
            for process in processes:
                if process.is_alive() or process.pid in reported:
                    continue
                reported.add(process.pid)
                lost = [index for index, pid in in_flight.items() if pid == process.pid]
                if process.exitcode != 0 or lost:
                    print("worker process {} ended ({}), {} jobs lost with it".format(
                        process.pid, ended(process.exitcode), len(lost)), file=sys.stderr)
                for index in lost:
                    del in_flight[index]
                    by_index[index] = record(
                        index, "-", 0,
                        error="its worker process ended ({}) during the job".format(
                            ended(process.exitcode)))
            if not any(process.is_alive() for process in processes):
                break
        else:
            if message[0] == "started":
                in_flight[message[1]] = message[2]
            else:
                result = message[1]
                in_flight.pop(result["index"], None)
                by_index[result["index"]] = result
        now = time.monotonic()
        if now - last_line >= args.progress:
            last_line = now
            elapsed = now - started
            nbytes, rows, retries = totals(by_index.values())
            print(
                "{:7.1f}s  {}/{} jobs  {:8.1f} MB/s  {:10.0f} rows/s  {} retries".format(
                    elapsed, len(by_index), len(jobs), mb(nbytes) / elapsed, rows / elapsed,
                    retries,
                ),
                flush=True,
            )
    # Every planned job has a result: a job no process reported is a failure too.
    for index, _ticket, _locations in jobs:
        if index not in by_index:
            by_index[index] = record(index, "-", 0, error="not run: every worker process ended")
    done = [by_index[index] for index, _ticket, _locations in jobs]
    elapsed = time.monotonic() - started
    for process in processes:
        process.join(timeout=5)

    ok = [result for result in done if result["error"] is None]
    errors = [result for result in done if result["error"] is not None]
    nbytes, rows, _ = totals(ok)
    ttfbs = [result["ttfb"] for result in ok if result["ttfb"] is not None]
    print()
    print("jobs       {} planned: {} done, {} failed, {} retries".format(
        len(jobs), len(ok), len(errors), totals(done)[2]))
    print("time       {:.1f} s".format(elapsed))
    print("read       {:.1f} MB, {} rows".format(mb(nbytes), rows))
    print("rate       {:.1f} MB/s, {:.0f} rows/s".format(mb(nbytes) / elapsed, rows / elapsed))
    print("ttfb       median {:.3f} s, p95 {:.3f} s".format(
        percentile(ttfbs, 0.5), percentile(ttfbs, 0.95)))
    per_server = defaultdict(lambda: [0, 0, 0.0])
    for result in ok:
        entry = per_server[result["server"]]
        entry[0] += 1
        entry[1] += result["bytes"]
        entry[2] += result["seconds"]
    print("per server (jobs, MB/s over the run, MB/s per stream):")
    for server in sorted(per_server):
        jobs_done, server_bytes, busy = per_server[server]
        print("  {:40s} {:5d} jobs  {:8.1f} MB/s  {:7.1f} MB/s".format(
            server, jobs_done, mb(server_bytes) / elapsed,
            mb(server_bytes) / busy if busy else 0.0))
    for result in errors[:10]:
        print("failed job {} at {}: {}".format(result["index"], result["server"], result["error"]))
    if len(errors) > 10:
        print("... and {} more failures".format(len(errors) - 10))
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
