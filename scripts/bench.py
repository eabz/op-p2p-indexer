#!/usr/bin/env python3
"""Arrow Flight bench with bounded per-server concurrency and strict range coverage.

Plans through the balancer, then reads directly from its servers. --per-server bounds
active RPCs across all processes/threads of THIS run; separate benchmark clients have
independent limits. Full/down servers are tried through fallback locations with backoff.
--retry-for bounds a started job including local slot waits; --rpc-timeout bounds each read
RPC and --plan-timeout bounds planning.
Queued jobs have not started their budget yet. A worker process failure aborts the run.

Progress counts decoded Arrow bytes as batches arrive, including unsuccessful attempts.
Final useful throughput counts successful jobs only; incomplete runs are marked explicitly.
MB means decimal 1,000,000 bytes, NOT network bytes (compression is decoded before counting).
TTFB and job latency include queueing and retries; planning is reported separately and in
the end-to-end rate. Latency is observed in the parent and includes IPC delivery. Empty
streams have no first-batch sample.

    scripts/bench.py --config config.toml --balancer grpc://balancer:50060 \
        --table logs --from 120000000 --to 120100000 --processes 4 --threads 8 --per-server 8

Needs pyarrow and Python 3.11+ (or tomli on older Python). Reads credentials from [bench]. Use matching ranges and report cold/warm runs separately.
"""

import argparse
import math
import multiprocessing
import os
import queue
import random
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

import pyarrow.flight as flight

# The failures a job is retried on, as pyarrow reports them: as several exception types, so the
# status is matched in the message too.
EXHAUSTED = ("resource_exhausted", "resource exhausted")
# Backoff after a round over a job's locations in which one was full (RESOURCE_EXHAUSTED):
# from the first, doubling each such round to the most, with jitter.
BACKOFF_FIRST = 0.2
BACKOFF_MOST = 5.0
# Pause after a round in which every location was down (UNAVAILABLE).
ROUND_PAUSE = 0.2


def failure(err):
    """Classifies retryable capacity, availability and deadline failures; others are fatal."""
    text = str(err).lower()
    if any(word in text for word in EXHAUSTED):
        return "exhausted"
    if isinstance(err, flight.FlightTimedOutError) or "deadline exceeded" in text or "deadline_exceeded" in text:
        return "timeout"
    if isinstance(err, flight.FlightUnavailableError) or "unavailable" in text:
        return "unavailable"
    return "other"


def options(key, compression, timeout):
    headers = [(b"authorization", b"Bearer " + key.encode())]
    if compression != "none":
        headers.append((b"op-indexer-compression", compression.encode()))
    return flight.FlightCallOptions(headers=headers, timeout=timeout)


def record(index, error=None, **values):
    """One job's useful output and cumulative work, including failed attempts."""
    result = dict(index=index, server="-", rows=0, bytes=0, received=0, failed_bytes=0,
                  ttfb=None, seconds=0.0, latency=0.0, queued=0.0, retries=0,
                  failures=0, exhausted=0, unavailable=0, timeout=0, other=0,
                  last_failure=None, cleanup_failures=0, waited=0.0, error=error)
    result.update(values)
    return result


def read_job(clients, job, args, permits, results):
    """Reads under a shared server limit and a finite budget, trying fallback locations."""
    index, ticket, locations = job
    began = time.monotonic()
    deadline = began + args.retry_for
    result = record(index)
    first_batch = False
    attempts = 0
    backoff = BACKOFF_FIRST
    last_progress = 0.0

    def progress(force=False):
        nonlocal last_progress
        now = time.monotonic()
        if force or now - last_progress >= 0.2:
            results.put(("progress", dict(result)))
            last_progress = now

    def finish(error=None):
        if error and error.startswith("job budget expired") and result["last_failure"]:
            error += "; last failure: " + result["last_failure"]
        result["error"] = error
        progress(True)
        return result

    while time.monotonic() < deadline:
        exhausted = False
        attempted = False
        for location in locations:
            if time.monotonic() >= deadline:
                break
            # Never wait on one location while a fallback has room. Slots cover the entire
            # RPC, including failure cleanup, across every process and thread in this run.
            if not permits[location].acquire(False):
                continue
            attempted = True
            started = time.monotonic()
            rows = nbytes = 0
            reader = None
            complete = False
            try:
                attempts += 1
                result["retries"] = attempts - 1
                result["server"] = location
                client = clients.get(location)
                if client is None:
                    client = flight.connect(location)
                    clients[location] = client
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return finish("job budget expired before RPC")
                call = options(args.key, args.compression, min(args.rpc_timeout, remaining))
                reader = client.do_get(flight.Ticket(ticket), call)
                for chunk in reader:
                    if chunk.data is None:
                        continue
                    if not first_batch:
                        first_batch = True
                        results.put(("first_batch", index))
                    rows += chunk.data.num_rows
                    nbytes += chunk.data.nbytes
                    result["received"] += chunk.data.nbytes
                    progress()
                complete = True
                result.update(rows=rows, bytes=nbytes, seconds=time.monotonic() - started)
                return finish()
            except Exception as err:  # a job failure is reported to the parent
                result["failed_bytes"] += nbytes
                result["failures"] += 1
                kind = failure(err)
                result[kind] += 1
                result["last_failure"] = describe(err)
                progress(True)
                if kind == "other":
                    return finish(result["last_failure"])
                exhausted = exhausted or kind == "exhausted"
            finally:
                try:
                    if reader is not None and not complete:
                        reader.cancel()
                except Exception:
                    # Keep the original failure and received-byte accounting. The RPC's
                    # deadline still bounds the read if its explicit cancellation fails.
                    result["cleanup_failures"] += 1
                finally:
                    permits[location].release()
        left = max(0.0, deadline - time.monotonic())
        if exhausted:
            pause = min(left, backoff * random.uniform(0.5, 1.0))
            backoff = min(BACKOFF_MOST, backoff * 2)
        else:
            pause = min(left, ROUND_PAUSE if attempted else 0.02)
        time.sleep(pause)
        result["waited"] += pause
    return finish("job budget expired after {:.1f} s (including slot waits)".format(args.retry_for))


def describe(err):
    """`Type: first line of the message`, whatever the message (it may be empty)."""
    lines = str(err).strip().splitlines()
    return "{}: {}".format(type(err).__name__, lines[0] if lines else repr(err))


def worker(jobs, results, args, permits):
    """One process with persistent clients per thread. Worker death aborts the whole run."""
    def run():
        clients = {}
        try:
            while True:
                job = jobs.get()
                if job is None:
                    return
                results.put(("started", job[0], os.getpid()))
                try:
                    result = read_job(clients, job, args, permits, results)
                except BaseException as err:
                    result = record(job[0], error=describe(err))
                results.put(("done", result))
        finally:
            for client in clients.values():
                client.close()

    pool = [threading.Thread(target=run, daemon=True) for _ in range(args.threads)]
    for thread in pool:
        thread.start()
    for thread in pool:
        thread.join()


def plan(args):
    """Requires tickets to cover exactly the requested range, without overlaps or gaps."""
    client = flight.connect(args.balancer)
    command = "{}:{}:{}:{}".format(args.table, args.start, args.end, args.cap).encode()
    try:
        info = client.get_flight_info(flight.FlightDescriptor.for_command(command),
                                     options(args.key, "none", args.plan_timeout))
    finally:
        client.close()
    jobs = []
    ranges = []
    for index, endpoint in enumerate(info.endpoints):
        ticket = endpoint.ticket.ticket
        parts = ticket.decode("utf-8").split(":")
        if len(parts) != 4 or parts[0] != args.table or parts[3] != args.cap:
            raise ValueError("plan contains an unexpected table, cap or ticket format")
        first, last = int(parts[1]), int(parts[2])
        if first < args.start or last > args.end or first > last:
            raise ValueError("plan contains a ticket outside the requested range")
        locations = list(dict.fromkeys(location.uri.decode() for location in endpoint.locations))
        if not locations:
            raise ValueError("plan contains a ticket without a server")
        ranges.append((first, last))
        jobs.append((index, ticket, locations))
    expected = args.start
    for first, last in sorted(ranges):
        if first != expected:
            raise ValueError("plan has a gap or overlap at block {}".format(expected))
        expected = last + 1
    if expected != args.end + 1:
        raise ValueError("plan ends at {}; requested through {}".format(expected - 1, args.end))
    return jobs


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


def benchmark_settings(path, stack=()):
    """Read inherited benchmark credentials; child fields override parent fields."""
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib
    path = Path(path).resolve(strict=True)
    if len(stack) >= 8 or path in stack:
        raise ValueError("configuration inheritance cycle or depth exceeds eight files")
    with path.open("rb") as source:
        document = tomllib.load(source)
    settings = document.get("bench", {})
    if not isinstance(settings, dict):
        raise ValueError("bench must be a table")
    # Validate each layer, including credentials that a child would replace.
    for key in ("api_key", "balancer_url"):
        if key in settings and not isinstance(settings[key], str):
            raise ValueError("benchmark credentials must be strings")
    parent = document.get("extends")
    if "extends" in document:
        if not isinstance(parent, str) or not parent:
            raise ValueError("extends must be a nonempty file path")
        inherited = benchmark_settings(path.parent / parent, (*stack, path))
        inherited.update(settings)
        return inherited
    return settings


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", default="config.toml", help="TOML file containing [bench] credentials")
    parser.add_argument("--balancer", help="grpc://host:port of the balancer")
    parser.add_argument("--table", required=True, choices=["blocks", "transactions", "receipts", "logs"])
    parser.add_argument("--from", dest="start", type=int, required=True, help="first block")
    parser.add_argument("--to", dest="end", type=int, required=True, help="last block")
    parser.add_argument("--cap", default="finalized", choices=["finalized", "safe", "any"])
    parser.add_argument("--processes", type=int, default=4)
    parser.add_argument("--threads", type=int, default=4, help="threads per process")
    parser.add_argument("--per-server", type=int, default=8,
                        help="maximum concurrent reads per server across this run (default 8)")
    parser.add_argument("--plan-timeout", type=float, default=30.0,
                        help="deadline in seconds for planning (default 30)")
    parser.add_argument("--rpc-timeout", type=float, default=120.0,
                        help="deadline in seconds for each read RPC (default 120)")
    parser.add_argument("--compression", default="none", choices=["none", "lz4", "zstd"])
    parser.add_argument("--retry-for", type=float, default=120.0,
                        help="seconds a job is retried for, from its start")
    parser.add_argument("--progress", type=float, default=5.0, help="seconds between lines")
    args = parser.parse_args()

    for name in ("processes", "threads", "per_server", "rpc_timeout", "plan_timeout", "retry_for", "progress"):
        value = getattr(args, name)
        if value <= 0 or (isinstance(value, float) and not math.isfinite(value)):
            parser.error("--{} must be positive and finite".format(name.replace("_", "-")))
    if not 0 <= args.start <= args.end <= 2**64 - 1:
        parser.error("require 0 <= --from <= --to <= 2^64-1")
    try:
        settings = benchmark_settings(args.config)
        args.key = settings.get("api_key")
        args.balancer = args.balancer or settings.get("balancer_url")
    except ImportError:
        parser.error("TOML support needs Python 3.11+ or the tomli package")
    except (OSError, ValueError, AttributeError):
        parser.error("cannot read TOML benchmark configuration (contents withheld)")
    if not isinstance(args.key, str) or not args.key:
        parser.error("set bench.api_key in the TOML file")
    if not isinstance(args.balancer, str) or not args.balancer:
        parser.error("set bench.balancer_url or give --balancer")

    overall_started = time.monotonic()
    try:
        jobs = plan(args)
    except Exception as err:
        sys.exit("planning failed: " + describe(err))
    planning_seconds = time.monotonic() - overall_started
    print(
        "{} jobs for {} {}..{} ({}), {} processes x {} threads, compression {}".format(
            len(jobs), args.table, args.start, args.end, args.cap,
            args.processes, args.threads, args.compression,
        ),
        flush=True,
    )

    print("local cap  {} concurrent reads per server (not discovered capacity)".format(args.per_server),
          flush=True)

    # gRPC does not survive fork: every process starts fresh.
    context = multiprocessing.get_context("spawn")
    locations = {location for _, _, servers in jobs for location in servers}
    permits = {location: context.BoundedSemaphore(args.per_server) for location in locations}
    job_queue = context.Queue()
    results = context.Queue()
    enqueued = time.monotonic()
    for job in jobs:
        job_queue.put(job)
    for _ in range(args.processes * args.threads):
        job_queue.put(None)
    started = time.monotonic()
    processes = [
        context.Process(
            target=worker,
            args=(job_queue, results, args, permits),
            daemon=True,
        )
        for _ in range(args.processes)
    ]
    for process in processes:
        process.start()

    by_index = {}
    progress_by_index = {}
    queued = {}
    first_batches = {}
    # All cross-process latency is measured on receipt in the parent. Older Python/macOS
    # monotonic clocks have different process origins; IPC delivery is included in latency.
    last_line = started
    aborted = None
    # Also bound native client/process failures that cannot deliver a Python exception.
    watchdog = started + args.retry_for * len(jobs) + args.rpc_timeout + 30
    try:
        while len(by_index) < len(jobs):
            dead = [p for p in processes if p.exitcode not in (None, 0)]
            if dead:
                aborted = "worker {} ended ({})".format(dead[0].pid, ended(dead[0].exitcode))
                break
            if time.monotonic() >= watchdog:
                aborted = "run watchdog expired"
                break
            try:
                message = results.get(timeout=0.2)
            except queue.Empty:
                if not any(p.is_alive() for p in processes):
                    aborted = "workers ended before all jobs were reported"
                    break
            else:
                observed = time.monotonic() - enqueued
                if message[0] == "started":
                    queued[message[1]] = observed
                elif message[0] == "first_batch":
                    first_batches[message[1]] = observed
                else:
                    result = message[1]
                    index = result["index"]
                    result["queued"] = queued.get(index, 0.0)
                    result["ttfb"] = first_batches.get(index)
                    progress_by_index[index] = result
                    if message[0] == "done":
                        result["latency"] = observed
                        by_index[index] = result
            now = time.monotonic()
            if now - last_line >= args.progress:
                last_line = now
                elapsed = now - started
                received = sum(r["received"] for r in progress_by_index.values())
                failed = sum(r["failed_bytes"] for r in progress_by_index.values())
                print("{:7.1f}s  {}/{} jobs  {:.1f} MB received ({:.1f} failed-attempt MB), "
                      "{:.1f} decoded MB/s".format(elapsed, len(by_index), len(jobs),
                                                  mb(received), mb(failed), mb(received) / elapsed),
                      flush=True)
    except KeyboardInterrupt:
        aborted = "interrupted"
    finally:
        if aborted:
            print("aborted: " + aborted, file=sys.stderr)
        for process in processes:
            if aborted and process.is_alive():
                process.terminate()
        for process in processes:
            process.join(timeout=5)
            if process.is_alive():
                process.kill()
                process.join()
        # Do not wait for the feeder to flush jobs after workers have been terminated.
        job_queue.cancel_join_thread()
        job_queue.close()
        results.close()
    for index, _ticket, _locations in jobs:
        if index not in by_index:
            result = dict(progress_by_index.get(index, record(index)))
            result["error"] = aborted or "worker did not report completion"
            # No completed result proves these partial bytes useful; account for all of them.
            result["failed_bytes"] = result["received"]
            result["bytes"] = result["rows"] = 0
            by_index[index] = result
    done = [by_index[index] for index, _ticket, _locations in jobs]
    elapsed = time.monotonic() - started
    overall_elapsed = time.monotonic() - overall_started

    ok = [result for result in done if result["error"] is None]
    errors = [result for result in done if result["error"] is not None]
    nbytes, rows, _ = totals(ok)
    retries = totals(done)[2]
    waited = sum(result["waited"] for result in done)
    ttfbs = [result["ttfb"] for result in done if result["ttfb"] is not None]
    print()
    print("jobs       {} planned: {} done, {} failed, {} retries, {:.1f} s slot/backoff waiting".format(
        len(jobs), len(ok), len(errors), retries, waited))
    print("time       {:.1f} s reading, {:.3f} s planning, {:.1f} s end-to-end".format(
        elapsed, planning_seconds, overall_elapsed))
    print("received   {:.1f} decoded MB, {:.1f} failed/incomplete MB (not wire bytes)".format(
        mb(sum(r["received"] for r in done)), mb(sum(r["failed_bytes"] for r in done))))
    if aborted:
        print("received counts are a lower bound: a terminated worker may not have reported its last batches")
    cleanup_failures = sum(r["cleanup_failures"] for r in done)
    if cleanup_failures:
        print("warning: {} stream cancellation failures; RPC deadlines remained active".format(cleanup_failures))
    print("read       {:.1f} MB, {} rows".format(mb(nbytes), rows))
    print("rate       {:.1f} MB/s, {:.0f} rows/s".format(mb(nbytes) / elapsed, rows / elapsed))
    print("end-to-end {:.1f} useful MB/s".format(mb(nbytes) / overall_elapsed))
    print("ttfb incl queue/retries  median {:.3f} s, p95 {:.3f} s".format(
        percentile(ttfbs, 0.5), percentile(ttfbs, 0.95)))
    latencies = [r["latency"] for r in done if r["latency"] > 0]
    queues = [r["queued"] for r in done]
    print("job latency incl queue/retries  median {:.3f} s, p95 {:.3f} s".format(
        percentile(latencies, 0.5), percentile(latencies, 0.95)))
    print("queue      median {:.3f} s, p95 {:.3f} s; failures {}".format(
        percentile(queues, 0.5), percentile(queues, 0.95), sum(r["failures"] for r in done)))
    print("attempt failures  " + ", ".join(
        "{} {}".format(kind, sum(r[kind] for r in done))
        for kind in ("exhausted", "unavailable", "timeout", "other")))
    if errors:
        print("INCOMPLETE RUN: successful-subset throughput is not a full-range benchmark")
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
    sys.exit(1 if errors or aborted else 0)


if __name__ == "__main__":
    main()
