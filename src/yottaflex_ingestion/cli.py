from __future__ import annotations

import argparse
import json
import logging
import tempfile
import time
from pathlib import Path

from yottaflex_ingestion.generator import generate_events
from yottaflex_ingestion.pipeline import run_ingestion
from yottaflex_ingestion.reporting import write_reconciliation_report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="yottaflex-ingest",
        description="Generate, ingest, and reconcile reproducible CSV change events.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    generate = commands.add_parser("generate", help="write a deterministic synthetic event CSV")
    generate.add_argument("--output", default="data/source/sample.csv")
    generate.add_argument("--records", type=int, default=100)
    generate.add_argument("--seed", type=int, default=42)
    generate.add_argument("--without-edge-cases", action="store_true")

    ingest = commands.add_parser("run", help="ingest a CSV file into SQLite")
    ingest.add_argument("--input", default="data/source/sample.csv")
    ingest.add_argument("--database", default="database/ingestion.db")
    ingest.add_argument("--source-name", default="default")
    ingest.add_argument("--batch-size", type=int, default=500)
    ingest.add_argument("--fail-after-batches", type=int)
    ingest.add_argument("--report", default=None)

    benchmark = commands.add_parser("benchmark", help="measure a reproducible generated ingestion")
    benchmark.add_argument("--records", type=int, default=100_000)
    benchmark.add_argument("--seed", type=int, default=42)
    benchmark.add_argument("--batch-size", type=int, default=1000)

    report = commands.add_parser("report", help="write a reconciliation report for a run")
    report.add_argument("--database", default="database/ingestion.db")
    report.add_argument("--run-id", required=True)
    report.add_argument("--input", default=None, help="full appendable CSV for source-to-target comparison")
    report.add_argument("--output", default=None)
    return parser


def _run_command(args: argparse.Namespace) -> int:
    result = run_ingestion(
        args.input,
        args.database,
        source_name=args.source_name,
        batch_size=args.batch_size,
        fail_after_batches=args.fail_after_batches,
    )
    report, report_path = write_reconciliation_report(
        args.database, result.run_id, args.report, source_path=args.input
    )
    print(
        json.dumps(
            {
                "run": result.to_dict(),
                "reconciliation": report["reconciliation"],
                "source_target_reconciliation": report["source_target_reconciliation"],
                "report_path": str(report_path),
            },
            indent=2,
        )
    )
    return 0 if result.status == "completed" else 1


def _benchmark(args: argparse.Namespace) -> int:
    with tempfile.TemporaryDirectory(prefix="yottaflex-benchmark-") as temporary:
        workspace = Path(temporary)
        source = workspace / "events.csv"
        database = workspace / "benchmark.db"
        generated_count = generate_events(source, args.records, args.seed)
        started = time.perf_counter()
        result = run_ingestion(source, database, batch_size=args.batch_size)
        elapsed = time.perf_counter() - started
        output = {
            "status": result.status,
            "requested_baseline_records": args.records,
            "generated_csv_rows": generated_count,
            "applied": result.applied_count,
            "stale": result.stale_count,
            "duplicates": result.duplicate_count,
            "rejected": result.rejected_count,
            "elapsed_seconds": round(elapsed, 3),
            "rows_per_second": round(generated_count / elapsed, 1) if elapsed else None,
            "batch_size": args.batch_size,
            "seed": args.seed,
        }
        print(json.dumps(output, indent=2))
        return 0 if result.status == "completed" else 1


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parser().parse_args(argv)
    if args.command == "generate":
        count = generate_events(
            args.output,
            args.records,
            args.seed,
            include_edge_cases=not args.without_edge_cases,
        )
        print(json.dumps({"output": str(Path(args.output)), "rows_written": count, "seed": args.seed}, indent=2))
        return 0
    if args.command == "run":
        return _run_command(args)
    if args.command == "benchmark":
        return _benchmark(args)
    if args.command == "report":
        report, report_path = write_reconciliation_report(
            args.database, args.run_id, args.output, source_path=args.input
        )
        print(json.dumps({
            "report_path": str(report_path),
            "reconciliation": report["reconciliation"],
            "source_target_reconciliation": report["source_target_reconciliation"],
        }, indent=2))
        return 0 if report["reconciliation"]["passed"] else 1
    return 2
