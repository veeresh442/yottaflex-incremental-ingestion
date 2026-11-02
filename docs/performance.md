# Performance Notes

## Recorded run

| Measure | Result |
| --- | ---: |
| Baseline rows requested | 100,000 |
| CSV rows including built-in edge cases | 100,006 |
| Applied events | 100,002 |
| Stale events | 0 |
| Duplicate events | 1 |
| Rejected events | 3 |
| Batch size | 1,000 |
| Random seed | 42 |
| Elapsed ingestion time | 9.040 seconds |
| Throughput | 11,062.6 rows/second |
| Environment | Python 3.11.9 on Windows 11 |

The benchmark timer covers source staging, extraction, validation, SQLite writes, and run metrics. It excludes synthetic data generation. Hardware details were not recorded, so this result is specific to this run and is not a performance guarantee.

Reproduce it with:

~~~powershell
python -m yottaflex_ingestion benchmark --records 100000 --seed 42 --batch-size 1000
~~~

## Optimization

The pipeline stages records in SQLite and commits writes in configurable batches, which limits Python memory use and reduces commit overhead. A reasonable next optimization is to measure several batch sizes and evaluate prepared or set-based SQL for per-event lookups. Check that reconciliation results stay the same before adopting an optimization.

The benchmark command measures ingestion only and does not generate a reconciliation replay. The regular run command also produces a source-to-target comparison by replaying the full CSV into a temporary database, so its total wall time includes an additional pass.
