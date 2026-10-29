---
tags:
  - Performance
  - DuckDB
---

# A reproducible DuckDB benchmark: one billion records

The page contains on the results of reproducable performance benchmark that evaluates the performance of Splink on a large dataset.

In the example, we generate a large synthetic dataset. Splink then used to deduplicate **1bn records, generating 10.04 billion scored candidate pairs, in 9 minutes and 18 seconds and at a cost of less than $1 USD**.

This was run on an AWS EC2 `r8id.metal-48xl` with 96 physical cores (192 logical CPUs) and 1,536 GiB of RAM,



## Results

Our script was run on DuckDB `2.0.0.dev2609121639` and Splink `5.0.0.dev5`.  The results were as follows

| Step | Elapsed time |
|---|---:|
| Generate 1bn records and write them to Parquet | 3m 2s |
| Run Splink prediction, including writing results to Parquet | 9m 18s |
| Complete script, including setup and validation | 12m 34s |

The prediction timer includes blocking, term-frequency calculations, comparison scoring and output. Its main queries took:

| Query | Elapsed time |
|---|---:|
| Blocking | 121.9s |
| Term frequencies for email and postcode | 2.3s |
| Comparison scoring and prediction output | 433.4s |

The query times exclude the small amount of Python and profiling overhead included in the prediction timer.



Input Parquet files occupied 17.25 GB and final predictions 382.29 GB, with 192 files in each. Peak DuckDB buffer memory was and 677.41 GB during prediction. There was no temprary spill to disk.

## What the script does

The data represents 400 million synthetic people, with equal numbers having one, two, three or four records. Names, surnames, postcodes and emails have skewed frequency distributions, so some blocking keys are much more common than others. Deterministic hash assignments generate the values without constructing a billion-row Python dataframe.

Four example records:

| `unique_id` | `name_1` | `name_2` | `surname` | `dob` | `postcode` | `email` |
|---|---|---|---|---|---|---|
| 1 | name_04561 | `NULL` | surname_098933 | `NULL` | postcode_17798 | user_031969@example.com |
| 2 | name_00002 | name_02858 | surname_056465 | 2005-06-20 | postcode_09411 | user_057431@example.com |
| 3 | name_00002 | name_02858 | surname_056465 | 2005-06-20 | postcode_09411 | user_057431@example.com |
| 4 | name_00157 | name_23154 | surname_034202 | 2012-09-10 | postcode_17112 | user_023034@example.com |

Records belonging to the same person have identical matching fields: the generator does not introduce typos or other disagreements between duplicates. This is a synthetic performance workload, not a test of linkage accuracy.

Splink deduplicates the full dataset using six blocking rules:

```python
from splink import block_on

blocking_rules_to_generate_predictions = [
    block_on("surname", "date_trunc('month', dob)"),
    block_on("postcode", "dob"),
    block_on("surname", "postcode"),
    block_on("surname", "email"),
    block_on("postcode", "email"),
    block_on("name_2", "postcode", "year(dob)"),
]
```

The model uses Jaro–Winkler comparisons for names, surname and email; Levenshtein comparisons for date of birth and postcode; and an exact comparison for phone number. Email and postcode include term-frequency adjustments.

The script uses the model's default parameters without fitting them. Model training and clustering are outside this benchmark.

Intermediate results and final predictions are materialised directly to local Parquet with ZSTD compression.

## Machine and software

| Setting | Value |
|---|---|
| EC2 instance | `r8id.metal-48xl`, `eu-central-1` |
| Operating system | Amazon Linux 2023 |
| Processor | Intel Xeon 6975P-C; 96 physical cores / 192 logical CPUs |
| Instance RAM | 1,536 GiB |
| Local storage | Three 3.8 TB NVMe drives, RAID 0, XFS |
| DuckDB threads / memory limit | `192` / `1200GB` |
| Python | `3.12.14` |
| DuckDB | `2.0.0.dev2609121639` |
| Splink | `5.0.0.dev5` |

The script pins these development releases and its direct supporting dependencies. Use those versions to reproduce this observation; results with other versions may differ.

## Run it yourself

Save the complete script below as `splink_reprex.py`. With `uv` installed, run it from a fresh directory on the local NVMe filesystem:

```bash
uv run --python 3.12.14 --script splink_reprex.py
```

`uv` installs the dependencies declared in the script. No AWS credentials, S3 dataset or benchmark framework are required. The script uses your existing machine; it does not provision an EC2 instance or configure its disks.

The defaults target the machine above. Allow substantially more than 400 GB of free disk space for generated input, intermediate files and final predictions.

For a smaller functional check, edit the top-level constants before running:

```python
ROW_COUNT = 100_000
THREADS = 4
MEMORY_LIMIT = "4GB"
CHUNK_INDEX = 1
CHUNK_COUNT = 1
EXPECTED_PREDICTION_ROWS = None
```

Choose thread and memory limits that fit your machine. Keep `ROW_COUNT` divisible by 10, and set `EXPECTED_PREDICTION_ROWS = None` whenever changing the scale, model or chunk fraction. Smaller tests check that the script works; their timings do not predict billion-row performance.

When it finishes, inspect:

- `splink-reprex/work/result.json` for timings, versions and row-count validation.
- `splink-reprex/work/profiles/` for native DuckDB query profiles.
- `splink-reprex/predictions/` for the scored pairs in Parquet.



## Complete script

This is the standalone script used for the run.

<details markdown="1">
<summary>Show the complete Python script</summary>


```python
# /// script
# requires-python = ">=3.12,<3.13"
# dependencies = [
#   "duckdb==2.0.0.dev2609121639", "splink==5.0.0.dev5", "pyarrow==25.0.1",
#   "altair==6.2.2", "igraph==1.0.0", "Jinja2==3.1.6", "sqlglot==30.17.0",
# ]
# ///
"""Generate 1bn synthetic records locally and run the full Splink workflow.

Run with Python 3.12: uv run --script splink_reprex.py
No S3 access, benchmark framework, or accompanying Python files are needed.
Default sizing: r8id.metal-48xl, 192 logical CPUs, 1200GB DuckDB limit,
local NVMe scratch. Run in a directory on the intended scratch filesystem.
Fresh directories are required; existing output is never overwritten.

The historical generator SQL and model are preserved. Files may differ in
layout while representing the same records. Standard native profiling is
included in timing; no EXPLAIN ANALYZE replay and no fsync barrier are added.
Generation and prediction have separate timers. The combined timer includes
setup and basic validations, but excludes dependency installation.
"""

import json
import platform
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb


def prepare(output, work, temp, threads, memory_limit, database=":memory:"):
    output, work, temp = (Path(p).resolve() for p in (output, work, temp))
    if output.exists():
        raise ValueError("Use a new output directory; existing data is never overwritten")
    if threads < 1:
        raise ValueError("THREADS must be positive")
    work.mkdir(parents=True, exist_ok=True)
    temp.mkdir(parents=True, exist_ok=True)
    (work / "profiles").mkdir(exist_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    if database != ":memory:" and Path(database).exists():
        raise ValueError("Use a fresh working database")
    con = duckdb.connect(str(database))
    for name, value in {
        "threads": threads,
        "memory_limit": memory_limit,
        "temp_directory": str(temp),
        "preserve_insertion_order": False,
        "python_scan_all_frames": True,
    }.items():
        con.execute(f"SET {name} = ?", [value])
    return con


def profiling(con, folder, mode):
    """Native last-query profile only; never replays a query or wraps a connection."""
    if mode not in (None, "standard", "detailed"):
        raise ValueError("PROFILE_MODE must be None, standard or detailed")
    if mode:
        # DuckDB 2 validates the output extension against the active format.
        con.execute("PRAGMA enable_profiling='json'")
        con.execute("SET profiling_output = ?", [str(Path(folder) / "last-query.json")])
        con.execute("SET profiling_mode = ?", [mode])
        try:
            con.execute("SET profiling_coverage='ALL'")
        except duckdb.CatalogException:
            pass  # Older DuckDB: use its native coverage, without replay.


def validate_parquet(path, expected_rows=None, columns=()):
    files = sorted(path if isinstance(path, list) else Path(path).rglob("*.parquet"))
    if not files:
        raise ValueError("No completed Parquet output")
    with duckdb.connect() as con:
        relation = con.read_parquet([str(p) for p in files], hive_partitioning=False)
        rows = relation.count("*").fetchone()[0]
        if expected_rows is not None and rows != expected_rows:
            raise ValueError(f"Expected {expected_rows} rows, got {rows}")
        if not set(columns) <= set(relation.columns):
            raise ValueError(f"Missing output columns: {set(columns) - set(relation.columns)}")
        return {
            "status": "passed",
            "rows": rows,
            "columns": relation.columns,
            "files": len(files),
            "bytes": sum(p.stat().st_size for p in files),
        }


def save(work, result):
    result.update(python=sys.version, platform=platform.platform(), duckdb_version=duckdb.__version__)
    Path(work, "result.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


def finish(work, result, output, expected_rows=None, columns=()):
    """Preserve the primary time even if later validation fails."""
    print(
        f"{result['operation']}: {result['elapsed_seconds']:.6f} seconds; native profiling={result['profile_mode'] or 'off'}",
        flush=True,
    )
    result["validation"] = {"status": "pending"}
    save(work, result)
    try:
        result["validation"] = validate_parquet(output, expected_rows, columns)
    except Exception as exc:
        result["validation"] = {"status": "failed", "error": str(exc)}
        save(work, result)
        raise
    save(work, result)
    return result


def require_paths(*paths):
    if any("EDIT_ME" in str(p) for p in paths):
        raise ValueError("Fill in the path constants at the top of the script")
# Edit these constants to change scale or resource limits.
ROW_COUNT = 1_000_000_000
THREADS = 192
MEMORY_LIMIT = "1200GB"
CHUNK_INDEX = 1
CHUNK_COUNT = 1
PROFILE_MODE = "standard"
EXPECTED_PREDICTION_ROWS = 10_037_267_664  # Set None when changing scale/model/chunks.
WORK_DIR = Path("splink-reprex/work")
TEMP_DIR = Path("splink-reprex/spill")
OUTPUT_DIR = Path("splink-reprex/predictions")



def generate_local(row_count, threads, memory, profile, output, work, temp):
    import time
    from pathlib import Path

    import duckdb

    OUTPUT_DIR = Path("EDIT_ME/new-people")
    WORK_DIR = Path("EDIT_ME/fresh-work")
    TEMP_DIR = Path("EDIT_ME/spill")
    ROW_COUNT = 100_000_000
    THREADS = 32
    MEMORY_LIMIT = "200GB"
    PROFILE_MODE = "standard"  # None disables native profiling.

    DUPLICATE_COUNTS = (0, 1, 2, 3)

    POOL_SIZE = 100_000

    VALUE_COUNTS = {
        "first_name": 50_000,
        "surname": 100_000,
        "postcode": 20_000,
        "email": 100_000,
    }

    ZIPF_EXPONENTS = {
        "first_name": 1.03,
        "surname": 0.35,
        "postcode": 0.15,
        "email": 0.15,
    }

    DOB_START = "1940-01-01"

    DOB_DAY_SPAN = 29_220

    ROW_GROUP_SIZE = 100_000

    ROW_GROUPS_PER_FILE = 80

    OUTPUT_COLUMNS = [
        "unique_id",
        "dupe_number",
        "name_1",
        "name_2",
        "name_3",
        "surname",
        "dob",
        "postcode",
        "email",
        "phone_number",
    ]


    def sql_path(path: Path) -> str:
        return path.as_posix().replace("'", "''")


    def validate_generation_options(row_count: int, threads: int) -> int:
        if row_count <= 0:
            raise ValueError("row_count must be positive")
        if threads <= 0:
            raise ValueError("threads must be positive")

        records_per_duplicate_cycle = sum(duplicate_count + 1 for duplicate_count in DUPLICATE_COUNTS)
        if row_count % records_per_duplicate_cycle != 0:
            raise ValueError(
                f"row_count must be divisible by {records_per_duplicate_cycle} "
                "to generate equal numbers of entities with 0, 1, 2, and 3 duplicates"
            )
        return row_count * len(DUPLICATE_COUNTS) // records_per_duplicate_cycle


    def create_skewed_pool(
        con: duckdb.DuckDBPyConnection,
        *,
        domain: str,
        value_count: int,
        value_sql: str,
        zipf_exponent: float,
    ) -> None:
        con.execute(
            f"""
            CREATE OR REPLACE TABLE {domain}_dist AS
            WITH ranked AS (
                SELECT
                    i AS rank,
                    {value_sql} AS value,
                    1.0 / pow(i::DOUBLE, {zipf_exponent}) AS weight
                FROM range(1, {value_count + 1}) AS t(i)
            ),
            normalized AS (
                SELECT
                    value,
                    weight / sum(weight) OVER () AS p
                FROM ranked
            ),
            cumulative AS (
                SELECT
                    value,
                    p,
                    sum(p) OVER (ORDER BY p DESC) AS cum_p
                FROM normalized
            )
            SELECT
                value,
                p,
                coalesce(lag(cum_p) OVER (ORDER BY p DESC), 0.0) AS lower_p
            FROM cumulative
            ORDER BY p DESC
            """
        )

        con.execute(
            f"""
            CREATE OR REPLACE TABLE {domain}_pool AS
            WITH buckets AS (
                SELECT
                    bucket,
                    (bucket + 0.5) / {POOL_SIZE}::DOUBLE AS u
                FROM range({POOL_SIZE}) AS t(bucket)
                ORDER BY u
            )
            SELECT
                bucket,
                value
            FROM buckets AS b
            ASOF JOIN {domain}_dist AS d
                ON b.u >= d.lower_p
            """
        )


    def main():
        require_paths(OUTPUT_DIR, WORK_DIR, TEMP_DIR)
        entity_count = validate_generation_options(ROW_COUNT, THREADS)
        output_dir = OUTPUT_DIR.resolve()
        con = prepare(output_dir, WORK_DIR, TEMP_DIR, THREADS, MEMORY_LIMIT, WORK_DIR / "generator.duckdb")
        output_dir.mkdir()
        create_skewed_pool(
            con,
            domain="first_name",
            value_count=VALUE_COUNTS["first_name"],
            value_sql="'name_' || lpad(i::VARCHAR, 5, '0')",
            zipf_exponent=ZIPF_EXPONENTS["first_name"],
        )
        create_skewed_pool(
            con,
            domain="surname",
            value_count=VALUE_COUNTS["surname"],
            value_sql="'surname_' || lpad(i::VARCHAR, 6, '0')",
            zipf_exponent=ZIPF_EXPONENTS["surname"],
        )
        create_skewed_pool(
            con,
            domain="postcode",
            value_count=VALUE_COUNTS["postcode"],
            value_sql="'postcode_' || lpad(i::VARCHAR, 5, '0')",
            zipf_exponent=ZIPF_EXPONENTS["postcode"],
        )
        create_skewed_pool(
            con,
            domain="email",
            value_count=VALUE_COUNTS["email"],
            value_sql="'user_' || lpad(i::VARCHAR, 6, '0') || '@example.com'",
            zipf_exponent=ZIPF_EXPONENTS["email"],
        )

        records_per_duplicate_cycle = sum(duplicate_count + 1 for duplicate_count in DUPLICATE_COUNTS)
        copy_sql = f"""
            COPY (
                WITH entities AS (
                    SELECT
                        i::BIGINT AS entity_id,
                        ((i - 1) % {len(DUPLICATE_COUNTS)} + 1)::BIGINT AS record_count,
                        (
                            (i - 1) // {len(DUPLICATE_COUNTS)} * {records_per_duplicate_cycle}
                            + ((i - 1) % {len(DUPLICATE_COUNTS)})
                                * (((i - 1) % {len(DUPLICATE_COUNTS)}) + 1) // 2
                            + 1
                        )::BIGINT AS first_unique_id,
                        n1.value AS name_1,
                        CASE WHEN hash(i + 200_003) % 100 < 50 THEN NULL ELSE n2.value END AS name_2,
                        CASE WHEN hash(i + 300_007) % 100 < 90 THEN NULL ELSE n3.value END AS name_3,
                        s.value AS surname,
                        CASE
                            WHEN hash(i + 500_009) % 100 < 10 THEN NULL
                            ELSE DATE '{DOB_START}'
                                + CAST(hash(i + 500_029) % {DOB_DAY_SPAN} AS INTEGER)
                        END AS dob,
                        p.value AS postcode,
                        e.value AS email,
                        lpad(CAST(hash(i + 800_011) % 1_000_000_000_000 AS VARCHAR), 12, '0')
                            AS phone_number
                    FROM range(1, {entity_count + 1}) AS rows(i)
                    JOIN first_name_pool AS n1
                        ON CAST(hash(i + 100_003) % {POOL_SIZE} AS BIGINT) = n1.bucket
                    JOIN first_name_pool AS n2
                        ON CAST(hash(i + 200_003) % {POOL_SIZE} AS BIGINT) = n2.bucket
                    JOIN first_name_pool AS n3
                        ON CAST(hash(i + 300_007) % {POOL_SIZE} AS BIGINT) = n3.bucket
                    JOIN surname_pool AS s
                        ON CAST(hash(i + 400_009) % {POOL_SIZE} AS BIGINT) = s.bucket
                    JOIN postcode_pool AS p
                        ON CAST(hash(i + 600_011) % {POOL_SIZE} AS BIGINT) = p.bucket
                    JOIN email_pool AS e
                        ON CAST(hash(i + 700_001) % {POOL_SIZE} AS BIGINT) = e.bucket
                )
                SELECT
                    (first_unique_id + copy_index)::BIGINT AS unique_id,
                    copy_index::UTINYINT AS dupe_number,
                    name_1,
                    name_2,
                    name_3,
                    surname,
                    dob,
                    postcode,
                    email,
                    phone_number
                FROM entities
                CROSS JOIN LATERAL range(record_count) AS copies(copy_index)
            ) TO '{sql_path(output_dir)}'
            (
                FORMAT PARQUET,
                COMPRESSION ZSTD,
                PER_THREAD_OUTPUT TRUE,
                ROW_GROUP_SIZE {ROW_GROUP_SIZE},
                ROW_GROUPS_PER_FILE {ROW_GROUPS_PER_FILE},
                OVERWRITE_OR_IGNORE
            )
        """

        (WORK_DIR / "profiles/generate.sql").write_text(copy_sql, encoding="utf-8")
        profiling(con, WORK_DIR / "profiles", PROFILE_MODE)
        started_at = time.time()
        cpu_start = time.process_time()
        start = time.perf_counter()
        # PRIMARY TIMER: one COPY, including completed native Parquet output.
        con.execute(copy_sql)
        elapsed = time.perf_counter() - start
        cpu_seconds = time.process_time() - cpu_start
        # Profiling teardown, validation and uploads are outside this timer.
        con.execute("PRAGMA disable_profiling")
        con.close()
        return finish(
            WORK_DIR,
            {
                "operation": "Generate synthetic people to Parquet",
                "elapsed_seconds": elapsed,
                "cpu_seconds": cpu_seconds,
                "started_at": started_at,
                "profile_mode": PROFILE_MODE,
                "profile_coverage": "Native generation COPY; setup is not profiled",
                "row_count": ROW_COUNT,
                "entity_count": entity_count,
                "row_group_size": ROW_GROUP_SIZE,
                "row_groups_per_file": ROW_GROUPS_PER_FILE,
            },
            OUTPUT_DIR,
            ROW_COUNT,
            OUTPUT_COLUMNS,
        )

    ROW_COUNT, THREADS, MEMORY_LIMIT, PROFILE_MODE = row_count, threads, memory, profile
    OUTPUT_DIR, WORK_DIR, TEMP_DIR = output, work, temp
    return main()


def predict_local(row_count, threads, memory, profile, chunk_index, chunk_count, source, output, work, temp):
    import re
    import shutil
    import time
    from datetime import datetime, timezone
    from pathlib import Path

    import splink
    import splink.comparison_library as cl
    from splink import ColumnExpression, Linker, SettingsCreator, block_on
    from splink.backends.duckdb import DuckDBAPI

    INPUT_DIR = Path("EDIT_ME/full-people-100m")
    OUTPUT_DIR = Path("EDIT_ME/new-predictions")
    WORK_DIR = Path("EDIT_ME/fresh-work")
    TEMP_DIR = Path("EDIT_ME/spill")
    THREADS = 32
    MEMORY_LIMIT = "200GB"
    EXPECTED_INPUT_ROWS = 100_000_000
    CHUNK_INDEX = 1
    CHUNK_COUNT = 10  # A hash chunk on EACH side, not 10% of all candidate pairs.
    PROFILE_MODE = "standard"  # None disables native profiling.
    CACHE_POLICY = "as_staged"  # Or read_inputs; no OS-cache clearing is attempted.
    MATERIALISATION = "default"  # Or parquet (Splink dev5+) for direct Parquet materialisation.

    INPUT_VIEW_NAME = "benchmark_input"

    BLOCKING_RULE_COLUMNS = [
        ["name_1", "name_2", "surname"],
        ["name_1", "name_2", "dob"],
    ]

    TEN_BILLION_BLOCKING_RULES = [
        'l."surname" = r."surname" AND date_trunc(\'month\', l."dob") = date_trunc(\'month\', r."dob")',
        'l."postcode" = r."postcode" AND l."dob" = r."dob"',
        'l."surname" = r."surname" AND l."postcode" = r."postcode"',
        'l."surname" = r."surname" AND l."email" = r."email"',
        'l."postcode" = r."postcode" AND l."email" = r."email"',
        'l."name_2" = r."name_2" AND l."postcode" = r."postcode" AND year(l."dob") = year(r."dob")',
    ]

    COMPARISON_MODEL = {
        "name_jaro_winkler_thresholds": [0.95, 0.88],
        "email_jaro_winkler_thresholds": [0.95, 0.85],
        "dob_levenshtein_thresholds": [1, 2],
        "postcode_levenshtein_thresholds": [1, 2],
        "phone_number": "exact",
    }


    class NativeProfilingDuckDBAPI(DuckDBAPI):
        """Capture actual CTAS/COPY executions; dev4's profiler replays SELECTs."""

        def __init__(self, connection, query_profiling_dir, profiling_mode):
            super().__init__(connection=connection)
            self.profile_dir = Path(query_profiling_dir)
            self.profile_dir.mkdir(parents=True, exist_ok=True)
            self.profile_counter = 0
            self._con.execute("SET profiling_mode = ?", [profiling_mode])
            self._con.execute("SET profiling_coverage = 'ALL'")
            self._con.execute("PRAGMA disable_profiling")

        def _execute_sql_against_backend(self, final_sql):
            # Keep Splink's default table materialisation and execute each query once.
            materialisation = re.match(r"\s*CREATE\s+TABLE\s+(\S+)\s+AS\b", final_sql, re.I)
            export = re.match(r"\s*COPY\b", final_sql, re.I)
            if not materialisation and not export:
                return super()._execute_sql_against_backend(final_sql)
            name = materialisation[1] if materialisation else "parquet_export"
            name = re.sub(r"[^A-Za-z0-9_.-]+", "_", name).strip("_")
            self.profile_counter += 1
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            path = self.profile_dir / f"{timestamp}_{self.profile_counter:04d}_{name}_duckdb.json"
            pending = self.profile_dir.parent / "current-query-profile.json"
            self._con.execute("PRAGMA enable_profiling='json'")
            try:
                self._con.execute("SET profiling_output = ?", [str(pending)])
                pending.unlink(missing_ok=True)
                result = super()._execute_sql_against_backend(final_sql)
                # ALL coverage also profiles control statements. Preserve this query
                # before disabling profiling can overwrite the active output file.
                pending.replace(path)
                return result
            finally:
                self._con.execute("PRAGMA disable_profiling")


    def create_settings(blocking_rules=None) -> SettingsCreator:
        if blocking_rules is None:
            blocking_rules = [block_on(*columns) for columns in BLOCKING_RULE_COLUMNS]
        return SettingsCreator(
            link_type="dedupe_only",
            comparisons=[
                cl.JaroWinklerAtThresholds("name_1", [0.95, 0.88]),
                cl.JaroWinklerAtThresholds("name_2", [0.95, 0.88]),
                cl.JaroWinklerAtThresholds("name_3", [0.95, 0.88]),
                cl.JaroWinklerAtThresholds("surname", [0.95, 0.88]),
                cl.LevenshteinAtThresholds(ColumnExpression("dob").cast_to_string(), [1, 2]),
                cl.LevenshteinAtThresholds("postcode", [1, 2]).configure(term_frequency_adjustments=True),
                cl.JaroWinklerAtThresholds("email", [0.95, 0.85]).configure(term_frequency_adjustments=True),
                cl.ExactMatch("phone_number"),
            ],
            blocking_rules_to_generate_predictions=blocking_rules,
            max_iterations=2,
        )


    def main():
        require_paths(INPUT_DIR, OUTPUT_DIR, WORK_DIR, TEMP_DIR)
        if not 1 <= CHUNK_INDEX <= CHUNK_COUNT:
            raise ValueError("Require 1 <= CHUNK_INDEX <= CHUNK_COUNT")
        if PROFILE_MODE not in (None, "standard", "detailed"):
            raise ValueError("Invalid PROFILE_MODE")
        if MATERIALISATION not in ("default", "parquet"):
            raise ValueError("MATERIALISATION must be default or parquet")
        files = sorted(INPUT_DIR.rglob("*.parquet"))
        if not files:
            raise ValueError("Supply the complete local Parquet input first")
        if CACHE_POLICY not in ("as_staged", "read_inputs"):
            raise ValueError("CACHE_POLICY must be as_staged or read_inputs")
        if CACHE_POLICY == "read_inputs":
            for path in files:
                with path.open("rb") as stream:
                    while stream.read(8 * 1024 * 1024):
                        pass
        con = prepare(OUTPUT_DIR, WORK_DIR, TEMP_DIR, THREADS, MEMORY_LIMIT)
        con.read_parquet([str(p) for p in files], hive_partitioning=False).create_view(INPUT_VIEW_NAME)
        rows = con.execute(f"SELECT count(*) FROM {INPUT_VIEW_NAME}").fetchone()[0]
        if rows != EXPECTED_INPUT_ROWS:
            raise ValueError(f"Expected {EXPECTED_INPUT_ROWS} input rows, found {rows}")
        if MATERIALISATION == "parquet":
            from splink.backends.duckdb import DuckDBAPIWithProfiling, ParquetWriteOptions

            materialisations = WORK_DIR / "native-parquet"
            materialisations.mkdir(exist_ok=False)
            options = dict(
                connection=con,
                materialisation="parquet",
                materialisation_dir=materialisations,
                parquet_materialisation_options=ParquetWriteOptions(compression="zstd", per_thread_output=True),
            )
            if PROFILE_MODE:
                options.update(
                    query_profiling_dir=WORK_DIR / "profiles",
                    enable_profiling="json",
                    profiling_coverage="ALL",
                    profiling_mode=PROFILE_MODE,
                )
                db_api = DuckDBAPIWithProfiling(**options)
            else:
                db_api = DuckDBAPI(**options)
        elif PROFILE_MODE:
            db_api = NativeProfilingDuckDBAPI(
                connection=con,
                query_profiling_dir=WORK_DIR / "profiles",
                profiling_mode=PROFILE_MODE,
            )
        else:
            db_api = DuckDBAPI(connection=con)
        frame = db_api.register(INPUT_VIEW_NAME, dataset_display_name="people")
        linker = Linker(frame, create_settings(TEN_BILLION_BLOCKING_RULES), log_level=1)
        chunk = (CHUNK_INDEX, CHUNK_COUNT)
        started_at = time.time()
        cpu_start = time.process_time()
        start = time.perf_counter()
        # PRIMARY TIMER: prediction through completed Parquet output in either mode.
        predictions = linker.inference.predict_chunk(left_chunk=chunk, right_chunk=chunk, warning_mode="never")
        prediction_elapsed = time.perf_counter() - start
        prediction_cpu_seconds = time.process_time() - cpu_start
        export_elapsed = None
        if MATERIALISATION == "default":
            export_start = time.perf_counter()
            predictions.to_parquet(str(OUTPUT_DIR / "predictions.parquet"))
            export_elapsed = time.perf_counter() - export_start
        elapsed = time.perf_counter() - start
        cpu_seconds = time.process_time() - cpu_start
        # Validation is outside the primary timer.
        count = predictions.as_duckdbpyrelation().count("*").fetchone()[0]
        if MATERIALISATION == "parquet":
            backing = Path(db_api._parquet_materialiser._owned_paths[predictions.physical_name]).resolve()
            if not backing.is_relative_to(materialisations.resolve()):
                raise ValueError("Native prediction files escaped the working directory")
        con.close()
        if MATERIALISATION == "parquet":
            shutil.move(str(backing), str(OUTPUT_DIR))  # Retain the original backing files; no second SQL export.
        return finish(
            WORK_DIR,
            {
                "operation": (
                    "Splink predict_chunk to native Parquet"
                    if MATERIALISATION == "parquet"
                    else "Splink predict_chunk with default materialisation then to_parquet"
                ),
                "elapsed_seconds": elapsed,
                "cpu_seconds": cpu_seconds,
                "prediction_elapsed_seconds": prediction_elapsed,
                "prediction_cpu_seconds": prediction_cpu_seconds,
                "parquet_export_elapsed_seconds": export_elapsed,
                "started_at": started_at,
                "profile_mode": PROFILE_MODE,
                "profile_coverage": "Actual materialising queries and any Parquet export; no replay" if PROFILE_MODE else "disabled",
                "splink_version": splink.__version__,
                "input_rows": rows,
                "prediction_rows": count,
                "left_chunk": list(chunk),
                "right_chunk": list(chunk),
                "blocking_rules": TEN_BILLION_BLOCKING_RULES,
                "comparison_model": COMPARISON_MODEL,
                "cache_policy": CACHE_POLICY,
                "materialisation": MATERIALISATION,
                "parquet_options": (
                    {"compression": "zstd", "per_thread_output": True}
                    if MATERIALISATION == "parquet"
                    else {}  # SplinkDataFrame.to_parquet defaults, without overrides.
                ),
            },
            OUTPUT_DIR,
            count,
            ("unique_id_l", "unique_id_r", "match_weight", "match_probability"),
        )

    EXPECTED_INPUT_ROWS, THREADS, MEMORY_LIMIT, PROFILE_MODE = row_count, threads, memory, profile
    CHUNK_INDEX, CHUNK_COUNT, MATERIALISATION = chunk_index, chunk_count, "parquet"
    INPUT_DIR, OUTPUT_DIR, WORK_DIR, TEMP_DIR = source, output, work, temp
    return main()


def main():
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    profiles = WORK_DIR / "profiles"
    profiles.mkdir(exist_ok=True)
    generated = WORK_DIR / "generated-input"
    start, cpu_start, started_at = time.perf_counter(), time.process_time(), time.time()
    print(f"[reprex] Generating {ROW_COUNT:,} rows locally", flush=True)
    generation = generate_local(ROW_COUNT, THREADS, MEMORY_LIMIT, PROFILE_MODE,
                                generated, WORK_DIR / "generation", TEMP_DIR / "generation")
    generation_wall = time.perf_counter() - start
    for path in (WORK_DIR / "generation/profiles").glob("*"):
        if path.name == "last-query.json":
            stamp = datetime.fromtimestamp(generation["started_at"], timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            shutil.copy2(path, profiles / f"{stamp}_0000_generate_input_duckdb.json")
        else:
            shutil.copy2(path, profiles / path.name)
    print(f"[reprex] Predicting chunk {CHUNK_INDEX}/{CHUNK_COUNT} on each side", flush=True)
    prediction = predict_local(ROW_COUNT, THREADS, MEMORY_LIMIT, PROFILE_MODE, CHUNK_INDEX, CHUNK_COUNT,
                               generated, OUTPUT_DIR, WORK_DIR / "prediction", TEMP_DIR / "prediction")
    elapsed, cpu = time.perf_counter() - start, time.process_time() - cpu_start
    shutil.copytree(WORK_DIR / "prediction/profiles", profiles, dirs_exist_ok=True)
    result = {**prediction, "operation": "Self-contained generation and full Splink workflow",
              "started_at": started_at, "elapsed_seconds": elapsed, "cpu_seconds": cpu,
              "generation": generation, "generation_wall_seconds": generation_wall,
              "prediction": prediction, "expected_prediction_rows": EXPECTED_PREDICTION_ROWS,
              "cache_policy": "freshly_generated_no_cache_flush",
              "timing_scope": "Generation setup/COPY/basic validation plus prediction setup/query/basic validation; excludes installation and cloud setup",
              "reference_distribution_validation": "Not rerun: no external dataset required by this script"}
    if EXPECTED_PREDICTION_ROWS is not None and prediction["prediction_rows"] != EXPECTED_PREDICTION_ROWS:
        result["validation"].update(status="failed", error="Prediction count differs from historical full-job baseline")
        save(WORK_DIR, result)
        raise ValueError(result["validation"]["error"])
    save(WORK_DIR, result)
    print(f"[reprex] Passed: generation COPY {generation['elapsed_seconds']:.3f}s; "
          f"prediction {prediction['elapsed_seconds']:.3f}s; combined {elapsed:.3f}s; "
          f"predictions {prediction['prediction_rows']:,}", flush=True)
    return result


if __name__ == "__main__":
    main()
```

</details>

