pytest benchmarking/test_performance.py  --benchmark-json benchmarking/output.json -k 'test_2_rounds_1k_sqlite or test_2_rounds_1k_duckdb'
python benchmarking/combine_benchmarks_timeseries.py