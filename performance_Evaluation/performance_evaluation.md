## Performance Evaluation Experiments

The repository includes four benchmark scripts in the performance_Evaluation folder.

1. Data ingestion latency
   Script: [data_ingesion_latency.py](http://_vscodecontentref_/6)
   Measures:
   - Average latency
   - Throughput in operations per second
   - Records per second for initialise and fetch
   - Distribution across SQL and Mongo after each run

2. Logical query response time
   Script: [logical_query_response_time.py](http://_vscodecontentref_/7)
   Measures:
   - Query latency for READ, CREATE, UPDATE, DELETE
   - Each case repeated 5 times by default
   - Aggregated metrics (avg, p50, p95, throughput)
   - Safe cleanup by record_id from both SQL and Mongo

3. Metadata lookup overhead
   Script: [metadata_lookup_overhead.py](http://_vscodecontentref_/8)
   Measures:
   - Metadata file read time
   - JSON parse time
   - Field lookup time
   - End-to-end metadata lookup path

4. Transaction coordination overhead across SQL and MongoDB
   Script: [transaction_cordination_overhead_sql_mongo.py](http://_vscodecontentref_/9)
   Measures:
   - Coordinated CREATE path latency
   - Manual non-transactional baseline latency
   - Estimated overhead due to coordination logic
   - Routing distribution insight from metadata

All reports are saved to:
data/performance_reports

Example runs:
python [data_ingesion_latency.py](http://_vscodecontentref_/10)
python [logical_query_response_time.py](http://_vscodecontentref_/11)
python [metadata_lookup_overhead.py](http://_vscodecontentref_/12)
python [transaction_cordination_overhead_sql_mongo.py](http://_vscodecontentref_/13)