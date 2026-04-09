CouchDB K6 Bench
================

Benchmark CouchDB doc reads and writes, bulk requests and views with k6.

 * Install k6 (ver 1.7+) from https://grafana.com/docs/k6/latest/set-up/install-k6/
 * Run `k6 run ./k6_couchdb.js`
 * Configure it with `BENCH_` environment variables

Scenarios
---------

`BENCH_SCENARIOS` is a comma separated list of `name` or `name:rate`
entries, for example `doc_get:500,doc_insert`. Each scenario runs at a
constant rate, in requests per second, for `BENCH_DURATION`. Before that
it ramps up to the rate for 10 seconds, which are not measured.

| scenario      | rate | request                                                |
|---------------|-----:|--------------------------------------------------------|
| `welcome`     | 1000 | `GET /`                                                |
| `doc_get`     | 1000 | `GET /db/{id}`, a random loaded doc                    |
| `doc_insert`  |  100 | `POST /db`, a new doc                                  |
| `doc_update`  |  100 | `GET` then `PUT /db/{id}`, a random loaded doc         |
| `bulk_docs`   |    2 | `POST /db/_bulk_docs`, `BATCH_SIZE` new docs           |
| `bulk_get`    |    2 | `POST /db/_bulk_get`, `BATCH_SIZE` random loaded docs  |
| `all_docs`    |    1 | `GET /db/_all_docs`, `BATCH_SIZE` rows from a random key |
| `changes`     |    1 | `GET /db/_changes`, the first `BATCH_SIZE` rows        |
| `view_query`  |   10 | map view, `BATCH_SIZE` rows from a random key          |
| `view_reduce` |   10 | reduce over 100 adjacent keys, no grouping             |
| `view_group`  |   10 | `group=true` reduce over 10 adjacent keys              |

Options
-------

```
BENCH_URL         http://localhost:15984
BENCH_USER        adm
BENCH_PASS        pass
BENCH_DB          bench_db
BENCH_Q           4          shards of the created db
BENCH_SCENARIOS   doc_get,doc_insert
BENCH_DURATION    5m
BENCH_DOCS        100000     docs loaded by setup
BENCH_DOC_SIZE    256        approximate size of a doc body, in bytes
BENCH_IDS         ordered    ordered | random doc ids
BENCH_W           3          write quorum, 0 uses the cluster default
BENCH_BATCH_SIZE  500        docs per bulk request, rows per page
BENCH_REUSE_DB    0          1 reuses a db loaded with the same options
BENCH_COMPACT     1          0 skips the compaction in setup
BENCH_TEARDOWN    1          0 keeps the db after the run
BENCH_TAG         ''         also saves the summary as summary-<tag>.json
BENCH_XHEADER     ''         one extra header, as name:value
BENCH_VIEW_KEYS   1000       distinct view keys
```

Docs have a variety of nested objects, arrays, numbers and strings. They are
about `BENCH_DOC_SIZE` bytes each. There is a `k` field with one of
`BENCH_VIEW_KEYS` keys, which is what the views emit.

`BENCH_IDS=ordered|random` controls whether doc ids are sequential or random.

Examples
--------

```
# Default scenarios, 30 sec duration
BENCH_DURATION=30s BENCH_USER=adm BENCH_PASS=pass k6 run k6_couchdb.js

# doc_get only, 10 rps, 25k docs of 64KB each
BENCH_DOCS=25000 BENCH_DOC_SIZE=65536 BENCH_SCENARIOS=doc_get:10 k6 run k6_couchdb.js

# Mix of reads/write at different rates with random ids
BENCH_IDS=random BENCH_SCENARIOS=doc_get:500,doc_insert:50,doc_update:50 k6 run k6_couchdb.js

# Override URL and add an extra header
BENCH_URL=https://foo.example.com BENCH_XHEADER=x-foo:bar k6 run k6_couchdb.js

# Views and re-use the db between runs
BENCH_REUSE_DB=1 BENCH_TEARDOWN=0 BENCH_SCENARIOS=view_query,view_reduce,view_group k6 run k6_couchdb.js

# Tag runs. These are used to save results in summary-<tag>.json files
BENCH_TAG=fancyfeature42 k6 run k6_couchdb.js

# Load from some profile
set -a; . my_test_profiles/profile1.env; set +a; k6 run k6_couchdb.js
```

Results
-------

Summary shows one line per request type. It will have a count, rate, failure
rate and latency stats in msec.

To help integrate this into an external runner the benchmark will exit with:

exit code `99` if:

 * fewer than 99.9% of the checks `pass`
 * 1% or more of the requests of one type `fail`
 * more iterations were dropped than one second's worth of the configured rate (overload!)

exit code `107` if:

 * setup failed

Notes
-----

 * There will probably be some compactions started during a run. To switch
   auto-compaction off do something like this, and `DELETE` the same keys to
   switch it back on:
   ```
   for c in ratio_dbs slack_dbs ratio_views slack_views; do
     curl -u adm:pass -X PUT -H 'X-Couch-Persist: false' \
       http://localhost:15984/_node/_local/_config/smoosh.$c/concurrency -d '"0"'
   done
   ```

 * We use `BENCH_W=3` to reduce the pressure on the internal replicator and
   avoid creating a trailing background workload following heavy inserts.

 * With the small defaults the data set will likely fit in the page cache.

 * View requests wait for the index to catch up, so writes running alongside
   views will make view requests quite a bit slower. You may or may not want
   that.
