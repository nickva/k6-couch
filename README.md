CouchDB K6 Bench
================

Benchmark doc put, get, bulk_docs and other operations with K6

 * Install k6 (version 1.7+) from https://grafana.com/docs/k6/latest/set-up/install-k6/
 
 * Start or point to a cluster
 
 * Run benchmark with `k6 run ./k6_couchdb.js`

 * Configuration options can be passed in as env variables by prefixing them
   with `BENCH_`. The list of options and defaults are in the js file:
 
```
const URL            = env_str('URL', 'http://localhost:15984');
const USER           = env_str('USER', 'adm');
const PASS           = env_str('PASS', 'pass');
const DB             = env_str('DB', 'bench_db');
const Q              = env_str('Q', '4');
const DOCS           = env_num('DOCS', 100000);
const DOC_SIZE       = env_num('DOC_SIZE', 256);
const DURATION       = env_str('DURATION', '5m');
// Rates for individual scenarios
const WELCOME_RATE   = env_num('WELCOME_RATE', 1000);
const GET_RATE       = env_num('GET_RATE', 1000);
const INSERT_RATE    = env_num('INSERT_RATE', 100);
const UPDATE_RATE    = env_num('UPDATE_RATE', 100);
const BULK_DOCS_RATE = env_num('BULK_DOCS_RATE', 2);
const BULK_GET_RATE  = env_num('BULK_GET_RATE', 2);
const ALL_DOCS_RATE  = env_num('ALL_DOCS_RATE', 1);
const CHANGES_RATE   = env_num('CHANGES_RATE', 1);
const TAG            = env_str('TAG', '');
// Default set of scenarios
const SCENARIOS      = env_str('SCENARIOS', 'doc_get,doc_insert');
const XHEADER        = env_str('XHEADER', '');
const BATCH_SIZE     = env_num('BATCH_SIZE', 500);
```

A few examples:

  * Run all scenarios for 30 sec with given user/pass:
  ```
  $ BENCH_DURATION=30s BENCH_USER=adm BENCH_PASS=pass k6 run k6_couchdb.js
  ```
  
  * Run just doc_update
  ```
  $ BENCH_SCENARIOS=doc_update k6 run k6_couchdb.js
  ```
  
  * Run just doc_get at 10 rps, starting with 25k docs of 64KB each
  ```
  $ BENCH_DOCS=25000 BENCH_SCENARIOS=doc_get BENCH_GET_RATE=10 k6 run k6_couchdb.js
  ```
  
  * Run doc_get scenario with a particular URL and an extra header
  ```
  $ BENCH_URL=https://foo.example.com BENCH_SCENARIOS=doc_get BENCH_XHEADER=x-foo:bar ./k6 run k6_couchdb.js
  ```
