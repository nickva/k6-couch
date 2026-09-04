//
// Examples:
//   1) Run all scenarios for 30 sec with given user/pass:
//     $ BENCH_DURATION=30s BENCH_USER=adm BENCH_PASS=pass k6 run k6_couchdb.js
//   2) Run just doc_update
//     $ BENCH_SCENARIOS=doc_update k6 run k6_couchdb.js
//   3) Run just doc_get at 10 rps, starting with 25k docs of 64KB each:
//     $ BENCH_DOCS=25000 BENCH_SCENARIOS=doc_get BENCH_GET_RATE=10 k6 run k6_couchdb.js
//   4) Run doc_get scenario with a particular URL and an extra header
//     $ BENCH_URL=https://foo.example.com BENCH_SCENARIOS=doc_get BENCH_XHEADER=x-foo:bar ./k6 run k6_couchdb.js
//   5) Benchmark the / (welcome) endpoint for 60s @ 2k rps. This might be interesting to exclude the effect
//     of disk IO and DB node CPU usage when, say, benchmarking the acceptor logic or a load balancer
//     $ BENCH_DOCS=1 BENCH_DURATION=60s BENCH_SCENARIOS=welcome BENCH_WELCOME_RATE=2000 k6 run k6_couchdb.js
//   6) Skip deleting the db at the end. This helps analyze the db files sizes after the benchmark.
//     $ BENCH_TEARDOWN=0 BENCH_SCENARIOS=doc_get,doc_update,doc_insert ./k6 run k6_couchdb.js

import http from 'k6/http';
import encoding from 'k6/encoding';
import { check, sleep } from 'k6';
import { randomString, randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// Parameters, defaults or from the environment
// Note: environment variables are prefixed with "BENCH_"
//

const URL            = env_str('URL', 'http://localhost:15984');
const USER           = env_str('USER', 'adm');
const PASS           = env_str('PASS', 'pass');
const DB             = env_str('DB', 'bench_db');
const Q              = env_num('Q', 4, 1);
const DOCS           = env_num('DOCS', 100000, 0);
const DOC_SIZE       = env_num('DOC_SIZE', 256, 0);
const DURATION       = env_str('DURATION', '5m');
// Rates for individual scenarios
const WELCOME_RATE   = env_num('WELCOME_RATE', 1000, 1);
const GET_RATE       = env_num('GET_RATE', 1000, 1);
const INSERT_RATE    = env_num('INSERT_RATE', 100, 1);
const UPDATE_RATE    = env_num('UPDATE_RATE', 100, 1);
const BULK_DOCS_RATE = env_num('BULK_DOCS_RATE', 2, 1);
const BULK_GET_RATE  = env_num('BULK_GET_RATE', 2, 1);
const ALL_DOCS_RATE  = env_num('ALL_DOCS_RATE', 1, 1);
const CHANGES_RATE   = env_num('CHANGES_RATE', 1, 1);
const TEARDOWN       = env_num('TEARDOWN', 1, 0, 1);
const TAG            = env_str('TAG', '');
// Default set of scenarios
const SCENARIOS      = env_str('SCENARIOS', 'doc_get,doc_insert');
const XHEADER        = env_str('XHEADER', '');
const BATCH_SIZE     = env_num('BATCH_SIZE', 500, 1);

// Derived params

const DB_URL        = `${URL}/${DB}`;
const HEADERS       = get_headers(XHEADER, USER, PASS);
const SETUP_PAR     = {'headers': HEADERS, tags: {name: 'setup'}, responseType: 'text'};
const WELCOME_PAR   = {'headers': HEADERS, tags: {name: 'welcome'}};
const GET_PAR        = {'headers': HEADERS, tags: {name: 'doc_get'}};
const UPDATE_GET_PAR = {'headers': HEADERS, tags: {name: 'doc_get'}, responseType: 'text'};
const PUT_PAR        = {'headers': HEADERS, tags: {name: 'doc_put'}};
const POST_PAR      = {'headers': HEADERS, tags: {name: 'doc_insert'}};
const BULK_DOCS_PAR = {'headers': HEADERS, tags: {name: 'bulk_docs'}};
const BULK_GET_PAR  = {'headers': HEADERS, tags: {name: 'bulk_get'}};
const ALL_DOCS_PAR  = {'headers': HEADERS, tags: {name: 'all_docs'}};
const CHANGES_PAR   = {'headers': HEADERS, tags: {name: 'changes'}};

const SCENARIO_DEFAULTS = {
  executor: 'constant-arrival-rate',
  duration: DURATION,
  timeUnit: '1s',
  preAllocatedVUs: 500,
  maxVUs: 5000
};

// Callbacks & options. These are what k6 expects to call

export const options = {
   scenarios: scenarios(),
   // Discard response bodies by default. doc_update overrides this for its GET request.
   discardResponseBodies: true,
   setupTimeout: '60m',
   // These are bogus, always passing thresholds just so we can
   // see the individual tagged requests times in the summary
   // but in principle these could be turned into a pass/fail test
   thresholds: {
     'http_req_duration{name:welcome}'    : ['p(99)>=0'],
     'http_req_duration{name:doc_get}'    : ['p(99)>=0'],
     'http_req_duration{name:doc_insert}' : ['p(99)>=0'],
     'http_req_duration{name:doc_put}'    : ['p(99)>=0'],
     'http_req_duration{name:bulk_docs}'  : ['p(99)>=0'],
     'http_req_duration{name:bulk_get}'   : ['p(99)>=0'],
     'http_req_duration{name:all_docs}'   : ['p(99)>=0'],
     'http_req_duration{name:changes}'    : ['p(99)>=0']
   }
};

export function setup() {
  console.log(`
    * tag: ${TAG}
    * scenarios: ${SCENARIOS}
    * url: ${URL}
    * user: ${USER}
    * q: ${Q}
    * docs: ${DOCS}
    * doc_size: ${DOC_SIZE}
    * duration: ${DURATION}
    * discardResponseBodies: ${options.discardResponseBodies}
  \n`);
  let res;
  res = http.put(DB_URL + "?q=" + Q, null, SETUP_PAR);
  if (res.status == 412) {
      res = http.del(DB_URL, null, SETUP_PAR);
      if (res.status != 200) {
          throw new Error(`Could not delete old DB ${DB_URL} ${res.body}`);
      }
      res = http.put(DB_URL + "?q=" + Q, null, SETUP_PAR);
  }
  if (res.status != 201) {
      throw new Error(`Could not create DB ${DB_URL} ${res.body} ${res.status}`);
  }
  insert_docs(DOCS, BATCH_SIZE, DOC_SIZE);
  sleep(10);
}

export function teardown(data) {
    if (TEARDOWN > 0) {
      const res = http.del(DB_URL, null, SETUP_PAR);
      if (res.status != 200) {
          throw new Error(`In teardown could not delete DB ${DB_URL} ${res.body}`);
      }
    }
}

function scenarios() {
  const scenario_keys = SCENARIOS.split(',').map(k => k.trim());
  if (scenario_keys.some(k => !k)) {
    throw new Error('BENCH_SCENARIOS must be a comma-separated list of scenario names');
  }
  const duplicates = scenario_keys.filter((k, i) => scenario_keys.indexOf(k) !== i);
  if (duplicates.length > 0) {
    throw new Error(`Duplicate scenarios: ${[...new Set(duplicates)].join(', ')}`);
  }
  const scenarios_available = {
    welcome    : {...SCENARIO_DEFAULTS, exec: 'welcome', rate: WELCOME_RATE},
    doc_get    : {...SCENARIO_DEFAULTS, exec: 'doc_get', rate: GET_RATE},
    doc_insert : {...SCENARIO_DEFAULTS, exec: 'doc_insert', rate: INSERT_RATE},
    doc_update : {...SCENARIO_DEFAULTS, exec: 'doc_update', rate: UPDATE_RATE},
    bulk_docs  : {...SCENARIO_DEFAULTS, exec: 'bulk_docs', rate: BULK_DOCS_RATE},
    bulk_get   : {...SCENARIO_DEFAULTS, exec: 'bulk_get', rate: BULK_GET_RATE},
    all_docs   : {...SCENARIO_DEFAULTS, exec: 'all_docs',rate: ALL_DOCS_RATE},
    changes    : {...SCENARIO_DEFAULTS, exec: 'changes', rate: CHANGES_RATE}
  };
  const invalid = scenario_keys.filter(k => !scenarios_available[k]);
  if (invalid.length > 0) {
    throw new Error(`Invalid scenarios: ${invalid.join(', ')}`);
  }
  const requires_docs = scenario_keys.some(k => ['doc_get', 'doc_update', 'bulk_get'].includes(k));
  if (requires_docs && DOCS === 0) {
    throw new Error('BENCH_DOCS must be greater than 0 for doc_get, doc_update, and bulk_get scenarios');
  }
  return Object.fromEntries(scenario_keys.map(k => [k, scenarios_available[k]]));
}

export function welcome () {
  const res = http.get(URL, WELCOME_PAR);
  check(res, {'welcome status is 200': r => r.status === 200});
}

export function doc_get () {
  const doc_id = fmt_doc_id(randomIntBetween(0, DOCS-1));
  const res = http.get(`${DB_URL}/${doc_id}`, GET_PAR);
  check(res, {'doc_get status is 200': r => r.status === 200});
}

export function doc_update () {
  const doc_id = fmt_doc_id(randomIntBetween(0, DOCS-1));
  const res = http.get(`${DB_URL}/${doc_id}`, UPDATE_GET_PAR);
  if (res.status != 200) {
      throw new Error(`Got error ${res.status} getting document ${doc_id}`);
  };
  const doc = res.json();
  delete doc['_id'];
  doc['data'] = randomString(DOC_SIZE);
  const put_res = http.put(`${DB_URL}/${doc_id}?rev=${doc._rev}`, JSON.stringify(doc), PUT_PAR);
  check(put_res, {'doc_update status is 201': r => r.status === 201});
}

export function doc_insert () {
  const doc = {'data': randomString(DOC_SIZE)};
  const res = http.post(`${DB_URL}`, JSON.stringify(doc), POST_PAR);
  check(res, {'doc_insert status is 201': r => r.status === 201});
}

export function bulk_docs() {
  const docs_arr = [];
  for(let i=0; i<BATCH_SIZE; i++){
      docs_arr.push({'data': randomString(DOC_SIZE)});
  };
  const body = JSON.stringify({'docs': docs_arr});
  const res = http.post(`${DB_URL}/_bulk_docs?w=3`, body, BULK_DOCS_PAR);
  check(res, {'bulk_docs status is 201 or 202': r => r.status === 201 || r.status === 202});
}

export function bulk_get() {
  const docs_arr = [];
  for(let i=0; i<BATCH_SIZE; i++){
      docs_arr.push({'id': fmt_doc_id(randomIntBetween(0, DOCS-1))});
  };
  const body = JSON.stringify({'docs': docs_arr});
  const res = http.post(`${DB_URL}/_bulk_get`, body, BULK_GET_PAR);
  check(res, {'bulk_get status is 200': r => r.status === 200});
}

export function all_docs() {
  const res = http.get(`${DB_URL}/_all_docs?limit=${BATCH_SIZE}`, ALL_DOCS_PAR)
  check(res, {'all_docs status is 200': r => r.status === 200});
}

export function changes() {
  const res = http.get(`${DB_URL}/_changes?limit=${BATCH_SIZE}`, CHANGES_PAR)
  check(res, {'changes status is 200': r => r.status === 200});
}

/// End of callback functions

// Helpers

function env_str(name, default_val) {
  name = 'BENCH_' + name;
  return __ENV[name] ? __ENV[name] : default_val;
}

function env_num(name, default_val, min, max = Infinity) {
  name = 'BENCH_' + name;
  const value = __ENV[name] === undefined ? default_val : __ENV[name];
  const num = Number(value);
  if (value === '' || !Number.isFinite(num) || !Number.isInteger(num) || num < min || num > max) {
    const range = max === Infinity ? `greater than or equal to ${min}` : `between ${min} and ${max}`;
    throw new Error(`Invalid numeric env var ${name}=${value}; expected an integer ${range}`);
  }
  return num;
}

function get_headers(xtra_header, user, pass) {
    const b64 = encoding.b64encode(`${user}:${pass}`);
    const xheader = parse_header(xtra_header);
    return {
        'authorization': `Basic ${b64}`,
        'content-type':'application/json',
        ...xheader
   }
};

function parse_header(header_str) {
  const [name, ...rest] = header_str.split(":");
  const val = rest.join(":");
  return name ? {[name]: val} : {};
}

function insert_docs(num, bsize, dsize) {
  let doc_id = 0;
  const batches = Math.trunc(num / bsize);
  for(let i=0; i<batches; i++) {
      doc_id = insert_batch(doc_id, bsize, dsize)
  };
  const remaining = num - (batches * bsize);
  return remaining > 0 ? insert_batch(doc_id, remaining, dsize) : doc_id;
}

function insert_batch(doc_id, count, size) {
  const docs_arr = [];
  for(let i=0; i<count; i++) {
      const doc = {'_id': fmt_doc_id(doc_id), 'data': randomString(size)};
      docs_arr.push(doc);
      doc_id ++;
  };
  const req = {'docs': docs_arr};
  // Use w=3 to avoid generating less of an internal replication
  // background load
  const res = http.post(`${DB_URL}/_bulk_docs?w=3`, JSON.stringify(req), SETUP_PAR);
  if (res.status != 201 && res.status != 202) {
    throw new Error(`Failed _bulk_docs: status=${res.status}, body=${res.body}`);
  }
  return doc_id;
}

function fmt_doc_id(n){
   // make the length 32 bytes (32 chars) to be the same
   // size as a uuid for the case when we're inserting
   // random docs with a post {} request
   return String(n).padStart(32, '0');
}
