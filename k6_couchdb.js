// CouchDB k6 benchmark
//
// Example runs:
//   k6 run k6_couchdb.js
//   BENCH_DURATION=30s BENCH_USER=adm BENCH_PASS=pass k6 run k6_couchdb.js
//   BENCH_URL=https://foo.example.com BENCH_XHEADER=x-foo:bar k6 run k6_couchdb.js
//   set -a; . my_test_profiles/profile1.env; set +a; k6 run k6_couchdb.js
//
// See README.md for more examples

import http from 'k6/http';
import encoding from 'k6/encoding';
import { md5 } from 'k6/crypto';
import { check, sleep } from 'k6';
import exec from 'k6/execution';
import { Counter } from 'k6/metrics';

// Configs parsed from BENCH_ env vars. Unknown BENCH_ vars are an error
const CONFIG = {};

const URL        = env_str('URL', 'http://localhost:15984');
const USER       = env_str('USER', 'adm');
const PASS       = env_str('PASS', 'pass');
const DB         = env_str('DB', 'bench_db');
const Q          = env_num('Q', 4, 1);
const SCENARIOS  = env_str('SCENARIOS', 'doc_get,doc_insert');
const DURATION   = env_str('DURATION', '5m');
const DOCS       = env_num('DOCS', 100000, 0);
const DOC_SIZE   = env_num('DOC_SIZE', 256, 0);
const IDS        = env_enum('IDS', 'ordered', ['ordered', 'random']);
const W          = env_num('W', 3, 0);
const BATCH_SIZE = env_num('BATCH_SIZE', 500, 1);
const REUSE_DB   = env_num('REUSE_DB', 0, 0, 1);
const COMPACT    = env_num('COMPACT', 1, 0, 1);
const TEARDOWN   = env_num('TEARDOWN', 1, 0, 1);
const TAG        = env_str('TAG', '');
const XHEADER    = env_str('XHEADER', '');
const VIEW_KEYS  = env_num('VIEW_KEYS', 1000, 1);

check_unknown_env();

// Derived params and constants

const DB_URL       = `${URL}/${DB}`;
const W_QS         = W > 0 ? `?w=${W}` : '';
const HEX          = '0123456789abcdef';
// Save params in db. Checked by REUSE_DB
const LOAD_DOC_URL = `${DB_URL}/_local/k6_bench`;
const LOAD_PARAMS  = {docs: DOCS, doc_size: DOC_SIZE, ids: IDS, view_keys: VIEW_KEYS};
const DDOC_NAME    = 'k6_bench_v1';
const DDOC_URL     = `${DB_URL}/_design/${DDOC_NAME}`;
const MAP_VIEW     = 'by_k';
const RED_VIEW     = 'count_by_k';
const MAP_FUN      = 'function(doc){if(typeof doc.k === "string"){emit(doc.k, null);}}';
const KEY_WIDTH    = String(VIEW_KEYS - 1).length;
const VIEW_REDUCE_SPAN = 100;
const VIEW_GROUP_SPAN  = 10;
const BUILD_RETRY_STATUSES = [0, 502, 503, 504];
const SETUP_TIMEOUT   = '4h';
const LOAD_LOG_EVERY  = 50;
const LOAD_PARALLEL   = 8;
const SETTLE_S        = 10;
const DURATION_S      = duration_s(DURATION);
const RAMP_S          = 10;
// Pass/fail limits
const MIN_CHECKS_RATE = 0.999;
const MAX_FAILED_RATE = 0.01;

// reqs: request names (tags) issued, docs: reads loaded docs, views:
// needs the index. A scenario runs the exported function of its name
const SCENARIO_TABLE = {
  welcome    : {rate: 1000, reqs: ['welcome']},
  doc_get    : {rate: 1000, reqs: ['doc_get'], docs: true},
  doc_insert : {rate: 100,  reqs: ['doc_insert']},
  doc_update : {rate: 100,  reqs: ['doc_update_get', 'doc_update_put'], docs: true},
  bulk_docs  : {rate: 2,    reqs: ['bulk_docs']},
  bulk_get   : {rate: 2,    reqs: ['bulk_get'], docs: true},
  all_docs   : {rate: 1,    reqs: ['all_docs']},
  changes    : {rate: 1,    reqs: ['changes']},
  view_query : {rate: 10,   reqs: ['view_query'], docs: true, views: true},
  view_reduce: {rate: 10,   reqs: ['view_reduce'], docs: true, views: true},
  view_group : {rate: 10,   reqs: ['view_group'], docs: true, views: true}
};
const SELECTED   = parse_scenarios(SCENARIOS);
const USES_VIEWS = SELECTED.some(s => SCENARIO_TABLE[s.name].views);
const REQ_NAMES  = SELECTED.flatMap(s => SCENARIO_TABLE[s.name].reqs);
CONFIG.scenarios = SELECTED.map(s => `${s.name}:${s.rate}`).join(',');

const HEADERS = get_headers(XHEADER, USER, PASS);
// Setup expects some 4xx, a 412 for an existing db say, so only 5xx
// count as failed there
const SETUP_PAR = {headers: HEADERS, tags: {name: 'setup'}, responseType: 'text',
                   responseCallback: http.expectedStatuses({min: 200, max: 499})};
const PAR = Object.fromEntries(Object.values(SCENARIO_TABLE).flatMap(s => s.reqs)
  .map(n => [n, {headers: HEADERS, tags: {name: n}}]));
PAR.doc_update_get.responseType = 'text';
// Conflicts are expected and counted in UPDATE_CONFLICTS
PAR.doc_update_put.responseCallback = http.expectedStatuses(201, 202, 409);
// Same params tagged apart, for requests sent during the ramp
const RAMP_PAR = Object.fromEntries(Object.entries(PAR)
  .map(([n, par]) => [n, {...par, tags: {name: 'ramp'}}]));

const UPDATE_CONFLICTS = new Counter('doc_update_conflicts');

// k6 options and callbacks

export const options = {
   scenarios: scenarios(),
   // Requests that need a response body set responseType: 'text'
   discardResponseBodies: true,
   setupTimeout: SETUP_TIMEOUT,
   batch: LOAD_PARALLEL,
   batchPerHost: LOAD_PARALLEL,
   summaryTrendStats: ['avg', 'min', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
   thresholds: thresholds()
};

export function setup() {
  const t0 = Date.now();
  print_config();
  if (REUSE_DB > 0 && db_is_reusable()) {
      console.log(`reusing existing db ${DB_URL}, skipping the load (q=${Q} is ignored)`);
  } else {
      create_db();
      load_docs();
  }
  if (COMPACT > 0) {
      compact(`${DB_URL}/_compact`, DB_URL, info => info.compact_running, `db ${DB}`);
  }
  if (USES_VIEWS) {
      create_ddoc();
      build_view();
      if (COMPACT > 0) {
          compact(`${DB_URL}/_compact/${DDOC_NAME}`, `${DDOC_URL}/_info`,
                  info => info.view_index.compact_running, `index ${DDOC_NAME}`);
      }
  }
  sleep(SETTLE_S);
  return {setup_ms: Date.now() - t0};
}

export function teardown(data) {
    if (TEARDOWN > 0) {
      const res = http.del(DB_URL, null, SETUP_PAR);
      if (!is_ok(res)) {
          throw new Error(`In teardown could not delete DB ${DB_URL} ${res.status} ${res.body}`);
      }
    }
}

// k6 calls this even when setup failed
export function handleSummary(data) {
  if (!data.setup_data) {
    return {'stdout': '\n  setup did not finish, so there is no summary\n\n'};
  }
  const out = {'stdout': compact_summary(data)};
  if (TAG) {
    out[`summary-${TAG}.json`] = JSON.stringify(data, null, 2);
  }
  return out;
}

function parse_scenarios(spec) {
  const entries = spec.split(',').map(e => e.trim());
  if (entries.some(e => !e)) {
    throw new Error('BENCH_SCENARIOS must be a comma separated list of name or name:rate entries');
  }
  const parsed = entries.map(e => {
    const [name, rate, ...rest] = e.split(':');
    if (rest.length > 0 || rate === '') {
      throw new Error(`Invalid BENCH_SCENARIOS entry ${e}; expected name or name:rate`);
    }
    return {name: name, rate: rate};
  });
  const names = parsed.map(s => s.name);
  const invalid = names.filter(n => !SCENARIO_TABLE[n]);
  if (invalid.length > 0) {
    const valid = Object.keys(SCENARIO_TABLE).join(', ');
    throw new Error(`Invalid scenarios: ${invalid.join(', ')}. Valid ones are: ${valid}`);
  }
  const duplicates = names.filter((n, i) => names.indexOf(n) !== i);
  if (duplicates.length > 0) {
    throw new Error(`Duplicate scenarios: ${[...new Set(duplicates)].join(', ')}`);
  }
  const need_docs = names.filter(n => SCENARIO_TABLE[n].docs);
  if (need_docs.length > 0 && DOCS === 0) {
    throw new Error(`BENCH_DOCS must be greater than 0 for ${need_docs.join(', ')}`);
  }
  return parsed.map(s => ({
    name: s.name,
    rate: s.rate === undefined ? SCENARIO_TABLE[s.name].rate : to_int(s.rate, `rate for ${s.name}`, 1)
  }));
}

function scenarios() {
  return Object.fromEntries(SELECTED.map(s => [s.name, scenario(s.name, s.rate)]));
}

// Ramp up for RAMP_S then run at rate DURATION
// Some servers have connection rate limit so ramping up should help with that some
function scenario(name, rate) {
  return {
    executor: 'ramping-arrival-rate',
    exec: name,
    startRate: 0,
    timeUnit: '1s',
    stages: [{target: rate, duration: `${RAMP_S}s`}, {target: rate, duration: DURATION}],
    preAllocatedVUs: clamp(rate, 10, 500),
    maxVUs: clamp(rate * 10, 100, 5000)
  };
}

// Real limits on checks, failed requests and dropped iterations
function thresholds() {
  const total_rate = SELECTED.reduce((sum, s) => sum + s.rate, 0);
  const th = {
    'checks': [`rate>=${MIN_CHECKS_RATE}`],
    'dropped_iterations': [`count<=${total_rate}`]
  };
  for (const n of REQ_NAMES) {
    th[`http_req_duration{name:${n}}`] = ['p(99)>=0'];
    th[`http_reqs{name:${n}}`] = ['count>=0'];
    th[`http_req_failed{name:${n}}`] = [`rate<${MAX_FAILED_RATE}`];
  }
  return th;
}

// Request params for the measured part of a scenario or the ramp
function par(name) {
  return Date.now() - exec.scenario.startTime < RAMP_S * 1000 ? RAMP_PAR[name] : PAR[name];
}

export function welcome() {
  const res = http.get(URL, par('welcome'));
  check(res, {'welcome status is 200': r => r.status === 200});
}

export function doc_get() {
  const res = http.get(`${DB_URL}/${doc_id(rand_int(0, DOCS - 1))}`, par('doc_get'));
  check(res, {'doc_get status is 200': r => r.status === 200});
}

export function doc_insert() {
  const res = http.post(`${DB_URL}${W_QS}`, new_doc(), par('doc_insert'));
  check(res, {'doc_insert status is 201 or 202': is_written});
}

// res.json('_rev') avoids parsing large docs in JS
export function doc_update() {
  const n = rand_int(0, DOCS - 1);
  const id = doc_id(n);
  const res = http.get(`${DB_URL}/${id}`, par('doc_update_get'));
  if (!check(res, {'doc_update get status is 200': r => r.status === 200})) {
      return;
  }
  const body = doc_json(doc_key(n), null, res.json('_rev'));
  const put_res = http.put(`${DB_URL}/${id}${W_QS}`, body, par('doc_update_put'));
  // Another VU updated the same doc in between
  if (put_res.status === 409) {
      UPDATE_CONFLICTS.add(1);
  }
  check(put_res, {'doc_update put status is 201, 202 or 409': r => is_written(r) || r.status === 409});
}

export function bulk_docs() {
  const docs = [];
  for (let i = 0; i < BATCH_SIZE; i++) {
      docs.push(new_doc());
  }
  const res = http.post(`${DB_URL}/_bulk_docs${W_QS}`, bulk_body(docs), par('bulk_docs'));
  check(res, {'bulk_docs status is 201 or 202': is_written});
}

export function bulk_get() {
  const docs = [];
  for (let i = 0; i < BATCH_SIZE; i++) {
      docs.push({'id': doc_id(rand_int(0, DOCS - 1))});
  }
  const res = http.post(`${DB_URL}/_bulk_get`, JSON.stringify({'docs': docs}), par('bulk_get'));
  check(res, {'bulk_get status is 200': r => r.status === 200});
}

export function all_docs() {
  const start = DOCS > 0 ? `&startkey=${enc_key(doc_id(rand_int(0, DOCS - 1)))}` : '';
  const res = http.get(`${DB_URL}/_all_docs?limit=${BATCH_SIZE}${start}`, par('all_docs'));
  check(res, {'all_docs status is 200': r => r.status === 200});
}

export function changes() {
  const res = http.get(`${DB_URL}/_changes?limit=${BATCH_SIZE}`, par('changes'));
  check(res, {'changes status is 200': r => r.status === 200});
}

export function view_query() {
  const key = view_key(rand_int(0, VIEW_KEYS - 1));
  const qs = `limit=${BATCH_SIZE}&startkey=${enc_key(key)}`;
  const res = http.get(`${DDOC_URL}/_view/${MAP_VIEW}?${qs}`, par('view_query'));
  check(res, {'view_query status is 200': r => r.status === 200});
}

export function view_reduce() {
  const [start, end] = view_key_range(VIEW_REDUCE_SPAN);
  const qs = `startkey=${enc_key(start)}&endkey=${enc_key(end)}`;
  const res = http.get(`${DDOC_URL}/_view/${RED_VIEW}?${qs}`, par('view_reduce'));
  check(res, {'view_reduce status is 200': r => r.status === 200});
}

export function view_group() {
  const [start, end] = view_key_range(VIEW_GROUP_SPAN);
  const qs = `group=true&startkey=${enc_key(start)}&endkey=${enc_key(end)}`;
  const res = http.get(`${DDOC_URL}/_view/${RED_VIEW}?${qs}`, par('view_group'));
  check(res, {'view_group status is 200': r => r.status === 200});
}

// Helpers

function env_str(name, default_val) {
  const value = __ENV['BENCH_' + name] || default_val;
  CONFIG[name.toLowerCase()] = value;
  return value;
}

function env_num(name, default_val, min, max = Infinity) {
  const raw = __ENV['BENCH_' + name];
  const value = to_int(raw === undefined ? default_val : raw, `BENCH_${name}`, min, max);
  CONFIG[name.toLowerCase()] = value;
  return value;
}

function env_enum(name, default_val, allowed) {
  const value = env_str(name, default_val);
  if (!allowed.includes(value)) {
    throw new Error(`Invalid BENCH_${name}=${value}; expected one of ${allowed.join(', ')}`);
  }
  return value;
}

// Footgun prevention. User may set some BENCH_ var hoping they would do
// something. assume they are for us and exit with an error to prevent them
// being silently ignored
function check_unknown_env() {
  const known = new Set(Object.keys(CONFIG).map(k => 'BENCH_' + k.toUpperCase()));
  const unknown = Object.keys(__ENV).filter(k => k.startsWith('BENCH_') && !known.has(k));
  if (unknown.length > 0) {
    const hint = unknown.some(k => k.endsWith('_RATE')) ?
      '. Rates go in BENCH_SCENARIOS as name:rate, for example doc_get:500' : '';
    throw new Error(`Unknown env vars: ${unknown.join(', ')}${hint}`);
  }
}

// The password and the extra header can hold secrets
function print_config() {
  const hidden = ['pass', 'xheader'];
  const lines = Object.entries(CONFIG).map(([k, v]) =>
    `    * ${k}: ${hidden.includes(k) && v !== '' ? '***' : v}`);
  console.log(`\n${lines.join('\n')}\n`);
}

function to_int(value, what, min, max = Infinity) {
  const num = Number(value);
  if (value === '' || !Number.isInteger(num) || num < min || num > max) {
    const range = max === Infinity ? `greater than or equal to ${min}` : `between ${min} and ${max}`;
    throw new Error(`Invalid ${what}=${value}; expected an integer ${range}`);
  }
  return num;
}

// A k6 duration such as 90s, 5m or 1h30m, in seconds
function duration_s(str) {
  const units = {ms: 0.001, s: 1, m: 60, h: 3600, d: 86400};
  let total = 0;
  const rest = str.replace(/(\d+(?:\.\d+)?)(ms|s|m|h|d)/g, (_, n, unit) => {
    total += Number(n) * units[unit];
    return '';
  });
  if (rest !== '' || total <= 0) {
    throw new Error(`Invalid BENCH_DURATION=${str}; expected something like 90s, 5m or 1h30m`);
  }
  return total;
}

function clamp(v, lo, hi) {
  return Math.min(Math.max(v, lo), hi);
}

// Maybe we should do a session thinger here
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

function is_ok(res) {
  return res.status === 200 || res.status === 202;
}

function is_written(res) {
  return res.status === 201 || res.status === 202;
}

// copied from jslib k6-utils so we don't fetch it remotely
function rand_string(length, charset = 'abcdefghijklmnopqrstuvwxyz') {
  let res = '';
  while (length--) {
    res += charset[(Math.random() * charset.length) | 0];
  }
  return res;
}

function rand_int(min, max) {
  return Math.floor(Math.random() * (max - min + 1) + min);
}

// Id of loaded doc n
function doc_id(n) {
  return IDS === 'random' ? md5(String(n), 'hex') : String(n).padStart(32, '0');
}

function doc_key(n) {
  return view_key(n % VIEW_KEYS);
}

function view_key(n) {
  return String(n).padStart(KEY_WIDTH, '0');
}

// A bit before and after with ends inclusive
function view_key_range(span) {
  span = Math.min(span, VIEW_KEYS);
  const start = rand_int(0, VIEW_KEYS - span);
  return [view_key(start), view_key(start + span - 1)];
}

function enc_key(key) {
  return encodeURIComponent(JSON.stringify(key));
}

// A doc body is '{' + per doc fields (_id, _rev, k) + a pre-serialized tail
// from a small per VU pool, built on first use. The idea being generating
// these a lot in parallel is kinda expensive so we pregenerate some on first
// use then keep them around per VU.
let TAILS = null;

function doc_json(key, id, rev) {
  const head = (id ? `"_id":"${id}",` : '') + (rev ? `"_rev":"${rev}",` : '');
  return `{${head}"k":"${key}",${body_tail()}`;
}

// The script picks the id, so the id pattern does not depend on the
// server: random, or time ordered and sorting after the loaded ids
function new_doc() {
  const id = IDS === 'random' ? rand_string(32, HEX) :
    Date.now().toString(16).padStart(12, '0') + rand_string(20, HEX);
  return doc_json(view_key(rand_int(0, VIEW_KEYS - 1)), id);
}

function bulk_body(docs) {
  return `{"docs":[${docs.join(',')}]}`;
}

function body_tail() {
  if (TAILS === null) {
    // Up to 8 tails and about 256KB per VU
    const count = DOC_SIZE > 0 ? clamp(Math.floor(262144 / DOC_SIZE), 1, 8) : 1;
    TAILS = [];
    for (let i = 0; i < count; i++) {
      TAILS.push(object_tail(DOC_SIZE));
    }
  }
  return TAILS[rand_int(0, TAILS.length - 1)];
}

// A mish-mash of stuff
function object_tail(size) {
  const doc = {
    type: 'order',
    ts: new Date(Date.now() - rand_int(0, 1e10)).toISOString(),
    who: `${rand_string(6)} "${rand_string(4)}" Müller`,
    items: []
  };
  let len = JSON.stringify(doc).length;
  for (;;) {
    const item = {
      id: rand_string(8, HEX),
      qty: rand_int(1, 20),
      price: rand_int(100, 99999) / 100,
      ok: Math.random() < 0.5,
      opt: null,
      tags: [rand_string(3), rand_string(5)],
      dim: {w: rand_int(1, 500), h: rand_int(1, 500)}
    };
    const item_len = JSON.stringify(item).length + 1;
    if (len + item_len > size) {
      break;
    }
    doc.items.push(item);
    len += item_len;
  }
  const pad = size - len - 10;
  if (pad > 0) {
    doc.text = rand_string(pad, 'abcdefghijklmnopqrstuvwxyz ');
  }
  return JSON.stringify(doc).slice(1);
}

// Setup helpers

function create_db() {
  let res = http.put(`${DB_URL}?q=${Q}`, null, SETUP_PAR);
  if (res.status === 412) {
      res = http.del(DB_URL, null, SETUP_PAR);
      if (!is_ok(res)) {
          throw new Error(`Could not delete old DB ${DB_URL} ${res.status} ${res.body}`);
      }
      res = http.put(`${DB_URL}?q=${Q}`, null, SETUP_PAR);
  }
  if (!is_written(res)) {
      throw new Error(`Could not create DB ${DB_URL} ${res.status} ${res.body}`);
  }
}

function db_is_reusable() {
  const res = http.get(LOAD_DOC_URL, SETUP_PAR);
  if (res.status !== 200) {
      console.log(`db ${DB_URL} is missing or was not loaded by this script, re-creating it`);
      return false;
  }
  const loaded = res.json('params') || {};
  const diff = Object.keys(LOAD_PARAMS).filter(k => loaded[k] !== LOAD_PARAMS[k]);
  if (diff.length > 0) {
      const was = diff.map(k => `${k}=${loaded[k]}`).join(', ');
      console.log(`db ${DB_URL} was loaded with ${was}, re-creating it`);
      return false;
  }
  return true;
}

// Try to do LOAD_PARALLEL bulk batches at a time
function load_docs() {
  const t0 = Date.now();
  const batches = Math.ceil(DOCS / BATCH_SIZE);
  for (let b = 0; b < batches; b += LOAD_PARALLEL) {
      const last = Math.min(b + LOAD_PARALLEL, batches);
      const reqs = [];
      for (let i = b; i < last; i++) {
          const docs = [];
          for (let n = i * BATCH_SIZE; n < Math.min((i + 1) * BATCH_SIZE, DOCS); n++) {
              docs.push(doc_json(doc_key(n), doc_id(n)));
          }
          reqs.push(['POST', `${DB_URL}/_bulk_docs${W_QS}`, bulk_body(docs), SETUP_PAR]);
      }
      for (const res of http.batch(reqs)) {
          if (!is_written(res)) {
              throw new Error(`Failed _bulk_docs: status=${res.status}, body=${res.body}`);
          }
      }
      if (Math.floor(last / LOAD_LOG_EVERY) > Math.floor(b / LOAD_LOG_EVERY) || last === batches) {
          console.log(`loaded ${Math.min(last * BATCH_SIZE, DOCS)}/${DOCS} docs in ${elapsed_s(t0)}s`);
      }
  }
  const res = http.put(LOAD_DOC_URL, JSON.stringify({params: LOAD_PARAMS}), SETUP_PAR);
  if (!is_written(res)) {
      throw new Error(`Could not write ${LOAD_DOC_URL} ${res.status} ${res.body}`);
  }
}

// Try to compact. The idea being we're starting from stable state each time.
// But depending on the environment the user may not have permissions to
// compact so just ignore it and show an info line about it.
function compact(post_url, info_url, is_running, what) {
  const t0 = Date.now();
  const res = http.post(post_url, null, SETUP_PAR);
  if (res.status === 403) {
      console.log(`not allowed to compact ${what}, skipping it: ${res.json('reason')}`);
      return;
  }
  if (res.status !== 202) {
      throw new Error(`Could not compact ${what} ${res.status} ${res.body}`);
  }
  let seen = false;
  for (;;) {
      sleep(1);
      const info = http.get(info_url, SETUP_PAR);
      if (info.status !== 200) {
          throw new Error(`Could not get ${info_url} ${info.status} ${info.body}`);
      }
      const running = is_running(info.json());
      seen = seen || running;
      if (!running && (seen || Date.now() - t0 > 5000)) {
          break;
      }
  }
  console.log(`compacted ${what} in ${elapsed_s(t0)}s`);
}

// Given a fixed doc id, it could 409 on us. That's fine.
function create_ddoc() {
  const ddoc = {
      'views': {
          [MAP_VIEW]: {'map': MAP_FUN},
          [RED_VIEW]: {'map': MAP_FUN, 'reduce': '_count'}
      }
  };
  const res = http.put(DDOC_URL, JSON.stringify(ddoc), SETUP_PAR);
  if (!is_written(res) && res.status !== 409) {
      throw new Error(`Could not create ddoc ${DDOC_URL} ${res.status} ${res.body}`);
  }
}

// This query builds both views. If we timeout the indexer keeps going, so
// retry. setupTimeout bounds the loop a bit. Yeah, it  ain't pretty but works.
function build_view() {
  console.log(`building ${DDOC_URL} index, this may take a bit ...`);
  const t0 = Date.now();
  const par = {...SETUP_PAR, timeout: SETUP_TIMEOUT};
  for (;;) {
      const res = http.get(`${DDOC_URL}/_view/${MAP_VIEW}?limit=0`, par);
      if (res.status === 200) {
          break;
      }
      if (!BUILD_RETRY_STATUSES.includes(res.status)) {
          throw new Error(`Could not build view ${DDOC_URL} ${res.status} ${res.body}`);
      }
      console.log(`index build query dropped (status ${res.status}) after ${elapsed_s(t0)}s, retrying ...`);
      sleep(5);
  }
  console.log(`index built in ${elapsed_s(t0)}s`);
}

function elapsed_s(t0) {
  return Math.round((Date.now() - t0) / 1000);
}

// A shorter/nicer summary than the default k6 one

function compact_summary(data) {
  const m = data.metrics;
  const setup_ms = data.setup_data.setup_ms;
  // Requests can finish after the run ends, so use the configured or the
  // shorter one it actually ran
  const ran_s = (data.state.testRunDurationMs - setup_ms) / 1000 - RAMP_S;
  const run_s = Math.max(Math.min(DURATION_S, ran_s), 0.001);
  const header = ['request', 'reqs', 'rps', 'fail%', 'avg', 'med', 'p90', 'p95', 'p99', 'max'];
  const rows = REQ_NAMES.map(n => {
    const d = values(m, `http_req_duration{name:${n}}`);
    const r = values(m, `http_reqs{name:${n}}`);
    const f = values(m, `http_req_failed{name:${n}}`);
    const count = r.count || 0;
    return [n, String(count), (count / run_s).toFixed(1), pct(f.rate),
            ms(d.avg), ms(d.med), ms(d['p(90)']), ms(d['p(95)']), ms(d['p(99)']), ms(d.max)];
  });
  const checks = values(m, 'checks');
  const failed = values(m, 'http_req_failed');
  const dropped = values(m, 'dropped_iterations');
  const conflicts = values(m, 'doc_update_conflicts');
  const lines = [''];
  lines.push(`  ${TAG ? `tag: ${TAG}, ` : ''}measured for ${run_s.toFixed(0)}s after a ${(setup_ms / 1000).toFixed(0)}s setup and a ${RAMP_S}s ramp, durations in ms`);
  lines.push('');
  lines.push(...table([header].concat(rows)));
  lines.push('');
  const crossed = Object.keys(m).filter(name =>
    Object.values(m[name].thresholds || {}).some(t => !t.ok));
  lines.push(`  limits: ${crossed.length > 0 ? `FAILED for ${crossed.join(', ')}` : 'passed'}`);
  lines.push(`  checks passed: ${pct(checks.rate)}% (${checks.fails || 0} failed)`);
  lines.push(`  http_req_failed: ${pct(failed.rate)}%`);
  lines.push(`  dropped_iterations: ${dropped.count || 0}` +
             (dropped.count ? ' (no free VU: the server or k6 itself cannot keep up with the rate)' : ''));
  if (conflicts.count) {
    lines.push(`  doc_update conflicts (409): ${conflicts.count}`);
  }
  lines.push(`  vus_max: ${values(m, 'vus_max').value}`);
  lines.push(`  data_received: ${mb(values(m, 'data_received').count)} MB, data_sent: ${mb(values(m, 'data_sent').count)} MB`);
  lines.push('');
  return lines.join('\n') + '\n';
}

function values(metrics, name) {
  return metrics[name] ? metrics[name].values : {};
}

function ms(v) {
  return v === undefined ? '-' : v.toFixed(2);
}

function pct(v) {
  return v === undefined ? '-' : (v * 100).toFixed(2);
}

function mb(v) {
  return v === undefined ? '-' : (v / 1048576).toFixed(1);
}

// A simple table with padding
function table(rows) {
  const widths = rows[0].map((_, i) => Math.max(...rows.map(r => r[i].length)));
  return rows.map(r => '  ' + r.map((c, i) =>
    i === 0 ? c.padEnd(widths[i]) : c.padStart(widths[i])).join('  '));
}
