import http from 'k6/http';
import { check } from 'k6';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

// Two constant-arrival-rate scenarios running in parallel against the same service.
// async_path showcases the Redis Streams + batch worker design.
// sync_path is the baseline: every request blocks on a PostgreSQL write.
export const options = {
  scenarios: {
    async_path: {
      executor: 'constant-arrival-rate',
      rate: 500,
      timeUnit: '1s',
      duration: '60s',
      preAllocatedVUs: 50,
      maxVUs: 200,
      exec: 'sendAsync',
    },
    sync_path: {
      executor: 'constant-arrival-rate',
      rate: 500,
      timeUnit: '1s',
      duration: '60s',
      preAllocatedVUs: 50,
      maxVUs: 200,
      exec: 'sendSync',
    },
  },
  thresholds: {
    'http_req_duration{scenario:async_path}': ['p(95)<50'],
    'http_req_duration{scenario:sync_path}': ['p(95)<2000'],
  },
};

const BASE_URL = __ENV.TARGET_URL || 'http://localhost:8080';
const HEADERS = { 'Content-Type': 'application/json' };

const PAGES = ['/products/shoes', '/products/shirts', '/cart', '/checkout', '/home'];
const ELEMENTS = ['btn-add-to-cart', 'img-product', 'link-review', 'btn-checkout', 'nav-category'];

function makeEvent() {
  return JSON.stringify({
    event_id: uuidv4(),
    type: 'click',
    session_id: uuidv4(),
    page: PAGES[Math.floor(Math.random() * PAGES.length)],
    timestamp: new Date().toISOString(),
    properties: {
      x: Math.floor(Math.random() * 1920),
      y: Math.floor(Math.random() * 1080),
      element_id: ELEMENTS[Math.floor(Math.random() * ELEMENTS.length)],
    },
  });
}

export function sendAsync() {
  const res = http.post(`${BASE_URL}/events/async`, makeEvent(), { headers: HEADERS });
  check(res, { 'async 202': (r) => r.status === 202 });
}

export function sendSync() {
  const res = http.post(`${BASE_URL}/events/sync`, makeEvent(), { headers: HEADERS });
  check(res, { 'sync 200': (r) => r.status === 200 });
}
