'use strict';
// Software guard for fixed fixtures, not an OS firewall/arbitrary-code sandbox.
const net = require('node:net');
const dns = require('node:dns');
const fs = require('node:fs');
const path = require('node:path');
const counts = {canary_blocked: 0, denied_network: 0, checked_connects: 0, checked_fetches: 0, checked_websockets: 0};
function localHost(host) {
  return host === undefined || host === 'localhost' || host === '127.0.0.1' || host === '::1' || host === '[::1]';
}
function checkHost(host, canary = false) {
  if (!localHost(host)) {
    counts[canary ? 'canary_blocked' : 'denied_network']++;
    throw new Error('Fixture forbids non-loopback network destination');
  }
}
function checkUrl(raw, websocket = false) {
  const url = new URL(typeof raw === 'object' && raw.url ? raw.url : String(raw));
  if (!(websocket ? ['ws:', 'wss:'] : ['http:', 'https:']).includes(url.protocol)) throw new Error('Fixture forbids URL protocol');
  checkHost(url.hostname);
}
function checkConnect(args) {
  if (Array.isArray(args[0])) args = args[0];
  const first = args[0];
  if (first && typeof first === 'object') {
    if (!first.path) checkHost(first.host === undefined ? first.address : first.host);
  } else if (typeof first === 'number' || (typeof first === 'string' && /^\d+$/.test(first))) {
    checkHost(typeof args[1] === 'string' ? args[1] : undefined);
  } else if (typeof first !== 'string' || !first.startsWith('/')) {
    throw new Error('Fixture refuses unknown socket arguments');
  }
}
let refused = false;
try { checkHost('forbidden-fixture.invalid', true); } catch { refused = true; }
if (!refused) throw new Error('Fixture egress canary did not reject');
const connect = net.Socket.prototype.connect;
net.Socket.prototype.connect = function(...args) {
  counts.checked_connects++; checkConnect(args); return connect.apply(this, args);
};
const lookup = dns.lookup;
dns.lookup = function(host, ...args) { checkHost(host); return lookup.call(this, host, ...args); };
if (globalThis.fetch) {
  const fetch = globalThis.fetch;
  globalThis.fetch = function(raw, ...args) { counts.checked_fetches++; checkUrl(raw); return fetch.call(this, raw, ...args); };
}
if (globalThis.WebSocket) {
  const WebSocket = globalThis.WebSocket;
  globalThis.WebSocket = class extends WebSocket {
    constructor(url, ...args) { counts.checked_websockets++; checkUrl(url, true); super(url, ...args); }
  };
}
if (process.env.QUEUE_LAB_GUARD_AUDIT_DIR) {
  const output = process.env.QUEUE_LAB_GUARD_AUDIT_DIR;
  if (!fs.statSync(output).isDirectory()) throw new Error('Missing guard evidence directory');
  process.on('exit', () => fs.writeFileSync(path.join(output, `node-${process.pid}.json`), JSON.stringify(counts, null, 2) + '\n'));
}
module.exports = {checkHost, checkUrl, checkConnect, counts};
