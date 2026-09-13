"use strict";
// Network-free integration test for Apps Script range I/O and idempotent keys.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const crypto = require("node:crypto");
const context = vm.createContext({});
vm.runInContext(fs.readFileSync(__dirname + "/Code.gs", "utf8"), context);

function sheet(initial, capacity = 100) {
  return {
    rows: initial.map(row => row.slice()), reads: 0, readCells: 0,
    readRanges: [], writes: 0, capacity, columnCapacity: initial[0].length,
    getLastColumn() { return this.rows[0].length; },
    getMaxColumns() { return this.columnCapacity; },
    insertColumnsAfter(position, count) {
      assert.equal(position, this.columnCapacity);
      this.columnCapacity += count;
    },
    getLastRow() { return this.rows.length; },
    getMaxRows() { return this.capacity; },
    insertRowsAfter(position, count) {
      assert.equal(position, this.capacity);
      this.capacity += count;
    },
    getRange(start, column, count, width) {
      const owner = this;
      assert.ok(column >= 1);
      assert.ok(count > 0);
      return {
        getValues() {
          owner.reads++;
          owner.readCells += count * width;
          owner.readRanges.push({start, column, count, width});
          return Array.from({length: count}, (_, offset) =>
            Array.from({length: width}, (_, col) =>
              (owner.rows[start - 1 + offset] || [])[column - 1 + col] ?? ""));
        },
        setValues(values) {
          assert.equal(values.length, count);
          assert.ok(start + count - 1 <= owner.capacity);
          owner.writes++;
          values.forEach((row, offset) => {
            assert.equal(row.length, width);
            if (!owner.rows[start - 1 + offset]) owner.rows[start - 1 + offset] = [];
            row.forEach((value, col) => {owner.rows[start - 1 + offset][column - 1 + col] = value;});
          });
        },
      };
    },
  };
}
const outcomes = sheet([["outcome_id", "direction", "status"], ["old", "LONG", "OPEN"]], 2);
const snapshots = sheet([["snapshot_id", "direction"]]);
const sheets = {Outcomes: outcomes, Snapshots: snapshots};
const ss = {getSheetByName(name) { return sheets[name]; }, getSheets() { return Object.values(sheets); }};
const items = Array.from({length: 32}, (_, i) => ({
  sheet: "Outcomes", key: "outcome_id",
  row: {outcome_id: "event|" + i, direction: i % 2 ? "SHORT" : "LONG", status: "OPEN"},
}));
items.push({sheet: "Outcomes", key: "outcome_id", row: {outcome_id: "old", direction: "SHORT", status: "FAILURE"}});
items.push({sheet: "Outcomes", key: "outcome_id", row: {outcome_id: "event|0", direction: "LONG", status: "SUCCESS"}});
items.push({sheet: "Snapshots", key: "snapshot_id", row: {snapshot_id: "snap-1", direction: "SHORT"}});
context.upsertBatch_(ss, items);
assert.equal(outcomes.reads, 2, "32 outcome rows must share one header and one key-column read");
assert.equal(outcomes.readCells, 4, "non-key outcome cells must not be downloaded");
assert.equal(outcomes.writes, 1, "contiguous updates/appends must share one write");
assert.equal(outcomes.rows.length, 34);
assert.deepEqual(outcomes.rows[1], ["old", "SHORT", "FAILURE"]);
assert.deepEqual(outcomes.rows[2], ["event|0", "LONG", "SUCCESS"]);
assert.equal(snapshots.rows.length, 2);
context.upsertBatch_(ss, items);
assert.equal(outcomes.rows.length, 34, "retry must not append duplicates");
assert.equal(outcomes.reads, 4);
assert.equal(snapshots.rows.length, 2);
const before = outcomes.writes;
assert.throws(() => context.upsertBatch_(ss, [items[0], {sheet: "Outcomes", key: "missing", row: {}}]), /Missing key column/);
assert.equal(outcomes.writes, before, "validate full batch before writing");
assert.throws(() => context.upsertBatch_(ss, [{sheet: "Outcomes", key: "outcome_id", row: {status: "OPEN"}}]), /Missing key value/);
assert.equal(outcomes.writes, before);
// Composite keys normalize numeric cells and preserve nonadjacent old rows.
const composite = sheet([["id", "window", "status"], [1, 60, "OPEN"], [2, 60, "UNCHANGED"], [3, 60, "OPEN"]]);
sheets.Composite = composite;
context.upsertBatch_(ss, [1, 3].map(id => ({sheet: "Composite", key: "id, window", row: {id: String(id), window: 60, status: "SUCCESS"}})));
assert.equal(composite.rows.length, 4);
assert.equal(composite.rows[2][2], "UNCHANGED");
assert.equal(composite.reads, 2, "adjacent composite keys share one read after the header");
assert.equal(composite.readCells, 9);
assert.equal(composite.writes, 2);
console.log("google Apps Script batch self-test: PASS");

// Changing the key signature must see staged replacements, not stale cells.
// Numeric/string matching, legacy duplicates, appended rows and omitted-cell
// replacement semantics must remain identical to the full-row implementation.
const mixed = sheet([["id", "payload", "group", "window", "tail"],
  [1, "first", "old", 60, "clear this"],
  [1, "legacy duplicate", "old", 60, "clear duplicate"],
  [2, "second", "other", 60, "keep second"],
  [3, "third", "old", 240, "keep third"]]);
sheets.Mixed = mixed;
context.upsertBatch_(ss, [
  {sheet: "Mixed", key: "id", row: {id: "1", payload: "moved", group: "fresh", window: 60}},
  {sheet: "Mixed", key: "group,window", row: {id: 9, payload: "duplicate changed", group: "old", window: 60}},
  {sheet: "Mixed", key: "id", row: {id: 1, payload: "first final", group: "final", window: 60}},
  {sheet: "Mixed", key: "group,window", row: {id: 4, payload: "appended", group: "fresh", window: 60}},
  {sheet: "Mixed", key: "id", row: {id: "4", payload: "append final", group: "fresh", window: 60}},
]);
assert.deepEqual(mixed.rows, [
  ["id", "payload", "group", "window", "tail"],
  [1, "first final", "final", 60, ""],
  [9, "duplicate changed", "old", 60, ""],
  [2, "second", "other", 60, "keep second"],
  [3, "third", "old", 240, "keep third"],
  ["4", "append final", "fresh", 60, ""],
]);
assert.deepEqual(mixed.readRanges, [
  {start: 1, column: 1, count: 1, width: 5},
  {start: 2, column: 1, count: 4, width: 1},
  {start: 2, column: 3, count: 4, width: 2},
], "index rebuilds reuse each original key column and exclude payload cells");
assert.equal(mixed.readCells, 17);
const duplicate = sheet([["id", "value"], [7, "first"], ["7", "legacy duplicate"]]);
sheets.Duplicate = duplicate;
context.upsertBatch_(ss, [{sheet: "Duplicate", key: "id", row: {id: "7", value: "new"}}]);
assert.deepEqual(duplicate.rows, [["id", "value"], ["7", "new"], ["7", "legacy duplicate"]]);

// A production-sized tab downloads 80,000 keys, not 2.72 million cells.
// Distant composite keys must not drag all intervening columns into memory.
const largeCount = 80000;
const largeWidth = 34;
const largeHeaders = Array.from({length: largeWidth}, (_, column) => "field_" + column);
largeHeaders[0] = "outcome_id";
largeHeaders[largeWidth - 1] = "window";
const large = sheet([largeHeaders, ...Array.from({length: largeCount}, (_, index) => {
  const row = Array(largeWidth).fill("untouched");
  row[0] = "row-" + index;
  row[largeWidth - 1] = 60;
  return row;
})], largeCount + 1);
sheets.Large = large;
context.upsertBatch_(ss, [{sheet: "Large", key: "outcome_id",
  row: {outcome_id: "row-40000", window: 60, field_1: "updated"}}]);
assert.equal(large.readCells, largeCount + largeWidth);
assert.deepEqual(large.readRanges.map(range => range.width), [largeWidth, 1]);
assert.equal(large.rows[40001][1], "updated");
assert.equal(large.rows[40001][2], "", "absent payload columns clear the replaced row");
assert.equal(large.rows[40000][2], "untouched");
const priorReadCells = large.readCells;
const priorReadRanges = large.readRanges.length;
context.upsertBatch_(ss, [{sheet: "Large", key: "outcome_id,window",
  row: {outcome_id: "row-40000", window: 60, field_1: "composite updated"}}]);
assert.equal(large.readCells - priorReadCells, 2 * largeCount + largeWidth);
assert.deepEqual(large.readRanges.slice(priorReadRanges), [
  {start: 1, column: 1, count: 1, width: largeWidth},
  {start: 2, column: 1, count: largeCount, width: 1},
  {start: 2, column: largeWidth, count: largeCount, width: 1},
]);
assert.equal(large.rows.length, largeCount + 1);
console.log("google Apps Script key-only reads/mixed keys/80k-row volume: PASS");
// Header corruption must reject the complete batch, not silently drop time.
const live = sheet([["מה", "snapshot_id"]]);
sheets["תצוגת לייב"] = live;
const liveItem = {sheet: "תצוגת לייב", key: "snapshot_id",
  row: {snapshot_id: "fresh", "זמן סריקה": "07/09/2026 08:00"}};
const writesBeforeCorruptHeader = outcomes.writes;
assert.throws(() => context.upsertBatch_(ss, [items[0], liveItem]), /source-time column/);
assert.equal(outcomes.writes, writesBeforeCorruptHeader);
assert.equal(live.writes, 0);
live.rows[0][0] = "זמן סריקה";
context.upsertBatch_(ss, [liveItem]);
assert.equal(live.rows[1][0], "07/09/2026 08:00");
live.rows[0].push("זמן סריקה");
assert.throws(() => context.upsertBatch_(ss, [liveItem]), /source-time column/);
assert.equal(live.writes, 1);
const formulaTime = sheet([["id", "renamed_time"]]);
sheets.Formula_Current = formulaTime;
const formulaTimeItem = {sheet: "Formula_Current", key: "id",
  row: {id: "formula-time", last_evaluated_at: "2026-09-13T08:00:00Z"}};
assert.throws(() => context.upsertBatch_(ss, [formulaTimeItem]), /source-time column/);
assert.equal(formulaTime.writes, 0);
formulaTime.rows[0][1] = "last_evaluated_at";
context.upsertBatch_(ss, [formulaTimeItem]);
assert.equal(formulaTime.rows[1][1], formulaTimeItem.row.last_evaluated_at);
formulaTime.rows[0].push("last_evaluated_at");
assert.throws(() => context.upsertBatch_(ss, [formulaTimeItem]), /source-time column/);
assert.equal(formulaTime.writes, 1);
console.log("google Apps Script timestamp contract self-test: PASS");

// Event-level MaxPain keys preserve both alerts in one scan and every demo.
const tf = sheet([["snapshot_id", "timeframe", "selected_score", "quality_status"],
  ["demo", "24h", 60, "DEMO"]]);
sheets.MaxPain_TF = tf;
const tfItems = ["event1", "event2"].map(event_id => ({sheet: "MaxPain_TF", key: "event_id,timeframe",
  row: {event_id, snapshot_id: "same-scan", timeframe: "24h", selected_score: 70,
    quality_status: "FROZEN_TOTALS_ONLY", timestamp_utc: "2026-09-07T00:00:00Z"}}));
assert.throws(() => context.upsertBatch_(ss, [tfItems[0], {sheet: "Outcomes", key: "missing", row: {}}]), /Missing key column/);
assert.equal(tf.writes, 0, "a later validation failure must not migrate earlier headers");
assert.equal(tf.getMaxColumns(), 4);
assert.deepEqual(tf.rows[1], ["demo", "24h", 60, "DEMO"]);
const tfReadsBeforeMigration = tf.readRanges.length;
context.upsertBatch_(ss, tfItems);
assert.deepEqual(tf.readRanges.slice(tfReadsBeforeMigration), [
  {start: 1, column: 1, count: 1, width: 4},
  {start: 2, column: 2, count: 1, width: 1},
], "new event_id key columns are initially blank and require no remote read");
context.upsertBatch_(ss, tfItems);
assert.equal(tf.rows.length, 4);
assert.equal(tf.rows[1][3], "DEMO");
assert.equal(tf.rows[2][0], "same-scan");
// Actual readback is paginated and normalizes proven history without altering it.
const telegram = sheet([["event_id", "timestamp_utc", "record_type", "verification_status", "raw_text"],
  ["historical", "2026-09-05T00:00:00Z", "WATCH_CANDIDATE", "IMPORTED", "🎯 Max Pain\n#1 BTC / 24h"],
  ["other", "2026-09-05T00:00:00Z", "WATCH_CANDIDATE", "IMPORTED", "another kind"],
  ["live", "2026-09-07T00:00:00Z", "MAX_PAIN_ALERT", "DELIVERED", ""],
  ["live", "2026-09-07T00:00:00Z", "MAX_PAIN_ALERT", "DELIVERED", ""]]);
sheets.Telegram_Events = telegram;
const page = context.telegramAuditPage_(ss, {start_row: 2, page_size: 2});
assert.equal(page.rows.length, 2);
assert.equal(page.next_row, 4);
assert.equal(page.complete, false);
assert.equal(page.rows[0].normalized_record_type, "MAX_PAIN_ALERT");
assert.equal(page.rows[1].normalized_record_type, "WATCH_CANDIDATE");
assert.equal(telegram.rows[1][2], "WATCH_CANDIDATE");
assert.equal(telegram.rows[1][3], "IMPORTED");
const page2 = context.telegramAuditPage_(ss, {start_row: 4, page_size: 500, last_row: page.last_row});
assert.equal(page2.complete, true);
assert.equal(page2.rows.filter(row => row.event_id === "live").length, 2);
assert.equal(Object.keys(page2.rows[0]).includes("raw_text"), false);
const priorWrites = telegram.writes;
context.telegramAuditPage_(ss, {start_row: 2, page_size: 2, last_row: page.last_row});
assert.equal(telegram.writes, priorWrites, "unchanged derived fields need no writes");
assert.throws(() => context.telegramAuditPage_(ss, {start_row: 2, page_size: 501}), /bounds/);
assert.throws(() => context.telegramAuditPage_(ss, {start_row: 0, page_size: 2}), /bounds/);
assert.throws(() => context.telegramAuditPage_(ss, {start_row: 2, last_row: 999}), /bounds changed/);
// Auth checks happen before opening the workbook or handling audit reads.
let opened = 0;
context.PropertiesService = {getScriptProperties: () => ({getProperty: () => "test-secret"})};
context.SpreadsheetApp = {openById: () => {opened++; return ss;}};
context.ContentService = {MimeType: {JSON: "JSON"}, createTextOutput: value => ({setMimeType: () => JSON.parse(value)})};
const rejected = context.doPost({postData: {contents: JSON.stringify({secret: "bad", payload: {kind: "telegram_event_audit_page"}})}});
assert.equal(rejected.error, "unauthorized");
assert.equal(opened, 0);
const wrong = context.doPost({postData: {contents: JSON.stringify({secret: "test-secret", spreadsheet_id: "other", payload: {kind: "telegram_event_audit_page"}})}});
assert.equal(wrong.error, "wrong_workbook");
assert.equal(opened, 0);
console.log("google Apps Script event-level rows/authenticated readback: PASS");

// Spreadsheet writes are buffered until flush; exclusive access must cover
// visibility as well as setValues. A failed commit cannot acknowledge a row.
const durable = sheet([["outcome_id", "status"]]);
const committedRange = durable.getRange.bind(durable);
let held = false;
let pending = [];
let flushFailure = null;
let order = [];
durable.getRange = function(...args) {
  const range = committedRange(...args);
  return {
    getValues: () => range.getValues(),
    setValues(values) {
      assert.equal(held, true, "write requires exclusive lock");
      const staged = values.map(row => row.slice());
      pending.push(() => range.setValues(staged));
    },
  };
};
const committedBook = {
  getSheetByName: name => name === "Outcomes" ? durable : sheets[name],
  getSheets: () => Object.keys(sheets).map(name => name === "Outcomes" ? durable : sheets[name]),
};
context.LockService = {getScriptLock: () => ({
  waitLock() { assert.equal(held, false); held = true; order.push("lock"); },
  releaseLock() { assert.equal(held, true); held = false; order.push("release"); },
})};
context.SpreadsheetApp = {
  openById: () => committedBook,
  flush() {
    assert.equal(held, true, "flush must happen before releaseLock");
    order.push("flush");
    if (flushFailure === "before") throw new Error("simulated flush failure");
    const writes = pending;
    pending = [];
    writes.forEach(write => write());
    if (flushFailure === "after") throw new Error("simulated ambiguous flush failure");
  },
};
const request = id => ({postData: {contents: JSON.stringify({
  secret: "test-secret", spreadsheet_id: "1ci_T6v2r0MeGc3ErOsaY3ftMFo94m4syGF9U94X0fQQ",
  payload: {upserts: [{sheet: "Outcomes", key: "outcome_id", row: {outcome_id: id, status: "SUCCESS"}}]},
})}});
const committed = context.doPost(request("flush-success"));
assert.equal(committed.ok, true);
assert.deepEqual(order, ["lock", "flush", "release"]);
assert.deepEqual(durable.rows[1], ["flush-success", "SUCCESS"]);
assert.equal(pending.length, 0);

order = [];
flushFailure = "after";
const ambiguous = context.doPost(request("ambiguous"));
assert.equal(ambiguous.ok, false, "flush failure must not confirm delivery");
assert.equal(held, false, "flush failure must still release the lock");
assert.deepEqual(order, ["lock", "flush", "release"]);
assert.equal(durable.rows.length, 3, "ambiguous failure may already have committed");
flushFailure = null;
assert.equal(context.doPost(request("ambiguous")).ok, true);
assert.equal(durable.rows.length, 3, "retry after ambiguous commit must not duplicate row");

flushFailure = "before";
assert.equal(context.doPost(request("not-yet-committed")).ok, false);
assert.equal(held, false);
assert.equal(durable.rows.length, 3);
// Model an execution ending without committing its staged writes.
pending = [];
flushFailure = null;
assert.equal(context.doPost(request("not-yet-committed")).ok, true);
assert.equal(durable.rows.length, 4);

// Audit also writes derived classification columns; its early return must
// pass through the same flush-and-release boundary, including on failure.
flushFailure = "before";
const auditFlushFailure = context.doPost({postData: {contents: JSON.stringify({
  secret: "test-secret", spreadsheet_id: "1ci_T6v2r0MeGc3ErOsaY3ftMFo94m4syGF9U94X0fQQ",
  payload: {kind: "telegram_event_audit_page", start_row: 2, page_size: 1},
})}});
assert.equal(auditFlushFailure.ok, false);
assert.equal(held, false);
console.log("google Apps Script flush/lock/ambiguous retry self-test: PASS");

// Capacity counts allocated grids, including untouched tabs. A whole batch
// must fit before any write, while updates remain possible above the limit.
const capacityRows = sheet([["id", "value"], ["existing", "old"]], 2);
const allocatedElsewhere = sheet([["id"]], 8999996);
const capacityBook = {
  getSheetByName: name => name === "Target" ? capacityRows : null,
  getSheets: () => [capacityRows, allocatedElsewhere],
};
const capacityItem = (id, value) => ({sheet: "Target", key: "id", row: {id, value}});
context.upsertBatch_(capacityBook, [capacityItem("existing", "at limit")]);
assert.equal(capacityRows.rows[1][1], "at limit");
const capacityWritesBefore = capacityRows.writes;
assert.throws(() => context.upsertBatch_(capacityBook, [
  capacityItem("existing", "must not write"), capacityItem("new", "would expand"),
]), /9000000-cell soft limit/);
assert.equal(capacityRows.writes, capacityWritesBefore);
assert.equal(capacityRows.rows[1][1], "at limit");
assert.equal(capacityRows.capacity, 2);
allocatedElsewhere.capacity = 9000000;
context.upsertBatch_(capacityBook, [capacityItem("existing", "above limit update")]);
assert.equal(capacityRows.rows[1][1], "above limit update");
allocatedElsewhere.capacity = 8999994;
context.upsertBatch_(capacityBook, [capacityItem("new", "exact fit")]);
assert.equal(capacityRows.capacity, 3, "growth to exactly the soft limit is permitted");

const growthA = sheet([["id", "value"], ["old-a", "original"]], 2);
const growthB = sheet([["id", "value"], ["old-b", "original"]], 2);
const combinedOther = sheet([["id"]], 8999990);
const combinedGrowthBook = {
  getSheetByName: name => ({A: growthA, B: growthB})[name],
  getSheets: () => [growthA, growthB, combinedOther],
};
assert.throws(() => context.upsertBatch_(combinedGrowthBook, [
  {sheet: "A", key: "id", row: {id: "new-a"}},
  {sheet: "B", key: "id", row: {id: "new-b"}},
]), /soft limit/);
assert.equal(growthA.writes + growthB.writes, 0, "combined tab growth is checked before the first write");
assert.equal(growthA.capacity + growthB.capacity, 4);

const columnGrowth = sheet([["event_id"], ["existing"]], 2);
const columnOther = sheet([["id"]], 8999994);
const columnBook = {
  getSheetByName: name => name === "Telegram_Events" ? columnGrowth : null,
  getSheets: () => [columnGrowth, columnOther],
};
assert.throws(() => context.upsertBatch_(columnBook, [
  {sheet: "Telegram_Events", key: "event_id", row: {event_id: "existing"}},
]), /soft limit/);
assert.equal(columnGrowth.writes, 0, "additive header growth also obeys prewrite capacity checks");
assert.equal(columnGrowth.getMaxColumns(), 1);
console.log("google Apps Script workbook capacity/batch rejection: PASS");

// Diagnostic HTML ACKs are explicit, authenticated no-ops only. Their title
// signs the exact received UTF-8 body and is created after flush and unlock.
let htmlOutputs = 0;
context.Utilities = {
  Charset: {UTF_8: "UTF-8"},
  computeHmacSha256Signature(message, secret, charset) {
    assert.equal(charset, "UTF-8");
    return Array.from(crypto.createHmac("sha256", secret).update(message, "utf8").digest(),
      byte => byte > 127 ? byte - 256 : byte);
  },
};
context.HtmlService = {
  createHtmlOutput(content) {
    assert.equal(held, false, "HTML success can only be created after unlock");
    assert.deepEqual(order.slice(-2), ["flush", "release"]);
    htmlOutputs++;
    return {setTitle: title => ({html: content, title})};
  },
};
const probeBody = {
  secret: "test-secret", spreadsheet_id: "1ci_T6v2r0MeGc3ErOsaY3ftMFo94m4syGF9U94X0fQQ",
  response_mode: "html_ack_probe_v1", payload: {kind: "diagnostic_noop", upserts: []}, note: "בדיקה",
};
const probeRequest = body => ({postData: {contents: JSON.stringify(body, null, 2)}});
flushFailure = null;
order = [];
const probe = probeRequest(probeBody);
const probeWritesBefore = durable.writes;
const htmlAck = context.doPost(probe);
assert.equal(htmlAck.html, "ACK");
assert.equal(htmlAck.title, "sheets-ack-v1:" + crypto.createHmac("sha256", "test-secret")
  .update("sheets-ack-v1\n" + probe.postData.contents, "utf8").digest("hex"));
assert.match(htmlAck.title, /^sheets-ack-v1:[a-f0-9]{64}$/);
assert.equal(durable.writes, probeWritesBefore);
assert.deepEqual(order, ["lock", "flush", "release"]);
const htmlOutputsBeforeRejected = htmlOutputs;
for (const payload of [
  {kind: "diagnostic_noop", upserts: [{sheet: "Outcomes", key: "outcome_id", row: {outcome_id: "invalid-probe"}}]},
  {kind: "telegram_event_audit_page", upserts: []},
  {kind: "diagnostic_noop"},
  {kind: "diagnostic_noop", upserts: {}},
]) {
  const failedProbe = context.doPost(probeRequest({...probeBody, payload}));
  assert.equal(failedProbe.ok, false);
  assert.match(failedProbe.error, /requires a diagnostic no-op/);
}
assert.equal(htmlOutputs, htmlOutputsBeforeRejected);
assert.equal(durable.writes, probeWritesBefore);
assert.deepEqual(order, ["lock", "flush", "release"], "invalid probes reject before acquiring the lock");
flushFailure = "before";
const failedProbeFlush = context.doPost(probe);
assert.equal(failedProbeFlush.ok, false);
assert.equal(htmlOutputs, htmlOutputsBeforeRejected, "flush errors must never produce a signed HTML success");
assert.equal(held, false);
flushFailure = null;
const ordinaryNoop = context.doPost(probeRequest({...probeBody, response_mode: undefined}));
assert.equal(ordinaryNoop.ok, true);
assert.equal(ordinaryNoop.version, "sheets-batch-v3");
assert.equal(ordinaryNoop.html, undefined, "ordinary ACK format stays JSON");
console.log("google Apps Script authenticated no-op HTML probe: PASS");

// The compact formula projection has a hard row/column bound even when the
// workbook has spare cells or someone preallocates additional blank rows.
const formulaHeaders = ["id", ...Array.from({length: 24}, (_, index) => "value_" + index)];
const boundedFormula = sheet([formulaHeaders,
  ...Array.from({length: 38143}, (_, index) => ["formula-" + index, "original"])], 38145);
const formulaBook = {getSheetByName: name => name === "Formula_Current" ? boundedFormula : null,
  getSheets: () => [boundedFormula]};
const formulaItem = (id, value) => ({sheet: "Formula_Current", key: "id", row: {id, value_0: value}});
context.upsertBatch_(formulaBook, [formulaItem("last", "first value"), formulaItem("last", "last value")]);
assert.equal(boundedFormula.rows.length, 38145);
assert.equal(boundedFormula.rows[38144][1], "last value");
boundedFormula.capacity = 40000;
const formulaWritesBefore = boundedFormula.writes;
assert.throws(() => context.upsertBatch_(formulaBook, [formulaItem("one too many", "reject")]),
  error => error.error_code === "SHEET_CAPACITY" && /38144/.test(error.message));
assert.equal(boundedFormula.writes, formulaWritesBefore);
context.upsertBatch_(formulaBook, [formulaItem("last", "updated at bound")]);
assert.equal(boundedFormula.rows[38144][1], "updated at bound");
assert.equal(boundedFormula.rows.length, 38145);
const wideFormula = sheet([[...formulaHeaders, "unexpected_column"], ["one", "original"]]);
const wideFormulaBook = {getSheetByName: name => name === "Formula_Current" ? wideFormula : capacityRows,
  getSheets: () => [wideFormula, capacityRows]};
const earlierWrites = capacityRows.writes;
assert.throws(() => context.upsertBatch_(wideFormulaBook, [
  capacityItem("existing", "must not write"), formulaItem("one", "must not write"),
]), error => error.error_code === "SHEET_CAPACITY" && /25 columns/.test(error.message));
assert.equal(wideFormula.writes, 0);
assert.equal(capacityRows.writes, earlierWrites, "formula bound validates before any other tab writes");

// The audit migration uses the same whole-workbook growth check as upserts.
const auditCapacity = sheet([["event_id", "timestamp_utc", "record_type", "verification_status", "raw_text"],
  ["event", "2026-09-05T00:00:00Z", "WATCH_CANDIDATE", "IMPORTED", "Max Pain"]], 2);
const auditOther = sheet([["id"]], 8999990);
const auditCapacityBook = {getSheetByName: name => name === "Telegram_Events" ? auditCapacity : null,
  getSheets: () => [auditCapacity, auditOther]};
assert.throws(() => context.telegramAuditPage_(auditCapacityBook, {start_row: 2, page_size: 1}),
  error => error.error_code === "WORKBOOK_CAPACITY");
assert.equal(auditCapacity.writes, 0);
assert.equal(auditCapacity.getMaxColumns(), 5);
assert.equal(auditCapacity.rows[1][2], "WATCH_CANDIDATE");
auditOther.capacity = 8999984;
const fittingAudit = context.telegramAuditPage_(auditCapacityBook, {start_row: 2, page_size: 1});
assert.equal(fittingAudit.rows[0].normalized_record_type, "MAX_PAIN_ALERT");
assert.equal(auditCapacity.getMaxColumns(), 8);
auditOther.capacity = 9000000;
assert.equal(context.telegramAuditPage_(auditCapacityBook, {start_row: 2, page_size: 1}).complete, true);

const normalOpenById = context.SpreadsheetApp.openById;
context.SpreadsheetApp.openById = () => formulaBook;
const formulaCapacityResponse = context.doPost(probeRequest({
  secret: "test-secret", spreadsheet_id: probeBody.spreadsheet_id,
  payload: {upserts: [formulaItem("one too many", "reject")]},
}));
assert.equal(formulaCapacityResponse.ok, false);
assert.equal(formulaCapacityResponse.error_code, "SHEET_CAPACITY");
assert.match(formulaCapacityResponse.error, /38144/);
context.SpreadsheetApp.openById = () => capacityBook;
allocatedElsewhere.capacity = 9000000;
const workbookCapacityResponse = context.doPost(probeRequest({
  secret: "test-secret", spreadsheet_id: probeBody.spreadsheet_id,
  payload: {upserts: [capacityItem("another new", "reject")]},
}));
assert.equal(workbookCapacityResponse.ok, false);
assert.equal(workbookCapacityResponse.error_code, "WORKBOOK_CAPACITY");
assert.match(workbookCapacityResponse.error, /soft limit/);
context.SpreadsheetApp.openById = normalOpenById;
console.log("google Apps Script formula bounds/audit growth/structured capacity errors: PASS");

// Production HTML ACKs require a fresh-format nonce and cover real writes.
// Any validation or flush failure remains an ordinary negative JSON response.
const productionBody = {
  secret: "test-secret", spreadsheet_id: probeBody.spreadsheet_id,
  response_mode: "html_ack_v1", ack_nonce: "0123456789abcdef0123456789abcdef",
  payload: {upserts: [{sheet: "Outcomes", key: "outcome_id",
    row: {outcome_id: "html-production", status: "SUCCESS"}}]},
};
const productionRequest = probeRequest(productionBody);
const productionBefore = durable.rows.length;
const productionAck = context.doPost(productionRequest);
assert.equal(productionAck.html, "ACK");
assert.equal(productionAck.title, "sheets-ack-v1:" + crypto.createHmac("sha256", "test-secret")
  .update("sheets-ack-v1\n" + productionRequest.postData.contents, "utf8").digest("hex"));
assert.equal(durable.rows.length, productionBefore + 1);
assert.deepEqual(durable.rows[productionBefore], ["html-production", "SUCCESS"]);
const productionOutputs = htmlOutputs;
const productionOrderLength = order.length;
for (const ack_nonce of [undefined, "short", "G".repeat(32), "A".repeat(32), 0]) {
  const invalidNonce = context.doPost(probeRequest({...productionBody, ack_nonce}));
  assert.equal(invalidNonce.ok, false);
  assert.match(invalidNonce.error, /nonce/);
}
for (const payload of [{kind: "telegram_event_audit_page", upserts: []}, {upserts: {}}, {}]) {
  assert.equal(context.doPost(probeRequest({...productionBody, payload})).ok, false);
}
assert.equal(htmlOutputs, productionOutputs);
assert.equal(order.length, productionOrderLength, "invalid production mode requests reject before lock/workbook writes");
flushFailure = "after";
const ambiguousProduction = context.doPost(probeRequest({...productionBody,
  ack_nonce: "f".repeat(32), payload: {upserts: [{sheet: "Outcomes", key: "outcome_id",
    row: {outcome_id: "html-ambiguous", status: "SUCCESS"}}]},
}));
assert.equal(ambiguousProduction.ok, false);
assert.equal(ambiguousProduction.html, undefined);
assert.equal(htmlOutputs, productionOutputs);
assert.equal(held, false);
const productionRowsAfterAmbiguous = durable.rows.length;
flushFailure = null;
assert.equal(context.doPost(probeRequest({...productionBody,
  ack_nonce: "e".repeat(32), payload: {upserts: [{sheet: "Outcomes", key: "outcome_id",
    row: {outcome_id: "html-ambiguous", status: "SUCCESS"}}]},
})).html, "ACK");
assert.equal(durable.rows.length, productionRowsAfterAmbiguous, "fresh-nonce retries do not duplicate committed rows");
console.log("google Apps Script production HTML ACK/nonce/ambiguous retry: PASS");
