"use strict";
// Network-free integration test for Apps Script range I/O and idempotent keys.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const context = vm.createContext({});
vm.runInContext(fs.readFileSync(__dirname + "/Code.gs", "utf8"), context);

function sheet(initial, capacity = 100) {
  return {
    rows: initial.map(row => row.slice()), reads: 0, writes: 0, capacity,
    getLastColumn() { return this.rows[0].length; },
    getMaxColumns() { return this.rows[0].length; },
    insertColumnsAfter(position, count) {},
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
const ss = {getSheetByName(name) { return sheets[name]; }};
const items = Array.from({length: 32}, (_, i) => ({
  sheet: "Outcomes", key: "outcome_id",
  row: {outcome_id: "event|" + i, direction: i % 2 ? "SHORT" : "LONG", status: "OPEN"},
}));
items.push({sheet: "Outcomes", key: "outcome_id", row: {outcome_id: "old", direction: "SHORT", status: "FAILURE"}});
items.push({sheet: "Outcomes", key: "outcome_id", row: {outcome_id: "event|0", direction: "LONG", status: "SUCCESS"}});
items.push({sheet: "Snapshots", key: "snapshot_id", row: {snapshot_id: "snap-1", direction: "SHORT"}});
context.upsertBatch_(ss, items);
assert.equal(outcomes.reads, 1, "32 outcome rows must share one remote read");
assert.equal(outcomes.writes, 1, "contiguous updates/appends must share one write");
assert.equal(outcomes.rows.length, 34);
assert.deepEqual(outcomes.rows[1], ["old", "SHORT", "FAILURE"]);
assert.deepEqual(outcomes.rows[2], ["event|0", "LONG", "SUCCESS"]);
assert.equal(snapshots.rows.length, 2);
context.upsertBatch_(ss, items);
assert.equal(outcomes.rows.length, 34, "retry must not append duplicates");
assert.equal(outcomes.reads, 2);
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
assert.equal(composite.reads, 1);
assert.equal(composite.writes, 2);
console.log("google Apps Script batch self-test: PASS");
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
console.log("google Apps Script timestamp contract self-test: PASS");

// Event-level MaxPain keys preserve both alerts in one scan and every demo.
const tf = sheet([["snapshot_id", "timeframe", "selected_score", "quality_status"],
  ["demo", "24h", 60, "DEMO"]]);
sheets.MaxPain_TF = tf;
const tfItems = ["event1", "event2"].map(event_id => ({sheet: "MaxPain_TF", key: "event_id,timeframe",
  row: {event_id, snapshot_id: "same-scan", timeframe: "24h", selected_score: 70,
    quality_status: "FROZEN_TOTALS_ONLY", timestamp_utc: "2026-09-07T00:00:00Z"}}));
context.upsertBatch_(ss, tfItems);
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
