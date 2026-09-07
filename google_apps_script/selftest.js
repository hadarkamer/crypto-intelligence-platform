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
    getLastRow() { return this.rows.length; },
    getMaxRows() { return this.capacity; },
    insertRowsAfter(position, count) {
      assert.equal(position, this.capacity);
      this.capacity += count;
    },
    getRange(start, column, count, width) {
      const owner = this;
      assert.equal(column, 1);
      assert.ok(count > 0);
      return {
        getValues() {
          owner.reads++;
          return Array.from({length: count}, (_, offset) =>
            (owner.rows[start - 1 + offset] || Array(width).fill("")).slice());
        },
        setValues(values) {
          assert.equal(values.length, count);
          assert.ok(start + count - 1 <= owner.capacity);
          owner.writes++;
          values.forEach((row, offset) => {
            assert.equal(row.length, width);
            owner.rows[start - 1 + offset] = Array.from(row);
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
