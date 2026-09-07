// Bound Apps Script for the approved research workbook.
// Set Script Property SHEETS_WEBHOOK_SECRET before deploying as a Web App.
function doPost(e) {
  try {
    const body = JSON.parse(e.postData.contents || "{}");
    const expected = PropertiesService.getScriptProperties().getProperty("SHEETS_WEBHOOK_SECRET");
    if (!expected || body.secret !== expected) return json_({ok: false, error: "unauthorized"});
    const ss = SpreadsheetApp.openById(body.spreadsheet_id);
    const lock = LockService.getScriptLock();
    lock.waitLock(20000);
    try {
      upsertBatch_(ss, body.payload.upserts || []);
    } finally {
      lock.releaseLock();
    }
    return json_({ok: true, version: "sheets-batch-v2"});
  } catch (err) {
    return json_({ok: false, error: String(err)});
  }
}

function upsert_(ss, item) {
  upsertBatch_(ss, [item]);
}

function upsertBatch_(ss, items) {
  // One remote read per affected sheet, including its headers. The outcome
  // worker sends 32 labels for an event; re-reading the entire growing sheet
  // for each label can exceed the webhook timeout and stall the durable outbox.
  const states = new Map();
  items.forEach(item => {
    let state = states.get(item.sheet);
    if (!state) {
      const sheet = ss.getSheetByName(item.sheet);
      if (!sheet) throw new Error("Unknown sheet: " + item.sheet);
      const width = sheet.getLastColumn();
      if (!width) throw new Error("Missing headers in " + item.sheet);
      const all = sheet.getRange(1, 1, Math.max(1, sheet.getLastRow()), width).getValues();
      state = {sheet: sheet, headers: all[0], rows: all.slice(1), indexes: new Map(), changed: new Set()};
      states.set(item.sheet, state);
    }
    const rowObject = item.row || {};
    // A renamed header used to silently discard the source time while the
    // webhook acknowledged success. Validate populated source-time fields
    // before writing any part of this request; never infer a replacement time.
    const timeFields = {
      "תצוגת לייב": ["זמן סריקה"],
      Snapshots: ["timestamp_utc"],
      Telegram_Events: ["timestamp_utc"],
      Outcomes: ["decision_time_utc"],
    }[item.sheet] || [];
    timeFields.forEach(name => {
      if (rowObject[name] !== undefined && rowObject[name] !== "" && rowObject[name] !== null &&
          state.headers.filter(header => header === name).length !== 1) {
        throw new Error("Missing or duplicate source-time column: " + item.sheet + "." + name);
      }
    });
    const values = state.headers.map(header => rowObject[header] === undefined ? "" : rowObject[header]);
    const keyNames = String(item.key || state.headers[0]).split(",").map(name => name.trim());
    const keyIndexes = keyNames.map(name => state.headers.indexOf(name));
    if (keyIndexes.some(index => index < 0)) throw new Error("Missing key column in " + item.sheet);
    if (keyIndexes.some(index => values[index] === "" || values[index] === null)) {
      throw new Error("Missing key value in " + item.sheet);
    }
    const signature = JSON.stringify(keyNames);
    let index = state.indexes.get(signature);
    const keyOf = row => JSON.stringify(keyIndexes.map(column => String(row[column])));
    if (!index) {
      index = new Map();
      state.rows.forEach((row, position) => {
        const key = keyOf(row);
        // Preserve the prior first-match behavior for legacy duplicate rows.
        if (!index.has(key)) index.set(key, position);
      });
      state.indexes.set(signature, index);
    }
    const key = keyOf(values);
    const target = index.has(key) ? index.get(key) : state.rows.length;
    state.rows[target] = values;
    state.changed.add(target);
    index.set(key, target);
    // Other composite indexes may contain a column changed by this upsert.
    // Rebuild them from the in-memory rows if a later item uses that key.
    state.indexes.forEach((unused, other) => {
      if (other !== signature) state.indexes.delete(other);
    });
  });
  // Validate/stage the complete request before performing any writes. Only
  // changed contiguous ranges are written; append batches need one setValues.
  states.forEach(state => {
    const positions = Array.from(state.changed).sort((left, right) => left - right);
    const requiredRows = state.rows.length + 1;
    if (requiredRows > state.sheet.getMaxRows()) {
      state.sheet.insertRowsAfter(state.sheet.getMaxRows(), requiredRows - state.sheet.getMaxRows());
    }
    for (let offset = 0; offset < positions.length;) {
      const start = positions[offset];
      let end = start;
      while (++offset < positions.length && positions[offset] === end + 1) end++;
      state.sheet.getRange(start + 2, 1, end - start + 1, state.headers.length)
        .setValues(state.rows.slice(start, end + 1));
    }
  });
}

function json_(value) {
  return ContentService.createTextOutput(JSON.stringify(value)).setMimeType(ContentService.MimeType.JSON);
}
