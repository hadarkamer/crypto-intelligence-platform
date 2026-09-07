// Bound Apps Script for the approved research workbook.
// Set Script Property SHEETS_WEBHOOK_SECRET before deploying as a Web App.
function doPost(e) {
  try {
    const body = JSON.parse(e.postData.contents || "{}");
    const expected = PropertiesService.getScriptProperties().getProperty("SHEETS_WEBHOOK_SECRET");
    if (!expected || body.secret !== expected) return json_({ok: false, error: "unauthorized"});
    // This receiver belongs only to the approved research workbook.
    if (body.spreadsheet_id !== "1ci_T6v2r0MeGc3ErOsaY3ftMFo94m4syGF9U94X0fQQ") {
      return json_({ok: false, error: "wrong_workbook"});
    }
    const ss = SpreadsheetApp.openById(body.spreadsheet_id);
    const lock = LockService.getScriptLock();
    lock.waitLock(20000);
    try {
      if (body.payload && body.payload.kind === "telegram_event_audit_page") {
        return json_({ok: true, version: "sheets-batch-v3", audit: telegramAuditPage_(ss, body.payload)});
      }
      upsertBatch_(ss, body.payload.upserts || []);
    } finally {
      lock.releaseLock();
    }
    return json_({ok: true, version: "sheets-batch-v3"});
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
      const extra = requiredColumns_(item.sheet).filter(name => !all[0].includes(name));
      // Additive columns only; preexisting data/headers and demo keys survive.
      if (extra.length) {
        all[0].push(...extra);
        all.slice(1).forEach(row => extra.forEach(() => row.push("")));
      }
      state = {sheet: sheet, headers: all[0], rows: all.slice(1), indexes: new Map(), changed: new Set(), extra: extra};
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
      MaxPain_TF: ["timestamp_utc"],
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
    if (state.extra.length) {
      ensureColumnCapacity_(state.sheet, state.headers.length);
      state.sheet.getRange(1, 1, 1, state.headers.length).setValues([state.headers]);
    }
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


function requiredColumns_(name) {
  return ({
    Telegram_Events: ["source_record_type", "normalized_record_type", "classification_version"],
    MaxPain_TF: ["event_id", "timestamp_utc", "source_side", "score_direction_basis",
      "consensus_score", "components_json", "is_alert_timeframe", "target_distance_pct",
      "selected_liquidity_share_pct", "consensus_hits", "consensus_total", "source_record_type"],
  })[name] || [];
}

function ensureColumnCapacity_(sheet, width) {
  if (width > sheet.getMaxColumns()) {
    sheet.insertColumnsAfter(sheet.getMaxColumns(), width - sheet.getMaxColumns());
  }
}

function telegramAuditPage_(ss, payload) {
  // Explicit narrow operation. No arbitrary tab, range, query, or public GET.
  const first = Number(payload.start_row === undefined ? 2 : payload.start_row);
  const count = Number(payload.page_size === undefined ? 500 : payload.page_size);
  if (!Number.isInteger(first) || first < 2 || !Number.isInteger(count) || count < 1 || count > 500) {
    throw new Error("Invalid audit page bounds");
  }
  const sheet = ss.getSheetByName("Telegram_Events");
  if (!sheet) throw new Error("Missing Telegram_Events");
  const actualLast = sheet.getLastRow();
  const last = payload.last_row === undefined ? actualLast : Number(payload.last_row);
  if (!Number.isInteger(last) || last < 1 || last > actualLast) throw new Error("Audit row bounds changed; restart");
  let headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  ["event_id", "timestamp_utc", "record_type", "verification_status", "raw_text"].forEach(name => {
    if (headers.filter(h => h === name).length !== 1) throw new Error("Missing or duplicate audit column: " + name);
  });
  const extras = requiredColumns_("Telegram_Events").filter(name => !headers.includes(name));
  if (extras.length) {
    headers.push(...extras);
    ensureColumnCapacity_(sheet, headers.length);
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }
  const size = Math.min(count, Math.max(0, last - first + 1));
  const rows = size ? sheet.getRange(first, 1, size, headers.length).getValues() : [];
  const at = name => headers.indexOf(name);
  let normalized = 0;
  const changedColumns = new Set();
  const audit = rows.map((row, offset) => {
    const type = String(row[at("record_type")] || "");
    const raw = String(row[at("raw_text")] || "");
    const alias = type === "WATCH_CANDIDATE" && /\bMax\s*Pain\b/i.test(raw) ? "MAX_PAIN_ALERT" : type;
    const derived = {source_record_type: type, normalized_record_type: alias,
      classification_version: "maxpain-source-alias-v1"};
    // Only these audit columns change. Original type, text, IDs and status
    // remain untouched; historical imports never become DELIVERED alerts.
    Object.keys(derived).forEach(name => {
      if (row[at(name)] !== derived[name]) {
        row[at(name)] = derived[name];
        changedColumns.add(name);
      }
    });
    if (alias !== type) normalized++;
    return {event_id: String(row[at("event_id")] || ""),
      timestamp_utc: row[at("timestamp_utc")], record_type: type,
      normalized_record_type: alias, verification_status: String(row[at("verification_status")] || ""),
      row_number: first + offset};
  });
  // Three contiguous columns when freshly migrated, otherwise at most three
  // bounded column writes. Never rewrite the raw source records.
  if (rows.length) changedColumns.forEach(name => {
    const col = at(name);
    sheet.getRange(first, col + 1, rows.length, 1).setValues(rows.map(row => [row[col]]));
  });
  return {rows: audit, start_row: first, next_row: first + size,
    last_row: last, complete: first + size > last, normalized_maxpain_rows: normalized};
}
