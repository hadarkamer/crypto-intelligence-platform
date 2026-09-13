// Bound Apps Script for the approved research workbook.
// Set Script Property SHEETS_WEBHOOK_SECRET before deploying as a Web App.
const SHEETS_ALLOCATED_CELL_SOFT_LIMIT_ = 9000000;

function doPost(e) {
  try {
    const body = JSON.parse(e.postData.contents || "{}");
    const expected = PropertiesService.getScriptProperties().getProperty("SHEETS_WEBHOOK_SECRET");
    if (!expected || body.secret !== expected) return json_({ok: false, error: "unauthorized"});
    // This receiver belongs only to the approved research workbook.
    if (body.spreadsheet_id !== "1ci_T6v2r0MeGc3ErOsaY3ftMFo94m4syGF9U94X0fQQ") {
      return json_({ok: false, error: "wrong_workbook"});
    }
    const htmlAckProbe = body.response_mode === "html_ack_probe_v1";
    const htmlAck = body.response_mode === "html_ack_v1";
    if (htmlAckProbe && (!body.payload || body.payload.kind !== "diagnostic_noop" ||
        !Array.isArray(body.payload.upserts) || body.payload.upserts.length !== 0)) {
      throw new Error("HTML ACK probe requires a diagnostic no-op");
    }
    if (htmlAck && (!body.payload || body.payload.kind === "telegram_event_audit_page" ||
        !Array.isArray(body.payload.upserts) || typeof body.ack_nonce !== "string" ||
        !/^[a-f0-9]{32}$/.test(body.ack_nonce))) {
      throw new Error("HTML ACK requires normal upserts and a 32-character hexadecimal nonce");
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
      // Spreadsheet writes can remain buffered. Commit while still holding
      // the lock, including derived audit columns, before confirming success.
      // A failed flush must reject the request but always release the lock.
      try {
        SpreadsheetApp.flush();
      } finally {
        lock.releaseLock();
      }
    }
    if (htmlAckProbe || htmlAck) {
      const signature = Utilities.computeHmacSha256Signature(
        "sheets-ack-v1\n" + e.postData.contents, expected, Utilities.Charset.UTF_8)
        .map(byte => ((byte + 256) % 256).toString(16).padStart(2, "0")).join("");
      return HtmlService.createHtmlOutput("ACK").setTitle("sheets-ack-v1:" + signature);
    }
    return json_({ok: true, version: "sheets-batch-v3"});
  } catch (err) {
    const failure = {ok: false, error: String(err)};
    if (err && (err.error_code === "WORKBOOK_CAPACITY" || err.error_code === "SHEET_CAPACITY")) {
      failure.error_code = err.error_code;
    }
    return json_(failure);
  }
}

function upsert_(ss, item) {
  upsertBatch_(ss, [item]);
}

function upsertBatch_(ss, items) {
  // Read headers and only the key columns needed to locate existing rows.
  // Upserts replace complete rows, so non-key values need not be downloaded.
  // Cache original key columns and overlay staged rows when rebuilding an
  // index for a different key signature within the same request.
  const states = new Map();
  items.forEach(item => {
    let state = states.get(item.sheet);
    if (!state) {
      const sheet = ss.getSheetByName(item.sheet);
      if (!sheet) throw new Error("Unknown sheet: " + item.sheet);
      const width = sheet.getLastColumn();
      if (!width) throw new Error("Missing headers in " + item.sheet);
      const headers = sheet.getRange(1, 1, 1, width).getValues()[0];
      const extra = requiredColumns_(item.sheet).filter(name => !headers.includes(name));
      // Additive columns only; preexisting data/headers and demo keys survive.
      headers.push(...extra);
      const originalRows = Math.max(0, sheet.getLastRow() - 1);
      state = {sheet: sheet, headers: headers, originalWidth: width,
        originalRows: originalRows, rowCount: originalRows, rows: new Map(),
        keyColumns: new Map(), indexes: new Map(), extra: extra};
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
      Formula_Current: ["last_evaluated_at"],
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
      loadKeyColumns_(state, keyIndexes);
      index = new Map();
      for (let position = 0; position < state.rowCount; position++) {
        const staged = state.rows.get(position);
        const key = staged ? keyOf(staged) : JSON.stringify(keyIndexes.map(column =>
          String(state.keyColumns.has(column) ? state.keyColumns.get(column)[position] : "")));
        // Preserve the prior first-match behavior for legacy duplicate rows.
        if (!index.has(key)) index.set(key, position);
      }
      state.indexes.set(signature, index);
    }
    const key = keyOf(values);
    const target = index.has(key) ? index.get(key) : state.rowCount++;
    state.rows.set(target, values);
    index.set(key, target);
    // Other composite indexes may contain a column changed by this upsert.
    // Rebuild them from the in-memory rows if a later item uses that key.
    state.indexes.forEach((unused, other) => {
      if (other !== signature) state.indexes.delete(other);
    });
  });
  // Validate/stage the complete request before performing any writes. Only
  // changed contiguous ranges are written; append batches need one setValues.
  validateBatchCapacity_(ss, states);
  states.forEach(state => {
    if (state.extra.length) {
      ensureColumnCapacity_(state.sheet, state.headers.length);
      state.sheet.getRange(1, 1, 1, state.headers.length).setValues([state.headers]);
    }
    const positions = Array.from(state.rows.keys()).sort((left, right) => left - right);
    const requiredRows = state.rowCount + 1;
    if (requiredRows > state.sheet.getMaxRows()) {
      state.sheet.insertRowsAfter(state.sheet.getMaxRows(), requiredRows - state.sheet.getMaxRows());
    }
    for (let offset = 0; offset < positions.length;) {
      const start = positions[offset];
      let end = start;
      while (++offset < positions.length && positions[offset] === end + 1) end++;
      state.sheet.getRange(start + 2, 1, end - start + 1, state.headers.length)
        .setValues(Array.from({length: end - start + 1}, (_, offset) => state.rows.get(start + offset)));
    }
  });
}

function validateBatchCapacity_(ss, states) {
  let addedCells = 0;
  states.forEach((state, name) => {
    if (name === "Formula_Current" && (state.rowCount > 38144 || state.headers.length > 25)) {
      throw capacityError_("SHEET_CAPACITY", "Formula_Current is limited to 38144 data rows and 25 columns");
    }
    const rows = state.sheet.getMaxRows();
    const columns = state.sheet.getMaxColumns();
    addedCells += Math.max(rows, state.rowCount + 1) * Math.max(columns, state.headers.length) - rows * columns;
  });
  // Existing-row updates still work when an older workbook is above the
  // soft limit. Check all planned growth together before any header or row
  // mutation, and count allocated grid cells across the whole workbook.
  if (!addedCells) return;
  const allocatedCells = ss.getSheets().reduce((total, sheet) =>
    total + sheet.getMaxRows() * sheet.getMaxColumns(), 0);
  if (allocatedCells + addedCells > SHEETS_ALLOCATED_CELL_SOFT_LIMIT_) {
    throw capacityError_("WORKBOOK_CAPACITY", "Workbook cell capacity would exceed the 9000000-cell soft limit");
  }
}

function capacityError_(code, message) {
  const error = new Error(message);
  error.error_code = code;
  return error;
}

function loadKeyColumns_(state, keyIndexes) {
  // Fetch each original key column at most once per request. Adjacent key
  // columns share a read; never include non-key columns between distant keys.
  // Newly added headers have blank original values and need no remote read.
  const missing = Array.from(new Set(keyIndexes)).filter(column =>
    column < state.originalWidth && !state.keyColumns.has(column)).sort((left, right) => left - right);
  if (!state.originalRows) return;
  for (let offset = 0; offset < missing.length;) {
    const start = missing[offset];
    let end = start;
    while (++offset < missing.length && missing[offset] === end + 1) end++;
    const values = state.sheet.getRange(2, start + 1, state.originalRows, end - start + 1).getValues();
    for (let column = start; column <= end; column++) {
      state.keyColumns.set(column, values.map(row => row[column - start]));
    }
  }
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
    validateBatchCapacity_(ss, new Map([["Telegram_Events", {
      sheet: sheet, headers: headers, rowCount: Math.max(0, actualLast - 1),
    }]]));
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
