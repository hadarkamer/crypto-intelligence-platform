// Bound Apps Script for the approved research workbook.
// Set Script Property SHEETS_WEBHOOK_SECRET before deploying as a Web App.
const SHEETS_ALLOCATED_CELL_SOFT_LIMIT_ = 9000000;
const CURRENT_PUBLICATION_ = {
  "MaxPain_Current": {
    "headers": [
      "snapshot_id",
      "symbol",
      "direction",
      "timeframe",
      "current_price",
      "long_maxpain_price",
      "short_maxpain_price",
      "long_score",
      "short_score",
      "selected_side",
      "selected_score",
      "opposite_score",
      "score_edge",
      "score_ratio",
      "long_liquidity_usd",
      "short_liquidity_usd",
      "selected_liquidity_usd",
      "opposite_liquidity_usd",
      "gap_score",
      "proximity_score",
      "cluster_score",
      "quality_status",
      "calculation_version",
      "event_id",
      "timestamp_utc",
      "source_side",
      "score_direction_basis",
      "consensus_score",
      "components_json",
      "is_alert_timeframe",
      "target_distance_pct",
      "selected_liquidity_share_pct",
      "consensus_hits",
      "consensus_total",
      "source_record_type"
    ],
    "identity": "event_id",
    "key": "symbol,timeframe,source_side",
    "max_rows": 176,
    "sheet": "MaxPain_Current",
    "side": "source_side",
    "symbol": "symbol"
  },
  "Snapshots_Current": {
    "headers": [
      "snapshot_id",
      "timestamp_utc",
      "timestamp_israel",
      "watch_scan_id",
      "parent_event_id",
      "btc_parent_movement_id",
      "symbol",
      "direction",
      "no_alert_snapshot",
      "reference_price",
      "market_session",
      "is_weekend",
      "data_quality_status",
      "alert_sent",
      "alert_types",
      "primary_alert_type",
      "telegram_event_count",
      "price_oi_total_direction",
      "price_oi_total_score",
      "futures_cvd_total_direction",
      "futures_cvd_total_score",
      "spot_cvd_total_direction",
      "spot_cvd_total_score",
      "all_three_aligned",
      "strict_triple_65_match",
      "maxpain_selected_timeframe",
      "maxpain_selected_score",
      "maxpain_opposite_score",
      "maxpain_score_edge",
      "maxpain_score_ratio",
      "maxpain_direction_average",
      "maxpain_opposite_average",
      "maxpain_average_edge",
      "maxpain_average_ratio",
      "consensus_hits",
      "consensus_total",
      "target_price",
      "target_distance_pct",
      "liquidity_balance_pct",
      "selected_liquidity_usd",
      "opposite_liquidity_usd",
      "magnet_exists",
      "magnet_side",
      "magnet_rank",
      "magnet_low",
      "magnet_high",
      "magnet_quality",
      "magnet_spread_pct",
      "liquidity_edge_pct",
      "strategy_version",
      "code_version",
      "snapshot_written_at",
      "displayed_direction",
      "analysis_direction",
      "liquidity_long_pct",
      "liquidity_short_pct",
      "liquidity_timeframe",
      "liquidity_data_source",
      "liquidity_by_timeframe_json"
    ],
    "identity": "snapshot_id",
    "key": "symbol,direction",
    "max_rows": 16,
    "sheet": "Snapshots_Current",
    "side": "direction",
    "symbol": "symbol"
  },
  "Live_Current": {
    "headers": [
      "זמן סריקה",
      "מטבע",
      "כיוון נבדק",
      "מחיר ייחוס",
      "נשלחה התראה",
      "סוג התראה",
      "Price/OI כולל",
      "כיוון Price/OI",
      "Futures CVD כולל",
      "כיוון Futures",
      "Spot CVD כולל",
      "כיוון Spot",
      "שלישייה 65+",
      "MaxPain נבחר",
      "MaxPain נגדי",
      "פער MaxPain",
      "ממוצע לכיוון",
      "ממוצע נגדי",
      "יעד",
      "מרחק ליעד",
      "מאזן נזילות",
      "סטטוס נתונים",
      "snapshot_id",
      "כיוון מוצג",
      "כיוון ניתוח",
      "timestamp_utc"
    ],
    "identity": "snapshot_id",
    "key": "מטבע,כיוון נבדק",
    "max_rows": 16,
    "sheet": "Live_Current",
    "side": "כיוון נבדק",
    "symbol": "מטבע"
  }
};
const CURRENT_SYMBOLS_ = ["BTC", "ETH", "SOL", "HYPE", "DOGE", "ZEC", "BNB", "XRP"];
const CURRENT_TIMEFRAMES_ = ["15m", "30m", "1h", "4h", "12h", "24h", "48h", "3d", "1w", "2w", "1m"];
const TELEGRAM_MAX_ROWS_ = 32000;
const TELEGRAM_PROTECTED_MS_ = 16 * 24 * 60 * 60 * 1000;

function validateCurrentDimensions_(config, key) {
  const domains = config.sheet === "MaxPain_Current" ?
    [CURRENT_SYMBOLS_, CURRENT_TIMEFRAMES_, ["LONG", "SHORT"]] :
    [CURRENT_SYMBOLS_, ["LONG", "SHORT"]];
  if (key.length !== domains.length || key.some((value, i) => !domains[i].includes(String(value)))) {
    throw new Error("Invalid current publication slot");
  }
}

function currentSource_(timestamp, identity) {
  const result = outcomeSource_(timestamp, "1");
  if (typeof identity !== "string" || !identity) throw new Error("Missing current publication source identity");
  result.identity = identity;
  return result;
}

function validateCurrentRow_(state, config, row, keys) {
  if (state.headers.length !== config.headers.length ||
      config.headers.some(name => state.headers.filter(value => value === name).length !== 1) ||
      Object.keys(row).length !== config.headers.length ||
      config.headers.some(name => !Object.prototype.hasOwnProperty.call(row, name))) {
    throw new Error("Current publication requires its complete fixed header contract");
  }
  if (keys.join(",") !== config.key) throw new Error("Invalid current publication key");
  validateCurrentDimensions_(config, keys.map(name => row[name]));
  currentSource_(row.timestamp_utc, row[config.identity]);
}

function olderCurrentSource_(state, config, position, incoming) {
  const columns = ["timestamp_utc", config.identity].map(name => state.headers.indexOf(name));
  const staged = state.rows.get(position);
  if (!staged) loadKeyColumns_(state, columns);
  const prior = columns.map(column => staged ? staged[column] : state.keyColumns.get(column)[position]);
  const existing = currentSource_(prior[0], prior[1]);
  const candidate = currentSource_(incoming.timestamp_utc, incoming[config.identity]);
  if (candidate.timestamp !== existing.timestamp) return candidate.timestamp < existing.timestamp;
  if (candidate.submillisecond !== existing.submillisecond) return candidate.submillisecond < existing.submillisecond;
  return candidate.identity < existing.identity;
}

function telegramTimestamp_(value) {
  // Existing native exports predate ISO(T) normalization. Never infer dates
  // from display text, blanks or numbers when deciding a row can be reused.
  try { return outcomeSource_(typeof value === "string" ? value.replace(" ", "T") : value, "1").timestamp; }
  catch (error) { return NaN; }
}

function reusableTelegramRow_(ss, state, row, keyNames) {
  if (keyNames.join(",") !== "event_id") throw new Error("Telegram retention requires event_id key");
  if (!ss.getSheetByName("Telegram_Archive_20260913")) {
    throw capacityError_("SHEET_CAPACITY", "Telegram archive is required before any row can be reused");
  }
  const cutoff = Date.now() - TELEGRAM_PROTECTED_MS_;
  const incoming = telegramTimestamp_(row.timestamp_utc);
  if (!Number.isFinite(incoming) || incoming < cutoff) {
    throw capacityError_("SHEET_CAPACITY", "Expired Telegram payload stays held in the database");
  }
  const timestampColumn = state.headers.indexOf("timestamp_utc");
  if (timestampColumn < 0) throw new Error("Missing Telegram source timestamp");
  loadKeyColumns_(state, [timestampColumn]);
  let oldest = cutoff, target = null;
  for (let position = 0; position < state.rowCount; position++) {
    const staged = state.rows.get(position);
    const timestamp = telegramTimestamp_(staged ? staged[timestampColumn] : state.keyColumns.get(timestampColumn)[position]);
    if (timestamp < oldest) { oldest = timestamp; target = position; }
  }
  if (target === null) {
    throw capacityError_("SHEET_CAPACITY", "Telegram protected window exceeds 32000 rows; no recent evidence overwritten");
  }
  return target;
}
const OUTCOMES_CURRENT_HEADERS_ = [
  "event_id", "snapshot_id", "symbol", "direction", "threshold_pct", "measurement_start_utc",
  "status", "first_touch_side", "decision_time_utc", "minutes_to_decision", "mfe_pct", "mae_pct",
  "favorable_touch_price", "adverse_touch_price", "max_favorable_price", "max_adverse_price",
  "market_source", "market_pair", "candle_interval", "candle_count", "data_quality_status",
  "outcome_method_version", "outcome_id", "window_minutes", "threshold_bps", "observed_from_utc",
  "observed_through_utc", "terminal_reason", "initial_gap_seconds", "favorable_barrier_price",
  "adverse_barrier_price", "initial_gap_unobserved", "data_quality_note", "path_complete",
];
const OUTCOMES_CURRENT_KEYS_ = ["symbol", "direction", "window_minutes", "threshold_bps"];

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
      const bounded = CURRENT_PUBLICATION_[item.sheet];
      if (bounded && (originalRows > bounded.max_rows || headers.length > bounded.headers.length)) {
        throw capacityError_("SHEET_CAPACITY", "Current publication exceeds its fixed slot capacity");
      }
      state = {sheet: sheet, headers: headers, originalWidth: width,
        originalRows: originalRows, rowCount: originalRows, rows: new Map(),
        keyColumns: new Map(), indexes: new Map(), extra: extra};
      if (item.sheet === "Outcomes_Current") {
        validateOutcomesCurrentHeaders_(state);
      }
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
      Outcomes_Current: ["measurement_start_utc", "decision_time_utc", "observed_through_utc"],
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
    if (item.sheet === "Outcomes_Current") validateOutcomesCurrentRow_(rowObject, keyNames);
    const currentConfig = CURRENT_PUBLICATION_[item.sheet];
    if (currentConfig) validateCurrentRow_(state, currentConfig, rowObject, keyNames);
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
        if (item.sheet === "Outcomes_Current") {
          validateOutcomesCurrentDimensions_(JSON.parse(key));
          if (index.has(key)) throw new Error("Duplicate Outcomes_Current slot");
        }
        if (currentConfig) {
          validateCurrentDimensions_(currentConfig, JSON.parse(key));
          if (index.has(key)) throw new Error("Duplicate current publication slot");
        }
        // Preserve the prior first-match behavior for legacy duplicate rows.
        if (!index.has(key)) index.set(key, position);
      }
      state.indexes.set(signature, index);
    }
    const key = keyOf(values);
    const existing = index.has(key);
    let target = existing ? index.get(key) : state.rowCount;
    if (item.sheet === "Outcomes_Current" && existing && olderOutcomeSource_(state, target, rowObject)) return;
    if (currentConfig && existing && olderCurrentSource_(state, currentConfig, target, rowObject)) return;
    if (!existing && item.sheet === "Telegram_Events" && state.rowCount >= TELEGRAM_MAX_ROWS_) {
      target = reusableTelegramRow_(ss, state, rowObject, keyNames);
      // Replace one physical row. Every other row number stays stable for
      // the 14-day auditor, whose cycle must finish within 24 hours.
      index.forEach((position, oldKey) => { if (position === target) index.delete(oldKey); });
    } else if (!existing) state.rowCount++;
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
    const current = CURRENT_PUBLICATION_[name];
    if (current && (state.rowCount > current.max_rows || state.headers.length > current.headers.length)) {
      throw capacityError_("SHEET_CAPACITY", "Current publication exceeds its fixed slot capacity");
    }
    if (name === "Telegram_Events" && (state.rowCount > TELEGRAM_MAX_ROWS_ || state.headers.length > 15)) {
      throw capacityError_("SHEET_CAPACITY", "Telegram_Events is limited to 32000 data rows and 15 columns");
    }
    if (name === "Formula_Current" && (state.rowCount > 38144 || state.headers.length > 25)) {
      throw capacityError_("SHEET_CAPACITY", "Formula_Current is limited to 38144 data rows and 25 columns");
    }
    if (name === "Outcomes_Current" && (state.rowCount > 512 || state.headers.length > 34)) {
      throw capacityError_("SHEET_CAPACITY", "Outcomes_Current is limited to 512 data rows and 34 columns");
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

function validateOutcomesCurrentHeaders_(state) {
  if (state.originalRows > 512 || state.headers.length > 34) {
    throw capacityError_("SHEET_CAPACITY", "Outcomes_Current is limited to 512 data rows and 34 columns");
  }
  if (state.headers.length !== OUTCOMES_CURRENT_HEADERS_.length ||
      OUTCOMES_CURRENT_HEADERS_.some(name => state.headers.filter(header => header === name).length !== 1)) {
    throw new Error("Outcomes_Current requires the complete 34-column ordered outcome contract");
  }
}

function validateOutcomesCurrentDimensions_(keyValues) {
  const domains = [["BTC", "ETH", "SOL", "HYPE", "DOGE", "ZEC", "BNB", "XRP"],
    ["LONG", "SHORT"], ["60", "240", "720", "1440"], ["25", "50", "75", "100", "125", "150", "175", "200"]];
  if (keyValues.length !== domains.length || keyValues.some((value, index) => !domains[index].includes(String(value)))) {
    throw new Error("Invalid Outcomes_Current slot dimensions");
  }
}

function outcomeSource_(measurementStart, eventId) {
  // Sheet cells can be Dates, while webhook source values are ISO strings.
  const isDate = Object.prototype.toString.call(measurementStart) === "[object Date]";
  const iso = typeof measurementStart === "string" ? measurementStart.match(
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.(\d{1,9}))?(?:Z|[+-]\d{2}:\d{2})$/) : null;
  if (!isDate && (typeof measurementStart !== "string" ||
      !iso)) {
    throw new Error("Invalid Outcomes_Current measurement_start_utc");
  }
  const timestamp = isDate ? measurementStart.getTime() : Date.parse(measurementStart);
  if (!Number.isFinite(timestamp)) throw new Error("Invalid Outcomes_Current measurement_start_utc");
  if ((typeof eventId !== "string" && typeof eventId !== "number") ||
      (typeof eventId === "number" && !Number.isSafeInteger(eventId)) || !/^[1-9]\d{0,18}$/.test(String(eventId)) ||
      (String(eventId).length === 19 && String(eventId) > "9223372036854775807")) {
    throw new Error("Invalid Outcomes_Current event_id");
  }
  return {timestamp: timestamp, submillisecond: iso ? (iso[1] || "").padEnd(9, "0").slice(3) : "000000",
    eventId: String(eventId)};
}

function validateOutcomesCurrentRow_(row, keyNames) {
  if (JSON.stringify(keyNames) !== JSON.stringify(OUTCOMES_CURRENT_KEYS_)) {
    throw new Error("Outcomes_Current requires its stable composite slot key");
  }
  if (Object.keys(row).length !== 34 || OUTCOMES_CURRENT_HEADERS_.some(name =>
      !Object.prototype.hasOwnProperty.call(row, name))) {
    throw new Error("Outcomes_Current requires a complete 34-field row");
  }
  validateOutcomesCurrentDimensions_(keyNames.map(name => row[name]));
  if (typeof row.event_id !== "string" || !Number.isInteger(row.window_minutes) || !Number.isInteger(row.threshold_bps)) {
    throw new Error("Invalid Outcomes_Current source or dimension cell types");
  }
  const source = outcomeSource_(row.measurement_start_utc, row.event_id);
  if (row.outcome_method_version !== "ordered-first-touch-v7" ||
      row.outcome_id !== [source.eventId, row.window_minutes, row.threshold_bps, row.outcome_method_version].join("|") ||
      typeof row.threshold_pct !== "number" || row.threshold_pct !== Number(row.threshold_bps) / 100) {
    throw new Error("Inconsistent Outcomes_Current ordered outcome identity");
  }
}

function olderOutcomeSource_(state, position, incoming) {
  const columns = ["measurement_start_utc", "event_id"].map(name => state.headers.indexOf(name));
  const staged = state.rows.get(position);
  if (!staged) loadKeyColumns_(state, columns);
  const prior = columns.map(column => staged ? staged[column] : state.keyColumns.get(column)[position]);
  const existing = outcomeSource_(prior[0], prior[1]);
  const candidate = outcomeSource_(incoming.measurement_start_utc, incoming.event_id);
  if (candidate.timestamp !== existing.timestamp) return candidate.timestamp < existing.timestamp;
  if (candidate.submillisecond !== existing.submillisecond) return candidate.submillisecond < existing.submillisecond;
  // Compare arbitrary-length positive decimal IDs without floating-point loss.
  if (candidate.eventId.length !== existing.eventId.length) return candidate.eventId.length < existing.eventId.length;
  // Equal source events are deliberately replaceable: a canonical repair can
  // move observed_through_utc or decision_time_utc earlier when a gap is filled.
  return candidate.eventId < existing.eventId;
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
