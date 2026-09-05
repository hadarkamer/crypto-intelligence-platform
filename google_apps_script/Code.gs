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
      (body.payload.upserts || []).forEach(item => upsert_(ss, item));
    } finally {
      lock.releaseLock();
    }
    return json_({ok: true});
  } catch (err) {
    return json_({ok: false, error: String(err)});
  }
}

function upsert_(ss, item) {
  const sheet = ss.getSheetByName(item.sheet);
  if (!sheet) throw new Error("Unknown sheet: " + item.sheet);
  const lastColumn = sheet.getLastColumn();
  const headers = sheet.getRange(1, 1, 1, lastColumn).getValues()[0];
  const rowObject = item.row || {};
  const values = headers.map(header => rowObject[header] === undefined ? "" : rowObject[header]);
  const keyNames = String(item.key || headers[0]).split(",");
  const keyIndexes = keyNames.map(name => headers.indexOf(name));
  if (keyIndexes.some(index => index < 0)) throw new Error("Missing key column in " + item.sheet);
  let target = 0;
  if (sheet.getLastRow() > 1) {
    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, lastColumn).getValues();
    target = data.findIndex(existing => keyIndexes.every(index => String(existing[index]) === String(values[index])));
  }
  if (target >= 0) sheet.getRange(target + 2, 1, 1, lastColumn).setValues([values]);
  else sheet.appendRow(values);
}

function json_(value) {
  return ContentService.createTextOutput(JSON.stringify(value)).setMimeType(ContentService.MimeType.JSON);
}
