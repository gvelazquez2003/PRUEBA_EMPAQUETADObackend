/**
 * Google Apps Script para Solicitudes y Entregas a Sedes.
 *
 * Espera un payload desde el backend con:
 * {
 *   secret: '...',
 *   tipo: 'solicitud' | 'entrega',
 *   rows: [{ row_reference, ... }]
 * }
 *
 * Configura el secreto en Project Settings > Script properties:
 * SOLICITUDES_SHEETS_WEBHOOK_SECRET
 */
function doPost(e) {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(60000);
    const payload = JSON.parse((e && e.postData && e.postData.contents) || '{}');
    const expectedSecret = PropertiesService.getScriptProperties().getProperty('SOLICITUDES_SHEETS_WEBHOOK_SECRET') || '';

    if (expectedSecret && payload.secret !== expectedSecret) {
      return jsonResponse_(403, { ok: false, error: 'No autorizado' });
    }

    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    if (!rows.length) {
      return jsonResponse_(400, { ok: false, error: 'No hay filas para sincronizar' });
    }

    const spreadsheetId = PropertiesService.getScriptProperties().getProperty('SOLICITUDES_SPREADSHEET_ID') || '';
    const ss = spreadsheetId ? SpreadsheetApp.openById(spreadsheetId) : SpreadsheetApp.getActiveSpreadsheet();

    if (payload.tipo === 'prediccion') {
      return writePrediccionesDemanda_(ss, payload);
    }

    const sheetName = payload.tipo === 'entrega'
      ? (PropertiesService.getScriptProperties().getProperty('SOLICITUDES_SHEET_ENTREGAS') || 'Entregas Sedes')
      : (PropertiesService.getScriptProperties().getProperty('SOLICITUDES_SHEET_SOLICITUDES') || 'Solicitudes Sedes');
    const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);

    const headers = [
      'Row Reference',
      'Tipo',
      'Referencia Externa',
      'Fecha',
      'Hora',
      'Sede ID',
      'Sede',
      'Responsable',
      'Correo',
      'Observaciones',
      'Producto ID',
      'Codigo Producto',
      'Producto',
      'Familia',
      'Cantidad Solicitada',
      'Cantidad Entregada',
      'Unidad',
      'Sincronizado En'
    ];

    ensureHeaders_(sheet, headers);

    const rowReferenceCol = 1;
    const existing = buildExistingReferences_(sheet, rowReferenceCol);
    const now = new Date();
    let inserted = 0;
    let updated = 0;

    rows.forEach((item) => {
      const rowReference = String(item.row_reference || '').trim();
      if (!rowReference) return;
      const values = [
        rowReference,
        item.tipo || payload.tipo || '',
        item.referencia_externa || '',
        item.fecha || '',
        item.hora || '',
        item.sede_id || '',
        item.sede_nombre || '',
        item.responsable_nombre || '',
        item.responsable_email || '',
        item.observaciones || '',
        item.producto_id || '',
        item.codigo_producto || '',
        item.producto || '',
        item.familia || '',
        item.cantidad_solicitada === null || item.cantidad_solicitada === undefined ? '' : item.cantidad_solicitada,
        item.cantidad_entregada === null || item.cantidad_entregada === undefined ? '' : item.cantidad_entregada,
        item.unidad_medida || '',
        now
      ];

      const rowIndex = existing[rowReference];
      if (rowIndex) {
        sheet.getRange(rowIndex, 1, 1, headers.length).setValues([values]);
        updated++;
      } else {
        sheet.appendRow(values);
        inserted++;
      }
    });

    return jsonResponse_(200, { ok: true, sheet: sheetName, inserted: inserted, updated: updated });
  } catch (err) {
    return jsonResponse_(500, { ok: false, error: String(err) });
  } finally {
    try { lock.releaseLock(); } catch (_) {}
  }
}

function ensureHeaders_(sheet, headers) {
  if (sheet.getLastRow() === 0) {
    sheet.appendRow(headers);
    return;
  }

  const current = sheet.getRange(1, 1, 1, Math.max(sheet.getLastColumn(), headers.length)).getValues()[0];
  const hasHeaders = headers.every((header, index) => String(current[index] || '').trim() === header);
  if (!hasHeaders) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }
}

function buildExistingReferences_(sheet, col) {
  const lastRow = sheet.getLastRow();
  const map = {};
  if (lastRow < 2) return map;

  const values = sheet.getRange(2, col, lastRow - 1, 1).getValues();
  values.forEach((row, index) => {
    const key = String(row[0] || '').trim();
    if (key) map[key] = index + 2;
  });
  return map;
}

/**
 * Escribe las predicciones de demanda en el sheet asignado.
 *
 * Columnas en orden: Fecha, Codigo, Producto, Cantidad Proyectada, Sede.
 * Antes de escribir borra las filas de las mismas semanas (para no duplicar).
 *
 * Configura en Script properties:
 *   PREDICCIONES_SHEET_PREDICCIONES  (nombre de la hoja, default 'Predicciones Demanda')
 */
function writePrediccionesDemanda_(ss, payload) {
  const sheetName = PropertiesService.getScriptProperties().getProperty('PREDICCIONES_SHEET_PREDICCIONES') || 'Predicciones Demanda';
  const sheet = ss.getSheetByName(sheetName) || ss.insertSheet(sheetName);
  const headers = ['Fecha', 'Codigo', 'Producto', 'Cantidad Proyectada', 'Sede'];

  const rows = (Array.isArray(payload.rows) ? payload.rows : []).map((item) => [
    item.fecha || '',
    item.codigo || '',
    item.producto || '',
    item.cantidad_proyectada === null || item.cantidad_proyectada === undefined ? '' : item.cantidad_proyectada,
    item.sede || '',
  ]);

  if (!rows.length) {
    return jsonResponse_(200, { ok: true, sheet: sheetName, inserted: 0, updated: 0, message: 'Sin filas para escribir' });
  }

  const targetWeeks = new Set(rows.map((row) => weekKey_(row[0])));
  const lastRow = sheet.getLastRow();
  const keep = [];
  if (lastRow > 1) {
    const values = sheet.getRange(2, 1, lastRow - 1, headers.length).getValues();
    values.forEach((row) => {
      if (row[0] && targetWeeks.has(weekKey_(row[0]))) return;
      keep.push(row);
    });
  }

  const output = [headers].concat(keep, rows);
  sheet.clearContents();
  if (output.length) {
    writeRangeInChunks_(sheet, 1, output, headers.length, 5000);
  }

  return jsonResponse_(200, { ok: true, sheet: sheetName, inserted: rows.length, updated: 0, message: 'Predicciones escritas correctamente' });
}

function writeRangeInChunks_(sheet, startRow, rows, numCols, chunkSize) {
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    sheet.getRange(startRow + i, 1, chunk.length, numCols).setValues(chunk);
  }
}

function weekKey_(dateValue) {
  if (!dateValue) return '';
  let d;
  if (dateValue instanceof Date) {
    d = dateValue;
  } else {
    d = new Date(String(dateValue).slice(0, 10) + 'T00:00:00Z');
  }
  if (isNaN(d.getTime())) return '';
  const dow = (d.getUTCDay() + 6) % 7;
  const monday = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate() - dow));
  return monday.toISOString().slice(0, 10);
}

function jsonResponse_(status, obj) {
  return ContentService
    .createTextOutput(JSON.stringify(Object.assign({ status: status }, obj)))
    .setMimeType(ContentService.MimeType.JSON);
}
