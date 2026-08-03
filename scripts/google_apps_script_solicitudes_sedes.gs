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
    lock.waitLock(30000);
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

function jsonResponse_(status, obj) {
  return ContentService
    .createTextOutput(JSON.stringify(Object.assign({ status: status }, obj)))
    .setMimeType(ContentService.MimeType.JSON);
}
