import fs from 'fs';
import crypto from 'crypto';
import dotenv from 'dotenv';
import { Pool } from 'pg';

dotenv.config();

const empaPath = process.argv[2];
const entradasPath = process.argv[3];

if (!empaPath || !entradasPath) {
  console.error('Uso: npm run import:historicos -- "<ruta_empaquetado.csv>" "<ruta_entradas09.csv>"');
  process.exit(1);
}

if (!process.env.DATABASE_URL) {
  console.error('Falta DATABASE_URL en variables de entorno.');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

function decodeMojibake(value) {
  const text = String(value || '');
  return text
    .replace(/Ã‘/g, 'Ñ')
    .replace(/Ã±/g, 'ñ')
    .replace(/Ãƒâ€˜/g, 'Ñ')
    .replace(/Ãƒâ€“/g, 'Ö')
    .replace(/Ã/g, '');
}

function cleanText(value) {
  return decodeMojibake(value).replace(/\s+/g, ' ').trim();
}

function normalizeText(value) {
  return cleanText(value)
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .toUpperCase();
}

function parseNumber(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  const normalized = raw.replace(/\./g, '').replace(',', '.');
  const num = Number(normalized);
  if (!Number.isFinite(num)) return null;
  return Math.round(num);
}

function pad2(value) {
  return String(value).padStart(2, '0');
}

function parseDateTime(value) {
  const raw = cleanText(value);
  if (!raw) return null;

  const iso = /^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/;
  const dmy = /^(\d{1,2})[\/-](\d{1,2})[\/-](\d{4})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/;

  let match = raw.match(iso);
  if (match) {
    const [, y, m, d, hh = '00', mm = '00', ss = '00'] = match;
    return `${y}-${m}-${d} ${pad2(hh)}:${pad2(mm)}:${pad2(ss)}`;
  }

  match = raw.match(dmy);
  if (match) {
    const [, d, m, y, hh = '00', mm = '00', ss = '00'] = match;
    return `${y}-${pad2(m)}-${pad2(d)} ${pad2(hh)}:${pad2(mm)}:${pad2(ss)}`;
  }

  return null;
}

function parseCsv(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8').replace(/^\uFEFF/, '');
  const lines = raw.split(/\r?\n/).filter((line) => line.trim().length > 0);
  if (!lines.length) return [];

  const headers = splitCsvLine(lines[0]).map((h) => cleanText(h));
  const rows = [];

  for (let i = 1; i < lines.length; i += 1) {
    const parts = splitCsvLine(lines[i]);
    const row = {};
    for (let j = 0; j < headers.length; j += 1) {
      row[headers[j]] = parts[j] !== undefined ? parts[j] : '';
    }
    rows.push(row);
  }

  return rows;
}

function splitCsvLine(line) {
  const out = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    const next = line[i + 1];

    if (ch === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (ch === ',' && !inQuotes) {
      out.push(current);
      current = '';
      continue;
    }

    current += ch;
  }

  out.push(current);
  return out.map((v) => v.trim());
}

function buildHash(record) {
  const payload = [
    record.fecha || '',
    record.fecha_empaquetado || '',
    record.fecha_almacen09 || '',
    record.codigo_producto || '',
    record.producto || '',
    record.cantidad ?? '',
    record.entregado_a || '',
    record.numero_registro || '',
    record.responsable || '',
    record.sede || '',
    record.numero_lote || '',
  ].join('|');

  return crypto.createHash('sha1').update(payload).digest('hex');
}

async function ensureHistoricoTable(client) {
  await client.query(`
    CREATE TABLE IF NOT EXISTS historico_resultados_consolidado (
      id_historico BIGSERIAL PRIMARY KEY,
      fecha DATE,
      fecha_empaquetado TIMESTAMP,
      fecha_almacen09 TIMESTAMP,
      codigo_producto VARCHAR(30),
      producto TEXT NOT NULL,
      cantidad INTEGER,
      entregado_a VARCHAR(120),
      numero_registro VARCHAR(50),
      responsable VARCHAR(120),
      sede VARCHAR(160),
      numero_lote VARCHAR(80),
      source_hash VARCHAR(64) UNIQUE NOT NULL,
      origen_historico VARCHAR(20) NOT NULL DEFAULT 'csv',
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);
}

async function loadProductCodeMap(client) {
  const result = await client.query('SELECT codigo_producto, descripcion FROM productos');
  const map = new Map();
  for (const row of result.rows) {
    const key = normalizeText(row.descripcion);
    if (!key) continue;
    map.set(key, cleanText(row.codigo_producto));
  }
  return map;
}

async function main() {
  const empaRows = parseCsv(empaPath);
  const entradasRows = parseCsv(entradasPath);

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await ensureHistoricoTable(client);

    const productCodeMap = await loadProductCodeMap(client);

    const historicos = [];
    const empaIndex = new Map();

    for (const row of empaRows) {
      const producto = cleanText(row.PRODUCTO);
      if (!producto) continue;

      const fechaEmpTs = parseDateTime(row['Marca temporal']) || parseDateTime(row.FECHA);
      const fecha = fechaEmpTs ? fechaEmpTs.slice(0, 10) : null;
      const lote = cleanText(row['NUMERO DE LOTE']);
      const record = {
        fecha,
        fecha_empaquetado: fechaEmpTs,
        fecha_almacen09: null,
        codigo_producto: productCodeMap.get(normalizeText(producto)) || null,
        producto,
        cantidad: parseNumber(row.CANTIDAD),
        entregado_a: cleanText(row['ENTREGADO A']) || null,
        numero_registro: cleanText(row['NUMERO REGISTRO']) || null,
        responsable: cleanText(row.RESPONSABLE) || null,
        sede: cleanText(row.SEDE) || null,
        numero_lote: lote || null,
      };

      historicos.push(record);

      const key = `${normalizeText(lote)}|${normalizeText(producto)}|${fecha || ''}`;
      if (!empaIndex.has(key)) empaIndex.set(key, []);
      empaIndex.get(key).push(record);
    }

    for (const row of entradasRows) {
      const producto = cleanText(row.PRODUCTO);
      if (!producto) continue;

      const lote = cleanText(row['NUMERO DE LOTE']);
      const fechaEmpTs = parseDateTime(row['FECHA EMPAQUETADO']);
      const fecha = fechaEmpTs ? fechaEmpTs.slice(0, 10) : null;
      const fechaEntradaTs = parseDateTime(row['FECHA ENTRADA']);

      const key = `${normalizeText(lote)}|${normalizeText(producto)}|${fecha || ''}`;
      const candidates = empaIndex.get(key) || [];
      const cantidadEmp = parseNumber(row['CANTIDAD EMPAQUETADO']);
      const match = candidates.find((item) => item.cantidad === cantidadEmp) || candidates[0] || null;

      if (match) {
        if (fechaEntradaTs) match.fecha_almacen09 = fechaEntradaTs;
        if (match.cantidad === null && cantidadEmp !== null) match.cantidad = cantidadEmp;
        if (!match.numero_lote && lote) match.numero_lote = lote;
        continue;
      }

      historicos.push({
        fecha,
        fecha_empaquetado: fechaEmpTs,
        fecha_almacen09: fechaEntradaTs,
        codigo_producto: productCodeMap.get(normalizeText(producto)) || null,
        producto,
        cantidad: cantidadEmp ?? parseNumber(row['CANTIDAD ALMACEN']),
        entregado_a: null,
        numero_registro: null,
        responsable: null,
        sede: null,
        numero_lote: lote || null,
      });
    }

    let inserted = 0;
    let updated = 0;

    for (const item of historicos) {
      const sourceHash = buildHash(item);
      const result = await client.query(
        `INSERT INTO historico_resultados_consolidado (
          fecha,
          fecha_empaquetado,
          fecha_almacen09,
          codigo_producto,
          producto,
          cantidad,
          entregado_a,
          numero_registro,
          responsable,
          sede,
          numero_lote,
          source_hash,
          origen_historico
        ) VALUES (
          $1::date,
          $2::timestamp,
          $3::timestamp,
          $4,
          $5,
          $6,
          $7,
          $8,
          $9,
          $10,
          $11,
          $12,
          'csv'
        )
        ON CONFLICT (source_hash) DO UPDATE
        SET
          fecha_almacen09 = COALESCE(EXCLUDED.fecha_almacen09, historico_resultados_consolidado.fecha_almacen09),
          cantidad = COALESCE(EXCLUDED.cantidad, historico_resultados_consolidado.cantidad),
          numero_lote = COALESCE(EXCLUDED.numero_lote, historico_resultados_consolidado.numero_lote)
        RETURNING (xmax = 0) AS inserted`,
        [
          item.fecha,
          item.fecha_empaquetado,
          item.fecha_almacen09,
          item.codigo_producto,
          item.producto,
          item.cantidad,
          item.entregado_a,
          item.numero_registro,
          item.responsable,
          item.sede,
          item.numero_lote,
          sourceHash,
        ]
      );

      if (result.rows[0]?.inserted) inserted += 1;
      else updated += 1;
    }

    await client.query('COMMIT');
    console.log(`Importacion completada. Insertados: ${inserted}. Actualizados: ${updated}. Total procesados: ${historicos.length}.`);
  } catch (error) {
    await client.query('ROLLBACK');
    console.error('Error en importacion historica:', error.message);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

main();
