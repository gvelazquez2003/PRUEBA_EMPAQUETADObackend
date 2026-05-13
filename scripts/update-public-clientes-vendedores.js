import fs from 'fs';
import dotenv from 'dotenv';
import { Pool } from 'pg';

dotenv.config();

const csvPath = process.argv[2];

if (!process.env.DATABASE_URL) {
  console.error('Falta DATABASE_URL en variables de entorno.');
  process.exit(1);
}

if (!csvPath || !fs.existsSync(csvPath)) {
  console.error('Uso: node scripts/update-public-clientes-vendedores.js "C:\\ruta\\Filtro Clientes SinDup - Sheet1.csv"');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

function cleanText(value) {
  return String(value ?? '').replace(/\uFEFF/g, '').trim().replace(/\s+/g, ' ');
}

function splitCsvLine(line) {
  const values = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i];
    const next = line[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === ',' && !inQuotes) {
      values.push(current);
      current = '';
      continue;
    }

    current += char;
  }

  values.push(current);
  return values;
}

function parseCsv(filePath) {
  const raw = fs.readFileSync(filePath, 'utf8').replace(/^\uFEFF/, '');
  const rows = [];
  let current = '';
  let inQuotes = false;

  for (let i = 0; i < raw.length; i += 1) {
    const char = raw[i];
    const next = raw[i + 1];

    if (char === '"') {
      if (inQuotes && next === '"') {
        current += '""';
        i += 1;
      } else {
        inQuotes = !inQuotes;
        current += char;
      }
      continue;
    }

    if ((char === '\n' || char === '\r') && !inQuotes) {
      if (char === '\r' && next === '\n') i += 1;
      if (current.trim()) rows.push(current);
      current = '';
      continue;
    }

    current += char;
  }

  if (current.trim()) rows.push(current);
  if (!rows.length) return [];

  const headers = splitCsvLine(rows[0]).map((header) => cleanText(header));
  return rows.slice(1).map((row) => {
    const values = splitCsvLine(row);
    const record = {};
    headers.forEach((header, index) => {
      record[header] = cleanText(values[index] ?? '');
    });
    return record;
  });
}

function pickField(row, fields) {
  for (const field of fields) {
    const value = row[field];
    if (cleanText(value)) return value;
  }
  return '';
}

function buildPayload(row) {
  return {
    id_cliente: cleanText(pickField(row, ['id_cliente', 'C\u00f3digo de Cliente', 'CÃ³digo de Cliente', 'Codigo de Cliente'])),
    direccion: cleanText(pickField(row, ['direccion', 'dire2'])),
    vendedor: cleanText(pickField(row, ['vendedor', 'Nombre Vendedor'])),
  };
}

async function main() {
  const rows = parseCsv(csvPath).map(buildPayload).filter((row) => row.id_cliente && row.vendedor);
  if (!rows.length) {
    console.log('No se encontraron vendedores validos en el CSV.');
    return;
  }

  const dedupe = new Map();
  rows.forEach((row) => {
    if (!dedupe.has(row.id_cliente)) dedupe.set(row.id_cliente, row);
  });
  const payload = Array.from(dedupe.values());

  const client = await pool.connect();
  let updated = 0;

  try {
    await client.query('BEGIN');
    await client.query('ALTER TABLE public.clientes ADD COLUMN IF NOT EXISTS vendedor TEXT');
    await client.query('UPDATE public.clientes SET vendedor = NULL');

    const chunkSize = 300;
    for (let offset = 0; offset < payload.length; offset += chunkSize) {
      const chunk = payload.slice(offset, offset + chunkSize);
      const values = [];
      const placeholders = chunk.map((row, rowIndex) => {
        const base = rowIndex * 3;
        values.push(row.id_cliente, row.direccion, row.vendedor);
        return `($${base + 1}, $${base + 2}, $${base + 3})`;
      });

      const result = await client.query(
        `WITH incoming(id_cliente, direccion, vendedor) AS (
           VALUES ${placeholders.join(', ')}
         )
         UPDATE public.clientes AS c
            SET vendedor = incoming.vendedor
           FROM incoming
          WHERE c.id_cliente = incoming.id_cliente
             OR (
               c.id_cliente ~ '^[0-9]+$'
               AND incoming.id_cliente ~ '^[0-9]+$'
               AND REGEXP_REPLACE(c.id_cliente, '^0+', '') = REGEXP_REPLACE(incoming.id_cliente, '^0+', '')
             )`,
        values
      );
      updated += result.rowCount || 0;
    }

    const summary = await client.query(`
      SELECT
        COUNT(*)::int AS total,
        COUNT(*) FILTER (WHERE TRIM(COALESCE(vendedor, '')) <> '')::int AS con_vendedor,
        COUNT(*) FILTER (WHERE TRIM(COALESCE(vendedor, '')) = '')::int AS sin_vendedor
      FROM public.clientes
    `);

    await client.query('COMMIT');
    console.log(`Actualizacion completada: ${updated} clientes con vendedor actualizado.`);
    console.table(summary.rows);
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('Error al actualizar vendedores de public.clientes:', error.message);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

main();
