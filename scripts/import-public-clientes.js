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
  console.error('Uso: node scripts/import-public-clientes.js "C:\\ruta\\ventas_clientes_neon_zonas.csv"');
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

function assertColumns(rows) {
  const required = ['id_cliente', 'descripcion', 'tipo_cliente', 'direccion', 'zona', 'ruta', 'transporte'];
  const first = rows[0] || {};
  const missing = required.filter((column) => !(column in first));
  if (missing.length) {
    throw new Error(`Faltan columnas requeridas en el CSV: ${missing.join(', ')}`);
  }
}

async function main() {
  const rows = parseCsv(csvPath);
  if (!rows.length) {
    console.log('No se encontraron filas en el CSV.');
    return;
  }
  assertColumns(rows);

  const client = await pool.connect();
  let inserted = 0;

  try {
    await client.query('BEGIN');
    await client.query('TRUNCATE TABLE public.clientes');

    const validRows = rows
      .map((row) => ({
        id_cliente: cleanText(row.id_cliente),
        descripcion: cleanText(row.descripcion),
        tipo_cliente: cleanText(row.tipo_cliente),
        direccion: cleanText(row.direccion),
        ruta: cleanText(row.ruta),
        transporte: cleanText(row.transporte),
        zona: cleanText(row.zona),
      }))
      .filter((row) => row.id_cliente);

    const chunkSize = 250;
    for (let offset = 0; offset < validRows.length; offset += chunkSize) {
      const chunk = validRows.slice(offset, offset + chunkSize);
      const values = [];
      const placeholders = chunk.map((row, rowIndex) => {
        const base = rowIndex * 7;
        values.push(
          row.id_cliente,
          row.descripcion,
          row.tipo_cliente,
          row.direccion,
          row.ruta,
          row.transporte,
          row.zona
        );
        return `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4}, $${base + 5}, $${base + 6}, $${base + 7})`;
      });

      await client.query(
        `INSERT INTO public.clientes (
           id_cliente,
           descripcion,
           tipo_cliente,
           direccion,
           ruta,
           transporte,
           zona
         )
         VALUES ${placeholders.join(', ')}`,
        values
      );
      inserted += chunk.length;
    }

    await client.query('COMMIT');
    console.log(`Importacion completada: ${inserted} clientes insertados en public.clientes.`);
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('Error al importar public.clientes:', error.message);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

main();
