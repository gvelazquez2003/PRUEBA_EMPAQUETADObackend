import fs from 'fs';
import dotenv from 'dotenv';
import { Pool } from 'pg';

dotenv.config();

const DEFAULT_CSV_PATH = 'C:\\Users\\gvela\\OneDrive\\Escritorio\\Zonas-PDT.csv';
const DEFAULT_SCHEMA = 'Nuevas Tablas';
const DEFAULT_TABLE = 'zonas';

const csvPath = process.argv[2] || DEFAULT_CSV_PATH;
const schemaName = process.argv[3] || DEFAULT_SCHEMA;
const tableName = process.argv[4] || DEFAULT_TABLE;

if (!process.env.DATABASE_URL) {
  console.error('Falta DATABASE_URL en variables de entorno.');
  process.exit(1);
}

if (!fs.existsSync(csvPath)) {
  console.error(`No existe el archivo CSV: ${csvPath}`);
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

function cleanText(value) {
  const raw = String(value ?? '').replace(/\uFEFF/g, '').trim();
  if (!raw || /^null$/i.test(raw)) return null;
  return raw.replace(/\s+/g, ' ').trim();
}

function quoteIdentifier(value) {
  return `"${String(value ?? '').replace(/"/g, '""')}"`;
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
  return values.map((value) => value.trim());
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
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if ((char === '\n' || char === '\r') && !inQuotes) {
      if (char === '\r' && next === '\n') {
        i += 1;
      }
      rows.push(current);
      current = '';
      continue;
    }

    current += char;
  }

  if (current.trim()) {
    rows.push(current);
  }

  if (!rows.length) return [];

  const headers = splitCsvLine(rows[0]).map((header) => cleanText(header) || '');
  return rows.slice(1)
    .filter((row) => row.trim().length > 0)
    .map((row) => {
      const values = splitCsvLine(row);
      const record = {};
      headers.forEach((header, index) => {
        record[header] = values[index] !== undefined ? values[index] : '';
      });
      return record;
    });
}

function buildZonePayload(row) {
  return {
    id_zona: cleanText(row.co_zon),
    descripcion: cleanText(row.zon_des),
    fecha_creacion: new Date(),
    fecha_modi: null,
    id_user: null,
    id_user_modi: null,
  };
}

async function ensureTargetStructure(client) {
  await client.query(`CREATE SCHEMA IF NOT EXISTS ${quoteIdentifier(schemaName)}`);
  await client.query(`
    ALTER TABLE ${quoteIdentifier(schemaName)}.clientes
      DROP CONSTRAINT IF EXISTS clientes_codigo_zona_fkey
  `);
  await client.query(`
    ALTER TABLE ${quoteIdentifier(schemaName)}.vendedor
      DROP CONSTRAINT IF EXISTS vendedor_id_zona_fkey
  `);
  await client.query(`
    ALTER TABLE ${quoteIdentifier(schemaName)}.zonas
      ALTER COLUMN id_zona DROP IDENTITY IF EXISTS
  `);
  await client.query(`
    ALTER TABLE ${quoteIdentifier(schemaName)}.zonas
      ALTER COLUMN id_zona TYPE VARCHAR(20)
      USING id_zona::text
  `);
  await client.query(`
    ALTER TABLE ${quoteIdentifier(schemaName)}.clientes
      ALTER COLUMN codigo_zona TYPE VARCHAR(20)
      USING codigo_zona::text
  `);
  await client.query(`
    ALTER TABLE ${quoteIdentifier(schemaName)}.vendedor
      ALTER COLUMN id_zona TYPE VARCHAR(20)
      USING id_zona::text
  `);
  await client.query(`
    CREATE TABLE IF NOT EXISTS ${quoteIdentifier(schemaName)}.${quoteIdentifier(tableName)} (
      id_zona VARCHAR(20) PRIMARY KEY,
      descripcion VARCHAR(160) NOT NULL,
      fecha_creacion TIMESTAMP NOT NULL DEFAULT NOW(),
      fecha_modi TIMESTAMP NULL,
      id_user BIGINT NULL,
      id_user_modi BIGINT NULL
    )
  `);

  await client.query(`
    ALTER TABLE ${quoteIdentifier(schemaName)}.${quoteIdentifier(tableName)}
      ALTER COLUMN id_zona DROP IDENTITY IF EXISTS
  `);

  await client.query(`
    ALTER TABLE ${quoteIdentifier(schemaName)}.${quoteIdentifier(tableName)}
      ALTER COLUMN id_zona TYPE VARCHAR(20)
      USING id_zona::text
  `);

  await client.query(`
    ALTER TABLE ${quoteIdentifier(schemaName)}.clientes
      ADD CONSTRAINT clientes_codigo_zona_fkey
      FOREIGN KEY (codigo_zona)
      REFERENCES ${quoteIdentifier(schemaName)}.${quoteIdentifier(tableName)}(id_zona)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION
  `);
  await client.query(`
    ALTER TABLE ${quoteIdentifier(schemaName)}.vendedor
      ADD CONSTRAINT vendedor_id_zona_fkey
      FOREIGN KEY (id_zona)
      REFERENCES ${quoteIdentifier(schemaName)}.${quoteIdentifier(tableName)}(id_zona)
      ON DELETE NO ACTION
      ON UPDATE NO ACTION
  `);
}

async function upsertZone(client, payload) {
  const query = `
    INSERT INTO ${quoteIdentifier(schemaName)}.${quoteIdentifier(tableName)}
      (id_zona, descripcion, fecha_creacion, fecha_modi, id_user, id_user_modi)
    VALUES
      ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (id_zona)
    DO UPDATE SET
      descripcion = EXCLUDED.descripcion,
      fecha_modi = NOW(),
      id_user_modi = EXCLUDED.id_user_modi
  `;

  await client.query(query, [
    payload.id_zona,
    payload.descripcion,
    payload.fecha_creacion,
    payload.fecha_modi,
    payload.id_user,
    payload.id_user_modi,
  ]);
}

async function main() {
  const rows = parseCsv(csvPath);
  if (!rows.length) {
    console.log('No se encontraron filas en el CSV.');
    return;
  }

  const client = await pool.connect();
  let inserted = 0;

  try {
    await client.query('BEGIN');
    await ensureTargetStructure(client);

    for (const row of rows) {
      const payload = buildZonePayload(row);

      if (!payload.id_zona || !payload.descripcion) {
        continue;
      }

      await upsertZone(client, payload);
      inserted += 1;
    }

    await client.query('COMMIT');
    console.log(`Importación completada: ${inserted} registros procesados desde ${csvPath}`);
    console.log(`Destino: "${schemaName}"."${tableName}"`);
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('Error al importar zonas:', error.message);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

main();