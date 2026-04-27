import fs from 'fs';
import dotenv from 'dotenv';
import { Pool } from 'pg';

dotenv.config();

const DEFAULT_CSV_PATH = 'C:\\Users\\gvela\\OneDrive\\Escritorio\\Clientes.csv';
const DEFAULT_SCHEMA = 'Nuevas Tablas';
const DEFAULT_TABLE = 'clientes';

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

function parseBooleanFromInactivo(value) {
  const raw = cleanText(value);
  if (raw === null) return true;
  return !['1', 'true', 't', 'si', 'sí', 's', 'y'].includes(raw.toLowerCase());
}

function parseTimestamp(value) {
  const raw = cleanText(value);
  if (!raw) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return `${raw} 00:00:00`;
  }
  const match = raw.match(/^(\d{4})-(\d{2})-(\d{2})(?:\s+(\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,3}))?)?)?$/);
  if (match) {
    const [, y, m, d, hh = '00', mm = '00', ss = '00', ms = '0'] = match;
    return `${y}-${m}-${d} ${hh}:${mm}:${ss}${ms ? `.${ms.padEnd(3, '0')}` : ''}`;
  }
  return raw;
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

function buildAddressValue(row) {
  const parts = [row.direc1, row.dir_ent2, row.direc2]
    .map((value) => cleanText(value))
    .filter(Boolean);
  if (!parts.length) return null;
  return parts.join(' | ').slice(0, 240);
}

function buildClientePayload(row) {
  const fechaCreacion = parseTimestamp(row.fe_us_in) || parseTimestamp(row.fecha_reg);
  const fechaUpdate = parseTimestamp(row.fe_us_mo) || fechaCreacion;
  const rif = cleanText(row.rif) || cleanText(row.co_cli);
  const tipoCliente = cleanText(row.tip_cli);
  const descripcion = cleanText(row.cli_des);
  const responsable = cleanText(row.respons);
  const telefono = cleanText(row.telefonos);

  return {
    tipo_cliente: tipoCliente,
    descripcion,
    id_direc: null,
    telf: telefono,
    estado: parseBooleanFromInactivo(row.inactivo),
    responsable,
    rif,
    codigo_zona: null,
    codigo_seg: null,
    codigo_ven: null,
    id_user: null,
    id_user_modi: null,
    fecha_creacion: fechaCreacion,
    fecha_update: fechaUpdate,
    direccion_texto: buildAddressValue(row),
  };
}

async function ensureTargetTable(client) {
  const result = await client.query(
    `SELECT 1
       FROM information_schema.tables
      WHERE table_schema = $1
        AND table_name = $2`,
    [schemaName, tableName]
  );

  if (!result.rowCount) {
    throw new Error(`No existe la tabla destino ${schemaName}.${tableName}`);
  }
}

async function upsertCliente(client, payload) {
  const columns = [
    'tipo_cliente',
    'descripcion',
    'id_direc',
    'telf',
    'estado',
    'responsable',
    'rif',
    'codigo_zona',
    'codigo_seg',
    'codigo_ven',
    'id_user',
    'id_user_modi',
    'fecha_creacion',
    'fecha_update',
  ];

  const values = columns.map((column) => payload[column]);
  const quotedSchema = quoteIdentifier(schemaName);
  const quotedTable = quoteIdentifier(tableName);

  const query = `
    INSERT INTO ${quotedSchema}.${quotedTable}
      (${columns.map((column) => `"${column}"`).join(', ')})
    VALUES
      (${columns.map((_, index) => `$${index + 1}`).join(', ')})
    ON CONFLICT (rif)
    DO UPDATE SET
      tipo_cliente = EXCLUDED.tipo_cliente,
      descripcion = EXCLUDED.descripcion,
      telf = EXCLUDED.telf,
      estado = EXCLUDED.estado,
      responsable = EXCLUDED.responsable,
      codigo_zona = EXCLUDED.codigo_zona,
      codigo_seg = EXCLUDED.codigo_seg,
      codigo_ven = EXCLUDED.codigo_ven,
      id_user = EXCLUDED.id_user,
      id_user_modi = EXCLUDED.id_user_modi,
      fecha_update = EXCLUDED.fecha_update
  `;

  await client.query(query, values);
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
    await ensureTargetTable(client);

    for (const row of rows) {
      const payload = buildClientePayload(row);
      if (!payload.rif) {
        continue;
      }

      await upsertCliente(client, payload);
      inserted += 1;
    }

    await client.query('COMMIT');
    console.log(`Importación completada: ${inserted} registros procesados desde ${csvPath}`);
    console.log(`Destino: "${schemaName}"."${tableName}"`);
    console.log('Campos foráneos no resueltos automáticamente quedaron en NULL para evitar romper la carga.');
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    console.error('Error al importar clientes:', error.message);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

main();