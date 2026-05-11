import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import { Pool } from 'pg';

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function parseCsvLine(line) {
  const out = [];
  let cur = '';
  let inQuotes = false;

  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (inQuotes) {
      if (ch === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        cur += ch;
      }
    } else if (ch === '"') {
      inQuotes = true;
    } else if (ch === ',') {
      out.push(cur);
      cur = '';
    } else {
      cur += ch;
    }
  }
  out.push(cur);
  return out;
}

function normalizeText(value, maxLength) {
  const raw = String(value ?? '').trim();
  if (!raw || /^null$/i.test(raw)) return null;
  if (typeof maxLength === 'number' && maxLength > 0) return raw.slice(0, maxLength);
  return raw;
}

function parseBoolean(value) {
  const raw = String(value ?? '').trim().toLowerCase();
  if (!raw) return null;
  if (['true', 't', '1', 'si', 'sí', 'y', 'yes'].includes(raw)) return true;
  if (['false', 'f', '0', 'no', 'n'].includes(raw)) return false;
  return null;
}

function parseDate(value) {
  const raw = normalizeText(value, 30);
  if (!raw) return null;
  const d = new Date(raw.replace(' ', 'T'));
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString();
}

function toRecord(headers, values) {
  const rec = {};
  headers.forEach((h, idx) => {
    rec[h] = values[idx] ?? '';
  });
  return rec;
}

async function ensureTable(pool) {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS almacen09_vendedores (
      id_vendedor BIGSERIAL PRIMARY KEY,
      codigo_ven VARCHAR(10),
      tipo VARCHAR(10),
      descripcion VARCHAR(200) NOT NULL,
      cedula VARCHAR(40),
      direc1 VARCHAR(240),
      direc2 VARCHAR(240),
      telefonos VARCHAR(120),
      fecha_creacion TIMESTAMP,
      estado BOOLEAN,
      codigo_zona VARCHAR(20),
      nombre VARCHAR(200),
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`ALTER TABLE almacen09_vendedores ADD COLUMN IF NOT EXISTS codigo_ven VARCHAR(10)`);
  await pool.query(`ALTER TABLE almacen09_vendedores ADD COLUMN IF NOT EXISTS tipo VARCHAR(10)`);
  await pool.query(`ALTER TABLE almacen09_vendedores ADD COLUMN IF NOT EXISTS descripcion VARCHAR(200)`);
  await pool.query(`ALTER TABLE almacen09_vendedores ADD COLUMN IF NOT EXISTS cedula VARCHAR(40)`);
  await pool.query(`ALTER TABLE almacen09_vendedores ADD COLUMN IF NOT EXISTS direc1 VARCHAR(240)`);
  await pool.query(`ALTER TABLE almacen09_vendedores ADD COLUMN IF NOT EXISTS direc2 VARCHAR(240)`);
  await pool.query(`ALTER TABLE almacen09_vendedores ADD COLUMN IF NOT EXISTS telefonos VARCHAR(120)`);
  await pool.query(`ALTER TABLE almacen09_vendedores ADD COLUMN IF NOT EXISTS fecha_creacion TIMESTAMP`);
  await pool.query(`ALTER TABLE almacen09_vendedores ADD COLUMN IF NOT EXISTS estado BOOLEAN`);
  await pool.query(`ALTER TABLE almacen09_vendedores ADD COLUMN IF NOT EXISTS codigo_zona VARCHAR(20)`);
  await pool.query(`ALTER TABLE almacen09_vendedores ADD COLUMN IF NOT EXISTS nombre VARCHAR(200)`);
  await pool.query(`ALTER TABLE almacen09_vendedores DROP CONSTRAINT IF EXISTS almacen09_vendedores_nombre_key`).catch(() => {});
  await pool.query(`UPDATE almacen09_vendedores SET descripcion = COALESCE(NULLIF(TRIM(descripcion), ''), NULLIF(TRIM(nombre), '')) WHERE COALESCE(TRIM(descripcion), '') = ''`).catch(() => {});
  await pool.query(`UPDATE almacen09_vendedores SET nombre = COALESCE(NULLIF(TRIM(nombre), ''), NULLIF(TRIM(descripcion), '')) WHERE COALESCE(TRIM(nombre), '') = ''`).catch(() => {});
  await pool.query(`ALTER TABLE almacen09_vendedores ALTER COLUMN descripcion SET NOT NULL`).catch(() => {});
  await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS idx_almacen09_vendedores_codigo ON almacen09_vendedores(codigo_ven)`);
}

async function main() {
  const csvArg = process.argv[2];
  if (!csvArg) {
    console.error('Uso: node scripts/import-vendedores.js "C:\\ruta\\VendedoresPDT_normalizado.csv"');
    process.exit(1);
  }

  const csvPath = path.resolve(process.cwd(), csvArg);
  if (!fs.existsSync(csvPath)) {
    console.error('No existe el archivo CSV:', csvPath);
    process.exit(1);
  }

  const csv = fs.readFileSync(csvPath, 'utf8');
  const lines = csv.split(/\r?\n/).filter((l) => String(l || '').trim());
  if (lines.length < 2) {
    console.error('CSV sin filas de datos.');
    process.exit(1);
  }

  const headers = parseCsvLine(lines[0]).map((h) => String(h || '').trim().replace(/^"|"$/g, ''));
  const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
  });

  try {
    await ensureTable(pool);
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      let count = 0;
      for (let i = 1; i < lines.length; i += 1) {
        const values = parseCsvLine(lines[i]);
        const row = toRecord(headers, values);

        const codigoVen = normalizeText(row.codigo_ven, 10);
        const descripcion = normalizeText(row.descripcion, 200);
        if (!codigoVen || !descripcion) continue;

        await client.query(
          `INSERT INTO almacen09_vendedores (
             codigo_ven, tipo, descripcion, cedula, direc1, direc2, telefonos,
             fecha_creacion, estado, codigo_zona, nombre
           )
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
           ON CONFLICT (codigo_ven)
           DO UPDATE SET
             tipo = EXCLUDED.tipo,
             descripcion = EXCLUDED.descripcion,
             cedula = EXCLUDED.cedula,
             direc1 = EXCLUDED.direc1,
             direc2 = EXCLUDED.direc2,
             telefonos = EXCLUDED.telefonos,
             fecha_creacion = EXCLUDED.fecha_creacion,
             estado = EXCLUDED.estado,
             codigo_zona = EXCLUDED.codigo_zona,
             nombre = EXCLUDED.nombre`,
          [
            codigoVen,
            normalizeText(row.tipo, 10),
            descripcion,
            normalizeText(row.cedula, 40),
            normalizeText(row.direc1, 240),
            normalizeText(row.direc2, 240),
            normalizeText(row.telefonos, 120),
            parseDate(row.fecha_creacion),
            parseBoolean(row.estado),
            normalizeText(row.codigo_zona, 20),
            descripcion,
          ]
        );
        count += 1;
      }

      await client.query('COMMIT');
      console.log(`Vendedores importados/actualizados: ${count}`);
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  } finally {
    await pool.end();
  }
}

main().catch((error) => {
  console.error('Error importando vendedores:', error.message);
  process.exit(1);
});
