import dotenv from 'dotenv';
import { Pool } from 'pg';

dotenv.config();

const args = process.argv.slice(2);
const apply = args.includes('--apply');
const dates = args.filter((arg) => arg !== '--apply');
const fromDate = String(dates[0] || '').trim();
const toDate = String(dates[1] || '').trim();

if (!/^\d{4}-\d{2}-\d{2}$/.test(fromDate) || !/^\d{4}-\d{2}-\d{2}$/.test(toDate)) {
  console.error('Uso: npm run approve:almacen09-range -- YYYY-MM-DD YYYY-MM-DD [--apply]');
  process.exit(1);
}

if (fromDate > toDate) {
  console.error('La fecha inicial no puede ser posterior a la fecha final.');
  process.exit(1);
}

const databaseUrl = String(process.env.DATABASE_URL || '').trim();
if (!databaseUrl) {
  console.error('DATABASE_URL es obligatorio.');
  process.exit(1);
}

const dbSslValue = String(process.env.DB_SSL || '').trim().toLowerCase();
const databaseUsesSsl = dbSslValue
  ? ['true', '1', 'yes', 'require'].includes(dbSslValue)
  : process.env.NODE_ENV === 'production';

const pool = new Pool({
  connectionString: databaseUrl,
  ssl: databaseUsesSsl ? { rejectUnauthorized: false } : false,
  max: 2,
  connectionTimeoutMillis: Number(process.env.DB_CONNECT_TIMEOUT_MS || 10000),
});

const candidatesSql = `
  WITH detalle_agregado AS (
    SELECT
      ec.id_cabecera,
      CONCAT('CAB-', ec.id_cabecera) AS codigo_lote,
      ec.fecha_hora,
      d.nombre AS destino,
      ed.id_producto,
      p.codigo_producto,
      SUM(ed.cantidad)::int AS cantidad_validada
    FROM (
      SELECT DISTINCT ON (id_detalle) *
      FROM empaquetados_detalle
      ORDER BY id_detalle
    ) ed
    JOIN empaquetados_cabecera ec ON ec.id_cabecera = ed.id_cabecera
    JOIN (
      SELECT DISTINCT ON (id_destino) *
      FROM destinos
      ORDER BY id_destino
    ) d ON d.id_destino = ec.id_destino
    JOIN (
      SELECT DISTINCT ON (id_producto) *
      FROM productos
      ORDER BY id_producto
    ) p ON p.id_producto = ed.id_producto
    WHERE ec.fecha_hora::date BETWEEN $1::date AND $2::date
      AND TRIM(COALESCE(ed.numero_lote, '')) <> ''
      AND REGEXP_REPLACE(UPPER(TRIM(COALESCE(d.nombre, ''))), '[^A-Z0-9]+', '', 'g') <> 'KFOOD'
    GROUP BY ec.id_cabecera, ec.fecha_hora, d.nombre, ed.id_producto, p.codigo_producto
  ),
  candidatos AS (
    SELECT
      id_cabecera,
      codigo_lote,
      MAX(fecha_hora) AS fecha_hora,
      MAX(destino) AS destino,
      JSONB_AGG(
        JSONB_BUILD_OBJECT(
          'id_producto', id_producto,
          'codigo_producto', UPPER(TRIM(codigo_producto)),
          'cantidad_validada', cantidad_validada
        )
        ORDER BY UPPER(TRIM(codigo_producto)), id_producto
      ) AS resumen_validacion
    FROM detalle_agregado
    GROUP BY id_cabecera, codigo_lote
  )
`;

const client = await pool.connect();
try {
  await client.query('BEGIN');

  const preview = await client.query(
    `${candidatesSql}
     SELECT
       c.codigo_lote,
       TO_CHAR(c.fecha_hora, 'YYYY-MM-DD HH24:MI') AS fecha_hora,
       c.destino,
       COALESCE(alp.estado, 'pendiente') AS estado_anterior,
       JSONB_ARRAY_LENGTH(c.resumen_validacion) AS productos
     FROM candidatos c
     LEFT JOIN almacen_lotes_procesados alp ON UPPER(TRIM(alp.codigo_lote)) = UPPER(TRIM(c.codigo_lote))
     ORDER BY c.fecha_hora, c.codigo_lote`,
    [fromDate, toDate]
  );

  const excluded = await client.query(
    `SELECT COUNT(DISTINCT ec.id_cabecera)::int AS total
       FROM empaquetados_cabecera ec
       JOIN destinos d ON d.id_destino = ec.id_destino
      WHERE ec.fecha_hora::date BETWEEN $1::date AND $2::date
        AND REGEXP_REPLACE(UPPER(TRIM(COALESCE(d.nombre, ''))), '[^A-Z0-9]+', '', 'g') = 'KFOOD'`,
    [fromDate, toDate]
  );

  if (!apply) {
    await client.query('ROLLBACK');
    console.log(JSON.stringify({
      modo: 'vista_previa',
      desde: fromDate,
      hasta: toDate,
      candidatos: preview.rowCount,
      kfood_excluidos: excluded.rows[0]?.total || 0,
      filas: preview.rows,
      aplicar_con: `npm run approve:almacen09-range -- ${fromDate} ${toDate} --apply`,
    }, null, 2));
  } else {
    const result = await client.query(
      `${candidatesSql}
       INSERT INTO almacen_lotes_procesados (codigo_lote, estado, processed_at, resumen_validacion)
       SELECT codigo_lote, 'validado', NOW(), resumen_validacion
       FROM candidatos
       ON CONFLICT (codigo_lote) DO UPDATE
         SET estado = 'validado',
             processed_at = CASE
               WHEN almacen_lotes_procesados.estado = 'validado'
                 THEN almacen_lotes_procesados.processed_at
               ELSE NOW()
             END,
             resumen_validacion = EXCLUDED.resumen_validacion
       RETURNING codigo_lote, estado, processed_at`,
      [fromDate, toDate]
    );
    await client.query('COMMIT');
    console.log(JSON.stringify({
      modo: 'aplicado',
      desde: fromDate,
      hasta: toDate,
      aprobados: result.rowCount,
      kfood_excluidos: excluded.rows[0]?.total || 0,
      filas: result.rows,
    }, null, 2));
  }
} catch (error) {
  await client.query('ROLLBACK').catch(() => {});
  console.error(error);
  process.exitCode = 1;
} finally {
  client.release();
  await pool.end();
}
