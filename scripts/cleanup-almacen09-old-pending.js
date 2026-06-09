import dotenv from 'dotenv';
import { Pool } from 'pg';

dotenv.config();

const targetDate = String(process.argv[2] || '').trim();

if (!/^\d{4}-\d{2}-\d{2}$/.test(targetDate)) {
  console.error('Uso: npm run cleanup:almacen09-old-pending -- YYYY-MM-DD');
  process.exit(1);
}

if (!process.env.DATABASE_URL) {
  console.error('DATABASE_URL es obligatorio.');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const startDate = String(process.env.ALMACEN09_ENTRADAS_START_DATE || '2026-06-09').trim();
const visibleDays = Math.max(0, Number(process.env.ALMACEN09_ENTRADAS_VISIBLE_DAYS || 2) || 2);

const pendingCte = `
  WITH detalle_agregado AS (
    SELECT
      CONCAT('CAB-', ec.id_cabecera) AS codigo_lote,
      MAX(ec.fecha_hora)::date AS fecha
    FROM empaquetados_detalle ed
    JOIN empaquetados_cabecera ec ON ec.id_cabecera = ed.id_cabecera
    JOIN destinos d ON d.id_destino = ec.id_destino
    WHERE TRIM(COALESCE(ed.numero_lote, '')) <> ''
      AND UPPER(TRIM(COALESCE(d.nombre, ''))) <> 'K FOOD'
    GROUP BY ec.id_cabecera
  ),
  ventana AS (
    SELECT GREATEST($2::date, $1::date - $3::int) AS desde
  )
`;

try {
  const before = await pool.query(
    `${pendingCte}
     SELECT COUNT(*)::int AS old_pending
     FROM detalle_agregado da
     CROSS JOIN ventana v
     LEFT JOIN almacen_lotes_procesados alp
       ON UPPER(TRIM(SPLIT_PART(alp.codigo_lote, '::', 1))) = UPPER(TRIM(da.codigo_lote))
     WHERE alp.codigo_lote IS NULL
       AND da.fecha < v.desde`,
    [targetDate, startDate, visibleDays]
  );

  const result = await pool.query(
    `${pendingCte},
     old_pending AS (
       SELECT da.codigo_lote
       FROM detalle_agregado da
       CROSS JOIN ventana v
       LEFT JOIN almacen_lotes_procesados alp
         ON UPPER(TRIM(SPLIT_PART(alp.codigo_lote, '::', 1))) = UPPER(TRIM(da.codigo_lote))
       WHERE alp.codigo_lote IS NULL
         AND da.fecha < v.desde
     )
     INSERT INTO almacen_lotes_procesados (codigo_lote, estado, processed_at)
     SELECT codigo_lote, 'descartado', NOW()
     FROM old_pending
     ON CONFLICT (codigo_lote)
     DO UPDATE SET estado = 'descartado', processed_at = NOW()
     RETURNING codigo_lote`,
    [targetDate, startDate, visibleDays]
  );

  const after = await pool.query(
    `${pendingCte}
     SELECT COUNT(*)::int AS pending_window
     FROM detalle_agregado da
     CROSS JOIN ventana v
     LEFT JOIN almacen_lotes_procesados alp
       ON UPPER(TRIM(SPLIT_PART(alp.codigo_lote, '::', 1))) = UPPER(TRIM(da.codigo_lote))
     WHERE alp.codigo_lote IS NULL
       AND da.fecha BETWEEN v.desde AND $1::date`,
    [targetDate, startDate, visibleDays]
  );

  console.log(JSON.stringify({
    fecha_objetivo: targetDate,
    fecha_arranque: startDate,
    dias_visibles: visibleDays,
    pendientes_viejos_antes: before.rows[0]?.old_pending || 0,
    marcados_descartado: result.rowCount || 0,
    pendientes_en_ventana: after.rows[0]?.pending_window || 0,
  }, null, 2));
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
} finally {
  await pool.end();
}
