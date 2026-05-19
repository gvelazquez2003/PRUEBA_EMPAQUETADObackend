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
  )
`;

try {
  const before = await pool.query(
    `${pendingCte}
     SELECT COUNT(*)::int AS old_pending
     FROM detalle_agregado da
     LEFT JOIN almacen_lotes_procesados alp ON alp.codigo_lote = da.codigo_lote
     WHERE alp.codigo_lote IS NULL
       AND da.fecha <> $1::date`,
    [targetDate]
  );

  const result = await pool.query(
    `${pendingCte},
     old_pending AS (
       SELECT da.codigo_lote
       FROM detalle_agregado da
       LEFT JOIN almacen_lotes_procesados alp ON alp.codigo_lote = da.codigo_lote
       WHERE alp.codigo_lote IS NULL
         AND da.fecha <> $1::date
     )
     INSERT INTO almacen_lotes_procesados (codigo_lote, estado, processed_at)
     SELECT codigo_lote, 'descartado', NOW()
     FROM old_pending
     ON CONFLICT (codigo_lote)
     DO UPDATE SET estado = 'descartado', processed_at = NOW()
     RETURNING codigo_lote`,
    [targetDate]
  );

  const after = await pool.query(
    `${pendingCte}
     SELECT COUNT(*)::int AS today_pending
     FROM detalle_agregado da
     LEFT JOIN almacen_lotes_procesados alp ON alp.codigo_lote = da.codigo_lote
     WHERE alp.codigo_lote IS NULL
       AND da.fecha = $1::date`,
    [targetDate]
  );

  console.log(JSON.stringify({
    fecha_objetivo: targetDate,
    pendientes_viejos_antes: before.rows[0]?.old_pending || 0,
    marcados_descartado: result.rowCount || 0,
    pendientes_hoy: after.rows[0]?.today_pending || 0,
  }, null, 2));
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
} finally {
  await pool.end();
}
