-- Completa las entradas de Almacen09 para registros entregados a DESPACHO.
-- La fecha de almacen visible queda exactamente una hora despues de la fecha de empaquetado.
-- No sobrescribe registros que ya esten validados.

BEGIN;

CREATE TEMP TABLE tmp_almacen09_despacho_detalles ON COMMIT DROP AS
SELECT
  ec.id_cabecera,
  CONCAT('CAB-', ec.id_cabecera) AS codigo_lote,
  ec.fecha_hora AS fecha_empaquetado,
  ed.id_producto,
  UPPER(TRIM(p.codigo_producto)) AS codigo_producto,
  UPPER(TRIM(ed.numero_lote)) AS numero_lote,
  ed.cantidad
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
WHERE ec.fecha_hora >= TIMESTAMP '2026-06-17 00:00:00'
  AND ec.fecha_hora < TIMESTAMP '2026-06-24 00:00:00'
  AND UPPER(TRIM(COALESCE(d.nombre, ''))) = 'DESPACHO';

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM tmp_almacen09_despacho_detalles
    WHERE COALESCE(numero_lote, '') = ''
  ) THEN
    RAISE EXCEPTION 'Hay productos sin lote dentro del rango. No se aplico ningun cambio.';
  END IF;
END $$;

CREATE TEMP TABLE tmp_almacen09_despacho_candidatos ON COMMIT DROP AS
WITH detalle_agrupado AS (
  SELECT
    id_cabecera,
    codigo_lote,
    MAX(fecha_empaquetado) AS fecha_empaquetado,
    id_producto,
    codigo_producto,
    numero_lote,
    SUM(cantidad)::int AS cantidad_validada
  FROM tmp_almacen09_despacho_detalles
  GROUP BY id_cabecera, codigo_lote, id_producto, codigo_producto, numero_lote
)
SELECT
  codigo_lote,
  -- processed_at se guarda como UTC sin zona; la interfaz lo convierte nuevamente a Caracas.
  ((MAX(fecha_empaquetado) + INTERVAL '1 hour') AT TIME ZONE 'America/Caracas' AT TIME ZONE 'UTC')::timestamp AS processed_at,
  JSONB_AGG(
    JSONB_BUILD_OBJECT(
      'id_producto', id_producto,
      'codigo_producto', codigo_producto,
      'cantidad_validada', cantidad_validada
    )
    ORDER BY id_producto, numero_lote
  ) AS resumen_validacion,
  SUM(cantidad_validada)::int AS cantidad_total
FROM detalle_agrupado
GROUP BY codigo_lote;

CREATE TABLE IF NOT EXISTS backup_almacen09_despacho_20260617_20260623 (
  codigo_lote VARCHAR(50) PRIMARY KEY,
  existia_antes BOOLEAN NOT NULL,
  estado_anterior VARCHAR(20),
  processed_at_anterior TIMESTAMP,
  resumen_validacion_anterior JSONB,
  backed_up_at TIMESTAMP NOT NULL DEFAULT NOW()
);

INSERT INTO backup_almacen09_despacho_20260617_20260623 (
  codigo_lote,
  existia_antes,
  estado_anterior,
  processed_at_anterior,
  resumen_validacion_anterior
)
SELECT
  c.codigo_lote,
  alp.codigo_lote IS NOT NULL,
  alp.estado,
  alp.processed_at,
  alp.resumen_validacion
FROM tmp_almacen09_despacho_candidatos c
LEFT JOIN almacen_lotes_procesados alp ON UPPER(TRIM(alp.codigo_lote)) = UPPER(TRIM(c.codigo_lote))
ON CONFLICT (codigo_lote) DO NOTHING;

CREATE TEMP TABLE tmp_almacen09_despacho_actualizados ON COMMIT DROP AS
WITH actualizados AS (
  INSERT INTO almacen_lotes_procesados (
    codigo_lote,
    estado,
    processed_at,
    resumen_validacion
  )
  SELECT
    codigo_lote,
    'validado',
    processed_at,
    resumen_validacion
  FROM tmp_almacen09_despacho_candidatos
  ON CONFLICT (codigo_lote) DO UPDATE
    SET estado = EXCLUDED.estado,
        processed_at = EXCLUDED.processed_at,
        resumen_validacion = EXCLUDED.resumen_validacion
    WHERE almacen_lotes_procesados.estado <> 'validado'
  RETURNING codigo_lote, processed_at, resumen_validacion
)
SELECT * FROM actualizados;

SELECT
  (SELECT COUNT(*) FROM tmp_almacen09_despacho_candidatos) AS registros_en_rango,
  (SELECT COUNT(*) FROM tmp_almacen09_despacho_actualizados) AS registros_actualizados,
  (SELECT COALESCE(SUM(cantidad_total), 0) FROM tmp_almacen09_despacho_candidatos) AS cantidad_total_en_rango,
  (SELECT COUNT(*)
     FROM tmp_almacen09_despacho_candidatos c
     JOIN almacen_lotes_procesados alp ON UPPER(TRIM(alp.codigo_lote)) = UPPER(TRIM(c.codigo_lote))
    WHERE alp.estado = 'validado') AS registros_validados_despues;

SELECT
  a.codigo_lote,
  TO_CHAR(a.processed_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Caracas', 'DD/MM/YYYY HH24:MI') AS fecha_almacen_visible,
  COALESCE(SUM((e.value->>'cantidad_validada')::int), 0)::int AS cantidad_almacen
FROM tmp_almacen09_despacho_actualizados a
LEFT JOIN LATERAL JSONB_ARRAY_ELEMENTS(COALESCE(a.resumen_validacion, '[]'::jsonb)) e(value) ON TRUE
GROUP BY a.codigo_lote, a.processed_at
ORDER BY a.processed_at, a.codigo_lote;

COMMIT;

