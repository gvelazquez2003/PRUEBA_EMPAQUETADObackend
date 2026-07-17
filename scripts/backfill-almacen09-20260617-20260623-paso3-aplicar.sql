WITH detalle_agrupado AS (
  SELECT
    CONCAT('CAB-', ec.id_cabecera) AS codigo_lote,
    ec.fecha_hora AS fecha_empaquetado,
    ed.id_producto,
    UPPER(TRIM(p.codigo_producto)) AS codigo_producto,
    UPPER(TRIM(ed.numero_lote)) AS numero_lote,
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
  WHERE ec.fecha_hora >= TIMESTAMP '2026-06-17 00:00:00'
    AND ec.fecha_hora < TIMESTAMP '2026-06-24 00:00:00'
    AND UPPER(TRIM(COALESCE(d.nombre, ''))) = 'DESPACHO'
    AND TRIM(COALESCE(ed.numero_lote, '')) <> ''
  GROUP BY
    ec.id_cabecera,
    ec.fecha_hora,
    ed.id_producto,
    UPPER(TRIM(p.codigo_producto)),
    UPPER(TRIM(ed.numero_lote))
),
candidatos AS (
  SELECT
    codigo_lote,
    ((MAX(fecha_empaquetado) + INTERVAL '1 hour') AT TIME ZONE 'America/Caracas' AT TIME ZONE 'UTC')::timestamp AS processed_at,
    JSONB_AGG(
      JSONB_BUILD_OBJECT(
        'id_producto', id_producto,
        'codigo_producto', codigo_producto,
        'cantidad_validada', cantidad_validada
      )
      ORDER BY id_producto, numero_lote
    ) AS resumen_validacion
  FROM detalle_agrupado
  GROUP BY codigo_lote
)
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
FROM candidatos
ON CONFLICT (codigo_lote) DO UPDATE
SET estado = EXCLUDED.estado,
    processed_at = EXCLUDED.processed_at,
    resumen_validacion = EXCLUDED.resumen_validacion
WHERE almacen_lotes_procesados.estado <> 'validado'
RETURNING
  codigo_lote,
  TO_CHAR(processed_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Caracas', 'DD/MM/YYYY HH24:MI') AS fecha_almacen;

