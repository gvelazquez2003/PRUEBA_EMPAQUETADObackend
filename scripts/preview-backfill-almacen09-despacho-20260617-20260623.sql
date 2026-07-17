-- Vista previa sin modificar datos.
-- Rango de fecha de empaquetado: 17/06/2026 00:00 hasta 23/06/2026 23:59:59.
-- Destino: DESPACHO.

WITH detalles AS (
  SELECT
    ec.id_cabecera,
    CONCAT('CAB-', ec.id_cabecera) AS codigo_lote,
    ec.fecha_hora AS fecha_empaquetado,
    ec.numero_registro,
    p.codigo_producto,
    p.descripcion AS producto,
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
    AND UPPER(TRIM(COALESCE(d.nombre, ''))) = 'DESPACHO'
),
candidatos AS (
  SELECT
    id_cabecera,
    codigo_lote,
    MAX(fecha_empaquetado) AS fecha_empaquetado,
    MAX(numero_registro) AS numero_registro,
    SUM(cantidad)::int AS cantidad_total,
    COUNT(*) FILTER (WHERE COALESCE(numero_lote, '') = '')::int AS lineas_sin_lote,
    STRING_AGG(DISTINCT codigo_producto, ', ' ORDER BY codigo_producto) AS codigos_producto
  FROM detalles
  GROUP BY id_cabecera, codigo_lote
)
SELECT
  c.codigo_lote,
  c.numero_registro,
  TO_CHAR(c.fecha_empaquetado, 'DD/MM/YYYY HH24:MI') AS fecha_empaquetado,
  TO_CHAR(c.fecha_empaquetado + INTERVAL '1 hour', 'DD/MM/YYYY HH24:MI') AS fecha_almacen_propuesta,
  c.cantidad_total,
  c.lineas_sin_lote,
  c.codigos_producto,
  COALESCE(alp.estado, 'PENDIENTE') AS estado_actual,
  CASE
    WHEN alp.processed_at IS NULL THEN NULL
    ELSE TO_CHAR(alp.processed_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Caracas', 'DD/MM/YYYY HH24:MI')
  END AS fecha_almacen_actual,
  CASE WHEN alp.estado = 'validado' THEN 'SE CONSERVA' ELSE 'SE ACTUALIZA' END AS accion
FROM candidatos c
LEFT JOIN almacen_lotes_procesados alp ON UPPER(TRIM(alp.codigo_lote)) = UPPER(TRIM(c.codigo_lote))
ORDER BY c.fecha_empaquetado, c.codigo_lote;

