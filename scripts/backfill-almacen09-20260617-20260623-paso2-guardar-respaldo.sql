INSERT INTO backup_almacen09_despacho_20260617_20260623 (
  codigo_lote,
  existia_antes,
  estado_anterior,
  processed_at_anterior,
  resumen_validacion_anterior
)
SELECT
  CONCAT('CAB-', ec.id_cabecera),
  alp.codigo_lote IS NOT NULL,
  alp.estado,
  alp.processed_at,
  alp.resumen_validacion
FROM empaquetados_cabecera ec
JOIN (
  SELECT DISTINCT ON (id_destino) *
  FROM destinos
  ORDER BY id_destino
) d ON d.id_destino = ec.id_destino
JOIN (
  SELECT DISTINCT id_cabecera
  FROM empaquetados_detalle
) ed ON ed.id_cabecera = ec.id_cabecera
LEFT JOIN almacen_lotes_procesados alp
  ON UPPER(TRIM(alp.codigo_lote)) = UPPER(TRIM(CONCAT('CAB-', ec.id_cabecera)))
WHERE ec.fecha_hora >= TIMESTAMP '2026-06-17 00:00:00'
  AND ec.fecha_hora < TIMESTAMP '2026-06-24 00:00:00'
  AND UPPER(TRIM(COALESCE(d.nombre, ''))) = 'DESPACHO'
ON CONFLICT (codigo_lote) DO NOTHING
RETURNING codigo_lote, existia_antes, estado_anterior;

