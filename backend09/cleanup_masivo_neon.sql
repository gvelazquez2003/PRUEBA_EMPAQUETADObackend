BEGIN;

WITH target_cab AS (
  SELECT id_cabecera
  FROM empaquetados_cabecera
  WHERE numero_registro LIKE 'MASS26-%'
), deleted_errores AS (
  DELETE FROM conteo_errores
  WHERE codigo_lote IN (SELECT 'CAB-' || id_cabecera::text FROM target_cab)
     OR codigo_lote LIKE 'MASS-26%'
  RETURNING 1
), deleted_alm AS (
  DELETE FROM almacen_lotes_procesados
  WHERE codigo_lote IN (SELECT 'CAB-' || id_cabecera::text FROM target_cab)
     OR codigo_lote LIKE 'MASS-26%'
  RETURNING 1
), deleted_det AS (
  DELETE FROM empaquetados_detalle
  WHERE id_cabecera IN (SELECT id_cabecera FROM target_cab)
  RETURNING 1
)
DELETE FROM empaquetados_cabecera
WHERE id_cabecera IN (SELECT id_cabecera FROM target_cab);

COMMIT;

SELECT
  (SELECT COUNT(*) FROM empaquetados_cabecera WHERE numero_registro LIKE 'MASS26-%') AS empaquetados_cabecera_masivo,
  (SELECT COUNT(*) FROM empaquetados_detalle ed JOIN empaquetados_cabecera ec ON ec.id_cabecera = ed.id_cabecera WHERE ec.numero_registro LIKE 'MASS26-%') AS empaquetados_detalle_masivo,
  (SELECT COUNT(*) FROM almacen_lotes_procesados alp WHERE alp.codigo_lote IN (SELECT 'CAB-' || ec.id_cabecera::text FROM empaquetados_cabecera ec WHERE ec.numero_registro LIKE 'MASS26-%')) AS lotes_procesados_masivo,
  (SELECT COUNT(*) FROM conteo_errores WHERE codigo_lote IN (SELECT 'CAB-' || ec.id_cabecera::text FROM empaquetados_cabecera ec WHERE ec.numero_registro LIKE 'MASS26-%')) AS errores_masivos;
