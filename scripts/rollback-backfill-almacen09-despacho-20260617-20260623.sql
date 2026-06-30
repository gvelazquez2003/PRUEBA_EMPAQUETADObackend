-- Revierte solamente lo aplicado por backfill-almacen09-despacho-20260617-20260623.sql.

BEGIN;

DELETE FROM almacen_lotes_procesados alp
USING backup_almacen09_despacho_20260617_20260623 b
WHERE UPPER(TRIM(alp.codigo_lote)) = UPPER(TRIM(b.codigo_lote))
  AND b.existia_antes = FALSE;

UPDATE almacen_lotes_procesados alp
SET estado = b.estado_anterior,
    processed_at = b.processed_at_anterior,
    resumen_validacion = b.resumen_validacion_anterior
FROM backup_almacen09_despacho_20260617_20260623 b
WHERE UPPER(TRIM(alp.codigo_lote)) = UPPER(TRIM(b.codigo_lote))
  AND b.existia_antes = TRUE;

COMMIT;

