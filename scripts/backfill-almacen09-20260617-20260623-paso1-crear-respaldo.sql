CREATE TABLE IF NOT EXISTS backup_almacen09_despacho_20260617_20260623 (
  codigo_lote VARCHAR(50) PRIMARY KEY,
  existia_antes BOOLEAN NOT NULL,
  estado_anterior VARCHAR(20),
  processed_at_anterior TIMESTAMP,
  resumen_validacion_anterior JSONB,
  backed_up_at TIMESTAMP NOT NULL DEFAULT NOW()
);

