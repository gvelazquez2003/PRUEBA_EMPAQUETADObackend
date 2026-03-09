BEGIN;

DROP TABLE IF EXISTS conteo_errores;
DROP TABLE IF EXISTS almacen_lotes_procesados;

CREATE TABLE conteo_errores (
  id SERIAL PRIMARY KEY,
  codigo_lote VARCHAR(50),
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE almacen_lotes_procesados (
  codigo_lote VARCHAR(50) PRIMARY KEY,
  estado VARCHAR(20) NOT NULL DEFAULT 'validado',
  processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
  resumen_validacion JSONB
);

COMMIT;

SELECT
  (SELECT COUNT(*) FROM almacen_lotes_procesados) AS total_lotes_procesados,
  (SELECT COUNT(*) FROM conteo_errores) AS total_errores;