BEGIN;

DROP TABLE IF EXISTS conteo_errores;
DROP TABLE IF EXISTS almacen_lotes_procesados;

CREATE TABLE conteo_errores (
  id SERIAL PRIMARY KEY,
  codigo_lote VARCHAR(50),
  lote_producto VARCHAR(120),
  codigo_producto VARCHAR(30),
  nombre_producto TEXT,
  cantidad_esperada INT,
  cantidad_recibida INT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE almacen_lotes_procesados (
  codigo_lote VARCHAR(50) PRIMARY KEY,
  estado VARCHAR(20) NOT NULL DEFAULT 'validado',
  processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
  resumen_validacion JSONB
);

CREATE INDEX idx_conteo_errores_created_at
ON conteo_errores (created_at DESC);

CREATE INDEX idx_conteo_errores_codigo_lote
ON conteo_errores (codigo_lote);

CREATE INDEX idx_almacen_lotes_procesados_estado_processed
ON almacen_lotes_procesados (estado, processed_at DESC);

COMMIT;

SELECT
  (SELECT COUNT(*) FROM almacen_lotes_procesados) AS total_lotes_procesados,
  (SELECT COUNT(*) FROM conteo_errores) AS total_errores;