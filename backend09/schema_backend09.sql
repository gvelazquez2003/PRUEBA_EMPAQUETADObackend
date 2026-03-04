CREATE TABLE IF NOT EXISTS conteo_errores (
  id SERIAL PRIMARY KEY,
  codigo_lote VARCHAR(50),
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS almacen_lotes_procesados (
  codigo_lote VARCHAR(50) PRIMARY KEY,
  estado VARCHAR(20) NOT NULL DEFAULT 'validado',
  processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS almacen_validaciones_detalle (
  id SERIAL PRIMARY KEY,
  codigo_lote VARCHAR(50) NOT NULL REFERENCES almacen_lotes_procesados(codigo_lote) ON DELETE CASCADE,
  id_producto INTEGER NOT NULL,
  codigo_producto VARCHAR(50) NOT NULL,
  cantidad_esperada INTEGER NOT NULL,
  cantidad_contada INTEGER NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);
