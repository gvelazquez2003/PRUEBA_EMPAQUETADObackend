BEGIN;

-- Limpia tablas legacy o no funcionales del flujo actual.
DROP TABLE IF EXISTS mermas_detalle CASCADE;
DROP TABLE IF EXISTS mermas_cabecera CASCADE;
DROP TABLE IF EXISTS lote_productos CASCADE;
DROP TABLE IF EXISTS lotes CASCADE;

ALTER TABLE IF EXISTS productos
  ADD COLUMN IF NOT EXISTS activo BOOLEAN NOT NULL DEFAULT TRUE;

CREATE TABLE IF NOT EXISTS conteo_errores (
  id SERIAL PRIMARY KEY,
  codigo_lote VARCHAR(50),
  lote_producto VARCHAR(120),
  codigo_producto VARCHAR(30),
  nombre_producto TEXT,
  cantidad_esperada INT,
  cantidad_recibida INT,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

ALTER TABLE IF EXISTS conteo_errores
  ADD COLUMN IF NOT EXISTS lote_producto VARCHAR(120);
ALTER TABLE IF EXISTS conteo_errores
  ADD COLUMN IF NOT EXISTS codigo_producto VARCHAR(30);
ALTER TABLE IF EXISTS conteo_errores
  ADD COLUMN IF NOT EXISTS nombre_producto TEXT;
ALTER TABLE IF EXISTS conteo_errores
  ADD COLUMN IF NOT EXISTS cantidad_esperada INT;
ALTER TABLE IF EXISTS conteo_errores
  ADD COLUMN IF NOT EXISTS cantidad_recibida INT;

CREATE TABLE IF NOT EXISTS almacen_lotes_procesados (
  codigo_lote VARCHAR(50) PRIMARY KEY,
  estado VARCHAR(20) NOT NULL DEFAULT 'validado',
  processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
  resumen_validacion JSONB
);

ALTER TABLE IF EXISTS almacen_lotes_procesados
  ADD COLUMN IF NOT EXISTS estado VARCHAR(20) NOT NULL DEFAULT 'validado';
ALTER TABLE IF EXISTS almacen_lotes_procesados
  ADD COLUMN IF NOT EXISTS processed_at TIMESTAMP NOT NULL DEFAULT NOW();
ALTER TABLE IF EXISTS almacen_lotes_procesados
  ADD COLUMN IF NOT EXISTS resumen_validacion JSONB;

CREATE TABLE IF NOT EXISTS historico_resultados_consolidado (
  id_historico BIGSERIAL PRIMARY KEY,
  fecha DATE,
  fecha_empaquetado TIMESTAMP,
  fecha_almacen09 TIMESTAMP,
  codigo_producto VARCHAR(30),
  producto TEXT NOT NULL,
  cantidad INTEGER,
  entregado_a VARCHAR(120),
  numero_registro VARCHAR(50),
  responsable VARCHAR(120),
  sede VARCHAR(160),
  numero_lote VARCHAR(80),
  source_hash VARCHAR(64) UNIQUE NOT NULL,
  origen_historico VARCHAR(20) NOT NULL DEFAULT 'csv',
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS auth_users (
  id_user SERIAL PRIMARY KEY,
  username VARCHAR(10) NOT NULL UNIQUE,
  role VARCHAR(20) NOT NULL CHECK (role IN ('administrador', 'empaquetado', 'almacen')),
  password_hash TEXT NOT NULL,
  activo BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS auth_sessions (
  token VARCHAR(128) PRIMARY KEY,
  id_user INT NOT NULL REFERENCES auth_users(id_user) ON DELETE CASCADE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
  expires_at TIMESTAMP NULL,
  revoked_at TIMESTAMP NULL,
  user_agent TEXT,
  ip_address VARCHAR(80)
);

CREATE TABLE IF NOT EXISTS control_inventario_guardia (
  id_control BIGSERIAL PRIMARY KEY,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  almacenista VARCHAR(120) NOT NULL,
  turno_actual VARCHAR(120) NOT NULL,
  momento_conteo VARCHAR(180) NOT NULL,
  id_producto INT NOT NULL REFERENCES productos(id_producto),
  cantidad_fisica_contada INT NOT NULL,
  fecha_elaboracion DATE NOT NULL,
  almacen VARCHAR(20) NOT NULL
);

CREATE TABLE IF NOT EXISTS almacen09_clientes (
  id_cliente BIGSERIAL PRIMARY KEY,
  nombre VARCHAR(160) NOT NULL UNIQUE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS almacen09_vendedores (
  id_vendedor BIGSERIAL PRIMARY KEY,
  nombre VARCHAR(160) NOT NULL UNIQUE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS almacen09_zonas (
  id_zona BIGSERIAL PRIMARY KEY,
  nombre VARCHAR(120) NOT NULL UNIQUE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS almacen09_sucursales (
  id_sucursal BIGSERIAL PRIMARY KEY,
  nombre VARCHAR(160) NOT NULL UNIQUE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS almacen09_direcciones (
  id_direccion BIGSERIAL PRIMARY KEY,
  direccion VARCHAR(240) NOT NULL UNIQUE,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS almacen09_salidas_facturas (
  id_factura BIGSERIAL PRIMARY KEY,
  numero_control BIGINT NOT NULL UNIQUE,
  numero_factura VARCHAR(80) NOT NULL UNIQUE,
  fecha_emision TIMESTAMP NOT NULL,
  cliente_id BIGINT REFERENCES almacen09_clientes(id_cliente) ON DELETE SET NULL,
  cliente_nombre VARCHAR(160),
  vendedor_id BIGINT REFERENCES almacen09_vendedores(id_vendedor) ON DELETE SET NULL,
  vendedor_nombre VARCHAR(160),
  zona_id BIGINT REFERENCES almacen09_zonas(id_zona) ON DELETE SET NULL,
  zona_nombre VARCHAR(120),
  sucursal_id BIGINT REFERENCES almacen09_sucursales(id_sucursal) ON DELETE SET NULL,
  sucursal_nombre VARCHAR(160),
  direccion_id BIGINT REFERENCES almacen09_direcciones(id_direccion) ON DELETE SET NULL,
  direccion_texto VARCHAR(240),
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  estado VARCHAR(20) NOT NULL DEFAULT 'emitida'
);

CREATE TABLE IF NOT EXISTS almacen09_salidas_detalle (
  id_detalle BIGSERIAL PRIMARY KEY,
  id_factura BIGINT NOT NULL REFERENCES almacen09_salidas_facturas(id_factura) ON DELETE CASCADE,
  id_producto INT NOT NULL REFERENCES productos(id_producto),
  codigo_producto VARCHAR(30) NOT NULL,
  producto TEXT NOT NULL,
  numero_lote VARCHAR(80) NOT NULL,
  maquina VARCHAR(10),
  cantidad INT NOT NULL CHECK (cantidad > 0),
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_auth_users_role ON auth_users(role);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(id_user);
CREATE INDEX IF NOT EXISTS idx_auth_sessions_revoked ON auth_sessions(revoked_at);

CREATE INDEX IF NOT EXISTS idx_productos_activo_codigo ON productos(activo, codigo_producto);
CREATE INDEX IF NOT EXISTS idx_productos_codigo_upper ON productos(UPPER(TRIM(codigo_producto)));

CREATE INDEX IF NOT EXISTS idx_empaquetados_cabecera_fecha_hora ON empaquetados_cabecera(fecha_hora DESC);
CREATE INDEX IF NOT EXISTS idx_empaquetados_cabecera_numero_registro ON empaquetados_cabecera(numero_registro);
CREATE INDEX IF NOT EXISTS idx_empaquetados_detalle_cabecera ON empaquetados_detalle(id_cabecera);
CREATE INDEX IF NOT EXISTS idx_empaquetados_detalle_producto ON empaquetados_detalle(id_producto);
CREATE INDEX IF NOT EXISTS idx_empaquetados_detalle_lote_upper ON empaquetados_detalle(UPPER(TRIM(numero_lote)));

CREATE INDEX IF NOT EXISTS idx_conteo_errores_created_at ON conteo_errores(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_conteo_errores_codigo_lote ON conteo_errores(codigo_lote);
CREATE INDEX IF NOT EXISTS idx_almacen_lotes_procesados_estado_processed ON almacen_lotes_procesados(estado, processed_at DESC);

CREATE INDEX IF NOT EXISTS idx_historico_resultados_fecha_empaquetado ON historico_resultados_consolidado(fecha_empaquetado DESC);
CREATE INDEX IF NOT EXISTS idx_historico_resultados_fecha_almacen09 ON historico_resultados_consolidado(fecha_almacen09 DESC);

CREATE INDEX IF NOT EXISTS idx_control_inventario_guardia_created_at ON control_inventario_guardia(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_control_inventario_guardia_producto_fecha ON control_inventario_guardia(id_producto, fecha_elaboracion DESC);

CREATE INDEX IF NOT EXISTS idx_salidas09_facturas_fecha ON almacen09_salidas_facturas(fecha_emision DESC);
CREATE INDEX IF NOT EXISTS idx_salidas09_detalle_codigo_lote ON almacen09_salidas_detalle(codigo_producto, numero_lote);
CREATE INDEX IF NOT EXISTS idx_salidas09_detalle_factura ON almacen09_salidas_detalle(id_factura);

COMMIT;

-- Inventario final de tablas clave para compartir.
SELECT
  t.tablename AS table_name,
  pg_size_pretty(pg_total_relation_size(quote_ident(t.schemaname) || '.' || quote_ident(t.tablename))) AS total_size
FROM pg_tables t
WHERE t.schemaname = 'public'
  AND t.tablename IN (
    'destinos',
    'sedes',
    'responsables',
    'productos',
    'empaquetados_cabecera',
    'empaquetados_detalle',
    'conteo_errores',
    'almacen_lotes_procesados',
    'historico_resultados_consolidado',
    'control_inventario_guardia',
    'almacen09_clientes',
    'almacen09_vendedores',
    'almacen09_zonas',
    'almacen09_sucursales',
    'almacen09_direcciones',
    'almacen09_salidas_facturas',
    'almacen09_salidas_detalle',
    'auth_users',
    'auth_sessions'
  )
ORDER BY t.tablename;
