DROP TABLE IF EXISTS auth_sessions;
DROP TABLE IF EXISTS auth_users;
DROP TABLE IF EXISTS almacen09_salidas_detalle;
DROP TABLE IF EXISTS almacen09_salidas_facturas;
DROP TABLE IF EXISTS almacen09_direcciones;
DROP TABLE IF EXISTS almacen09_sucursales;
DROP TABLE IF EXISTS almacen09_zonas;
DROP TABLE IF EXISTS almacen09_vendedores;
DROP TABLE IF EXISTS almacen09_clientes;
DROP TABLE IF EXISTS control_inventario_guardia;
DROP TABLE IF EXISTS historico_resultados_consolidado;
DROP TABLE IF EXISTS conteo_errores;
DROP TABLE IF EXISTS almacen_lotes_procesados;
DROP TABLE IF EXISTS lote_productos;
DROP TABLE IF EXISTS lotes;
DROP TABLE IF EXISTS mermas_detalle;
DROP TABLE IF EXISTS mermas_cabecera;
DROP TABLE IF EXISTS empaquetados_detalle;
DROP TABLE IF EXISTS empaquetados_cabecera;
DROP TABLE IF EXISTS productos;
DROP TABLE IF EXISTS responsables;
DROP TABLE IF EXISTS destinos;
DROP TABLE IF EXISTS sedes;

CREATE TABLE destinos (
    id_destino SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE sedes (
    id_sede SERIAL PRIMARY KEY,
    nombre VARCHAR(100) NOT NULL
);

CREATE TABLE responsables (
    id_responsable SERIAL PRIMARY KEY,
    nombre_completo VARCHAR(100) NOT NULL
);

CREATE TABLE productos (
    id_producto SERIAL PRIMARY KEY,
    codigo_producto VARCHAR(20) UNIQUE NOT NULL,
    descripcion VARCHAR(200) NOT NULL,
    unidad_primaria VARCHAR(50) NOT NULL,
    paquetes INT DEFAULT 0,
    cestas INT DEFAULT 0,
    sobre_piso INT DEFAULT 0,
    activo BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE empaquetados_cabecera (
    id_cabecera SERIAL PRIMARY KEY,
    fecha_hora TIMESTAMP NOT NULL,
    id_destino INT REFERENCES destinos(id_destino),
    numero_registro VARCHAR(50),
    id_responsable INT REFERENCES responsables(id_responsable),
    id_sede INT REFERENCES sedes(id_sede)
);

CREATE TABLE empaquetados_detalle (
    id_detalle SERIAL PRIMARY KEY,
    id_cabecera INT REFERENCES empaquetados_cabecera(id_cabecera) ON DELETE CASCADE,
    id_producto INT REFERENCES productos(id_producto),
    cantidad INT NOT NULL,
    numero_lote VARCHAR(50) NOT NULL
);

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

CREATE TABLE historico_resultados_consolidado (
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

CREATE TABLE control_inventario_guardia (
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

CREATE TABLE almacen09_clientes (
    id_cliente BIGSERIAL PRIMARY KEY,
    nombre VARCHAR(160) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE almacen09_vendedores (
    id_vendedor BIGSERIAL PRIMARY KEY,
    nombre VARCHAR(160) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE almacen09_zonas (
    id_zona BIGSERIAL PRIMARY KEY,
    nombre VARCHAR(120) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE almacen09_sucursales (
    id_sucursal BIGSERIAL PRIMARY KEY,
    nombre VARCHAR(160) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE almacen09_direcciones (
    id_direccion BIGSERIAL PRIMARY KEY,
    direccion VARCHAR(240) NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE almacen09_salidas_facturas (
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

CREATE TABLE almacen09_salidas_detalle (
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

CREATE TABLE auth_users (
    id_user SERIAL PRIMARY KEY,
    username VARCHAR(10) NOT NULL UNIQUE,
    role VARCHAR(20) NOT NULL CHECK (role IN ('administrador', 'empaquetado', 'almacen')),
    password_hash TEXT NOT NULL,
    activo BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE auth_sessions (
    token VARCHAR(128) PRIMARY KEY,
    id_user INT NOT NULL REFERENCES auth_users(id_user) ON DELETE CASCADE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP NULL,
    revoked_at TIMESTAMP NULL,
    user_agent TEXT,
    ip_address VARCHAR(80)
);

CREATE INDEX idx_auth_users_role ON auth_users(role);
CREATE INDEX idx_auth_sessions_user ON auth_sessions(id_user);
CREATE INDEX idx_auth_sessions_revoked ON auth_sessions(revoked_at);

CREATE INDEX idx_productos_activo_codigo ON productos(activo, codigo_producto);
CREATE INDEX idx_productos_codigo_upper ON productos(UPPER(TRIM(codigo_producto)));

CREATE INDEX idx_empaquetados_cabecera_fecha_hora ON empaquetados_cabecera(fecha_hora DESC);
CREATE INDEX idx_empaquetados_cabecera_numero_registro ON empaquetados_cabecera(numero_registro);
CREATE INDEX idx_empaquetados_detalle_cabecera ON empaquetados_detalle(id_cabecera);
CREATE INDEX idx_empaquetados_detalle_producto ON empaquetados_detalle(id_producto);
CREATE INDEX idx_empaquetados_detalle_lote_upper ON empaquetados_detalle(UPPER(TRIM(numero_lote)));

CREATE INDEX idx_conteo_errores_created_at ON conteo_errores(created_at DESC);
CREATE INDEX idx_conteo_errores_codigo_lote ON conteo_errores(codigo_lote);
CREATE INDEX idx_almacen_lotes_procesados_estado_processed ON almacen_lotes_procesados(estado, processed_at DESC);

CREATE INDEX idx_historico_resultados_fecha_empaquetado ON historico_resultados_consolidado(fecha_empaquetado DESC);
CREATE INDEX idx_historico_resultados_fecha_almacen09 ON historico_resultados_consolidado(fecha_almacen09 DESC);

CREATE INDEX idx_control_inventario_guardia_created_at ON control_inventario_guardia(created_at DESC);
CREATE INDEX idx_control_inventario_guardia_producto_fecha ON control_inventario_guardia(id_producto, fecha_elaboracion DESC);

CREATE INDEX idx_salidas09_facturas_fecha ON almacen09_salidas_facturas(fecha_emision DESC);
CREATE INDEX idx_salidas09_detalle_codigo_lote ON almacen09_salidas_detalle(codigo_producto, numero_lote);
CREATE INDEX idx_salidas09_detalle_factura ON almacen09_salidas_detalle(id_factura);

-- Insert data
INSERT INTO productos (codigo_producto, descripcion, unidad_primaria, paquetes, cestas, sobre_piso) VALUES
('PTEM0001', 'PAN DE HAMBURGUESA 85 GR 6 UND', 'PAQ', 10, 1, 1),
('PTEM0002', 'PAN DE PERRO 63 GR 8 UND', 'PAQ', 10, 1, 1),
('PTEM0003', 'PAN DE HAMBURGUESA MINI 30 GR 15 UND', 'PAQ', 10, 1, 1),
('PTEM0004', 'PAN TIPO DELI 110 GR 4 UND', 'PAQ', 10, 1, 1),
('PTEM0005', 'PAN CUADRADO PEQUEÑO 575 GR 17 UND', 'PAQ', 8, 1, 1),
('PTEM0006', 'PAN CUADRADO GRANDE 900 GR 21 UND', 'PAQ', 6, 1, 1),
('PTEM0007', 'PAN DULCE 30 GR 15 UND', 'PAQ', 10, 1, 1),
('PTEM0008', 'PAN MOLIDO 1 KG', 'PAQ', 12, 1, 1),
('PTEM0009', 'CROSTATAS 300 GR', 'PAQ', 12, 1, 1),
('PTEM0010', 'GALLETAS TATICAS 150 GR 1 UND', 'PAQ', 32, 1, 1),
('PTEM0011', 'PAN DE HAMBURGUESA ECON 60 GR 6 UND', 'PAQ', 10, 1, 1),
('PTEM0012', 'PAN DE HAMBURGUESA DE MANTEQUILLA 95 GR 6 UND', 'PAQ', 10, 1, 1),
('PTEM0013', 'PAN DE HAMBURGUESA DE MANTEQUILLA 50 GR 6 UND', 'PAQ', 10, 1, 1),
('PTEM0014', 'PAN DE HAMBURGUESA COLORES 95 GR 6 UND', 'PAQ', 10, 1, 1),
('PTEM0015', 'PAN DE HAMBURGUESA ESPECIAL 170 GR 2 UND WAB', 'PAQ', 6, 1, 1),
('PTEM0016', 'PAN DE HAMBURGUESA ESPECIAL 170 GR 2 UND WAN', 'PAQ', 6, 1, 1),
('PTEM0017', 'PAN DE HAMBURGUESA ESPECIAL 170 GR 2 UND WAM', 'PAQ', 6, 1, 1),
('PTEM0018', 'PAN DE HAMBURGUESA ESPECIAL 150 GR 4 UND WAB', 'PAQ', 5, 1, 1),
('PTEM0019', 'PAN DE HAMBURGUESA ESPECIAL 140 GR 4 UND WAN A18', 'PAQ', 5, 1, 1),
('PTEM0020', 'PAN DE HAMBURGUESA ESPECIAL 120 GR 4 UND WAB A18', 'PAQ', 5, 1, 1),
('PTEM0021', 'PAN DE HAMBURGUESA ESPECIAL 120 GR 4 UND WAN A18', 'PAQ', 5, 1, 1),
('PTEM0022', 'PAN DE HAMBURGUESA ESPECIAL 120 GR 4 UND WAM A18', 'PAQ', 5, 1, 1),
('PTEM0023', 'PAN DE HAMBURGUESA ESPECIAL 110 GR 4 UND WAB A21', 'PAQ', 6, 1, 1),
('PTEM0024', 'PAN DE HAMBURGUESA ESPECIAL 110 GR 4 UND WAN A18', 'PAQ', 6, 1, 1),
('PTEM0025', 'PAN DE HAMBURGUESA ESPECIAL 110 GR 4 UND WAN A21', 'PAQ', 6, 1, 1),
('PTEM0026', 'PAN DE HAMBURGUESA ESPECIAL 110 GR 4 UND WAM A21', 'PAQ', 6, 1, 1),
('PTEM0027', 'PAN DE HAMBURGUESA ESPECIAL 110 GR 4 UND A21', 'PAQ', 6, 1, 1),
('PTEM0028', 'PAN DE HAMBURGUESA ESPECIAL 100 GR 4 UND WAB A18', 'PAQ', 5, 1, 1),
('PTEM0029', 'PAN DE HAMBURGUESA ESPECIAL 100 GR 4 UND WAN A18', 'PAQ', 5, 1, 1),
('PTEM0030', 'PAN DE HAMBURGUESA ESPECIAL 100 GR 4 UND WAM A18', 'PAQ', 5, 1, 1),
('PTEM0031', 'PAN DE HAMBURGUESA ESPECIAL 100 GR 4 UND AB A18', 'PAQ', 5, 1, 1),
('PTEM0032', 'PAN DE HAMBURGUESA ESPECIAL 100 GR 4 UND W A18', 'PAQ', 5, 1, 1),
('PTEM0033', 'PAN DE HAMBURGUESA ESPECIAL 95 GR 6 UND AB A21', 'PAQ', 5, 1, 1),
('PTEM0034', 'PAN DE HAMBURGUESA ESPECIAL 95 GR 6 UND WAB A21', 'PAQ', 5, 1, 1),
('PTEM0035', 'PAN DE HAMBURGUESA ESPECIAL 95 GR 6 UND WAN A21', 'PAQ', 5, 1, 1),
('PTEM0036', 'PAN DE HAMBURGUESA ESPECIAL 95 GR 6 UND WAM A21', 'PAQ', 5, 1, 1),
('PTEM0037', 'PAN DE HAMBURGUESA ESPECIAL 95 GR 6 UND W A21', 'PAQ', 5, 1, 1),
('PTEM0038', 'PAN DE HAMBURGUESA ESPECIAL 95 GR 6 UND WAM A241', 'PAQ', 5, 1, 1),
('PTEM0039', 'PAN DE HAMBURGUESA ESPECIAL 85 GR 6 UND WAN A21', 'PAQ', 5, 1, 1),
('PTEM0040', 'PAN DE HAMBURGUESA ESPECIAL 85 GR 6 UND', 'PAQ', 5, 1, 1),
('PTEM0041', 'PAN DE HAMBURGUESA ESPECIAL 85 GR 6 UND AB A21', 'PAQ', 5, 1, 1),
('PTEM0042', 'PAN DE HAMBURGUESA ESPECIAL 85 GR 6 UND A21', 'PAQ', 5, 1, 1),
('PTEM0043', 'PAN DE HAMBURGUESA ESPECIAL 85 GR 6 UND A24', 'PAQ', 5, 1, 1),
('PTEM0044', 'PAN DE HAMBURGUESA ESPECIAL 85 GR 6 UND WAB A24', 'PAQ', 5, 1, 1),
('PTEM0045', 'PAN DE HAMBURGUESA ESPECIAL 85 GR 6 UND WAN A24', 'PAQ', 5, 1, 1),
('PTEM0046', 'PAN DE HAMBURGUESA ESPECIAL 85 GR 6 UND WAM A24', 'PAQ', 5, 1, 1),
('PTEM0047', 'PAN DE HAMBURGUESA ESPECIAL 85 GR 6 UND W A24', 'PAQ', 5, 1, 1),
('PTEM0048', 'PAN DE HAMBURGUESA ESPECIAL 75 GR 6 UND W A24', 'PAQ', 5, 1, 1),
('PTEM0049', 'PAN DE HAMBURGUESA ESPECIAL 75 GR 6 UND A24', 'PAQ', 5, 1, 1),
('PTEM0050', 'PAN DE HAMBURGUESA ESPECIAL 75 GR 6 UND WAB A24', 'PAQ', 5, 1, 1),
('PTEM0051', 'PAN DE HAMBURGUESA ESPECIAL 75 GR 6 UND WAN A24', 'PAQ', 5, 1, 1),
('PTEM0052', 'PAN DE HAMBURGUESA ESPECIAL 75 GR 6 UND WAM A24', 'PAQ', 5, 1, 1),
('PTEM0053', 'PAN DE HAMBURGUESA ESPECIAL 75 GR 6 UND AB A24', 'PAQ', 5, 1, 1),
('PTEM0054', 'PAN DE HAMBURGUESA ESPECIAL 75 GR 6 UND AN A24', 'PAQ', 5, 1, 1),
('PTEM0055', 'PAN DE HAMBURGUESA ESPECIAL 65 GR 6 UND WAM A24', 'PAQ', 5, 1, 1),
('PTEM0056', 'PAN DE HAMBURGUESA ESPECIAL 65 GR 6 UND WAN A24', 'PAQ', 5, 1, 1),
('PTEM0057', 'PAN DE HAMBURGUESA ESPECIAL 65 GR 6 UND AB A24', 'PAQ', 5, 1, 1),
('PTEM0058', 'PAN DE HAMBURGUESA ESPECIAL 65 GR 6 UND', 'PAQ', 5, 1, 1),
('PTEM0059', 'PAN DE HAMBURGUESA ESPECIAL 65 GR 6 UND AB', 'PAQ', 5, 1, 1),
('PTEM0060', 'PAN DE HAMBURGUESA ESPECIAL 55 GR 6 UND', 'PAQ', 10, 1, 1),
('PTEM0061', 'PAN DE HAMBURGUESA ESPECIAL 55 GR 6 UND AB', 'PAQ', 10, 1, 1),
('PTEM0062', 'PAN DE HAMBURGUESA ESPECIAL 45 GR 12 UND WAB A36', 'PAQ', 10, 1, 1),
('PTEM0063', 'PAN DE HAMBURGUESA MINI 40 GR 12 UND', 'PAQ', 10, 1, 1),
('PTEM0064', 'PAN DE HAMBURGUESA MINI ESPECIAL 40 GR 12 UND WAB', 'PAQ', 10, 1, 1),
('PTEM0065', 'PAN DE HAMBURGUESA MINI ESPECIAL 40 GR 12 UND WAN', 'PAQ', 10, 1, 1),
('PTEM0066', 'PAN DE HAMBURGUESA MINI ESPECIAL 40 GR 12 UND WAM', 'PAQ', 10, 1, 1),
('PTEM0067', 'PAN DE HAMBURGUESA MINI ESPECIAL 30 GR 15 UND', 'PAQ', 10, 1, 1),
('PTEM0068', 'PAN DE HAMBURGUESA MINI ESPECIAL 30 GR 15 UND WAN', 'PAQ', 10, 1, 1),
('PTEM0069', 'PAN DE PERRO 63 GR 8 UND W', 'PAQ', 10, 1, 1),
('PTEM0070', 'PAN DE PERRO 63 GR 8 UND WAB', 'PAQ', 10, 1, 1),
('PTEM0071', 'PAN DE PERRO 63 GR 8 UND WAN', 'PAQ', 10, 1, 1),
('PTEM0072', 'PAN DE PERRO MINI ESPECIAL 50 GR 12 UND', 'PAQ', 10, 1, 1),
('PTEM0073', 'PAN DE PERRO JUMBO CX 95 GR 6 UND', 'PAQ', 10, 1, 1),
('PTEM0074', 'PAN DE PERRO JUMBO ESPECIAL 85 GR 6 UND', 'PAQ', 10, 1, 1),
('PTEM0075', 'PAN DE PERRO JUMBO ESPECIAL 95 GR 5 UND', 'PAQ', 10, 1, 1),
('PTEM0076', 'PAN TIPO DELI 120 GR 4 UND PARMESANO A14', 'PAQ', 10, 1, 1),
('PTEM0077', 'PAN TIPO DELI ESPECIAL 120 GR 4 UND A14', 'PAQ', 10, 1, 1),
('PTEM0078', 'PAN TIPO DELI ESPECIAL 110 GR 4 UND WAB', 'PAQ', 10, 1, 1),
('PTEM0079', 'PAN TIPO DELI ESPECIAL 110 GR 4 UND WAN', 'PAQ', 10, 1, 1),
('PTEM0080', 'PAN TIPO DELI ESPECIAL 110 GR 4 UND WAM', 'PAQ', 10, 1, 1),
('PTEM0081', 'PAN GRANJERO ESPECIAL 75 GR 5 UND AB A19', 'PAQ', 10, 1, 1),
('PTEM0082', 'PAN DE HAMBURGUESA TIPO BRIOCHE 120 GR 4 UND WAB A18', 'PAQ', 5, 1, 1),
('PTEM0083', 'PAN DE HAMBURGUESA TIPO BRIOCHE 110 GR 4 UND WAB A241', 'PAQ', 6, 1, 1),
('PTEM0084', 'PAN DE HAMBURGUESA TIPO BRIOCHE 110 GR 4 UND WAM A241', 'PAQ', 6, 1, 1),
('PTEM0085', 'PAN DE HAMBURGUESA TIPO BRIOCHE 95 GR 6 UND WAN A21', 'PAQ', 5, 1, 1),
('PTEM0086', 'PAN DE HAMBURGUESA TIPO BRIOCHE 95 GR 6 UND WAB A21', 'PAQ', 5, 1, 1),
('PTEM0087', 'PAN DE HAMBURGUESA TIPO BRIOCHE 95 GR 6 UND WAM A21', 'PAQ', 5, 1, 1),
('PTEM0088', 'PAN DE HAMBURGUESA TIPO BRIOCHE 95 GR 6 UND WAN A24', 'PAQ', 5, 1, 1),
('PTEM0089', 'PAN DE HAMBURGUESA TIPO BRIOCHE 95 GR 6 UND WAB A241', 'PAQ', 5, 1, 1),
('PTEM0090', 'PAN DE HAMBURGUESA TIPO BRIOCHE 85 GR 6 UND WAB A24', 'PAQ', 5, 1, 1),
('PTEM0091', 'PAN DE HAMBURGUESA TIPO BRIOCHE 85 GR 6 UND WAN A24', 'PAQ', 5, 1, 1),
('PTEM0092', 'PAN DE HAMBURGUESA TIPO BRIOCHE 85 GR 6 UND WAM A24', 'PAQ', 5, 1, 1),
('PTEM0093', 'PAN DE HAMBURGUESA TIPO BRIOCHE 85 GR 6 UND WAN A24', 'PAQ', 5, 1, 1),
('PTEM0094', 'PAN DE HAMBURGUESA TIPO BRIOCHE 70 GR 6 UND WAB A24', 'PAQ', 5, 1, 1),
('PTEM0095', 'PAN DE HAMBURGUESA TIPO BRIOCHE 45 GR 12 UND WAB', 'PAQ', 10, 1, 1),
('PTEM0096', 'PAN DE HAMBURGUESA TIPO BRIOCHE 45 GR 12 UND WAN', 'PAQ', 10, 1, 1),
('PTEM0097', 'PAN DE PERRO TIPO BRIOCHE 70 GR 8 UND WAP', 'PAQ', 10, 1, 1),
('PTEM0098', 'PAN DE HAMBURGUESA BJ 120 GR 4 UND WAB A 241', 'PAQ', 5, 1, 1),
('PTEM0099', 'PAN DE HAMBURGUESA BJ 95 GR 6 UND WAB A 21', 'PAQ', 5, 1, 1),
('PTEM0100', 'PAN CUADRADO MEDIANO TIPO BRIOCHE 700 GR', 'PAQ', 8, 1, 1),
('PTEM0101', 'PAN DE HAMBURGUESA ESPECIAL 85 GR 6 UND WAM A18', 'PAQ', 5, 1, 1),
('PTEM0102', 'PAN DE HAMBURGUESA ESPECIAL 85 GR 6 UND AB', 'PAQ', 5, 1, 1),
('PTEM0103', 'PAN TIPO CHINO 45 GR 6 UND', 'PAQ', 0, 0, 1),
('PTEM0104', 'CAJA DE PALMERITAS 120 GR 8 UND', 'PAQ', 0, 0, 1),
('PTEM0105', 'PAN DE PERRO 63 GR 8 UND AB', 'PAQ', 10, 1, 1),
('PTEM0106', 'PAN INTEGRAL 700 GR', 'PAQ', 8, 1, 1);

INSERT INTO responsables (nombre_completo) VALUES
('Alexander Martinez'),
('Yefrin Arteaga'),
('Leandro Gil'),
('Jesus Alcedo'),
('Eliezer N'),
('Odalis'),
('Yosmar Blanco');

INSERT INTO destinos (nombre) VALUES ('K FOOD'), ('DESPACHO');
INSERT INTO sedes (nombre) VALUES ('PANIFICADORA COSTA DORADA, C.A.'), ('ALIMENTOS PB2, C.A.');
