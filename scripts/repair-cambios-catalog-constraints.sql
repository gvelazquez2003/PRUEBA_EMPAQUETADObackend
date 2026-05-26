BEGIN;

CREATE TEMP TABLE cambios_catalog_repair_report (
  object_name TEXT NOT NULL,
  deleted_rows BIGINT NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE duplicate_clientes ON COMMIT DROP AS
SELECT duplicate.id_cliente AS duplicate_id, canonical.retained_id
FROM almacen09_clientes duplicate
JOIN (
  SELECT nombre, MIN(id_cliente) AS retained_id
  FROM almacen09_clientes
  GROUP BY nombre
  HAVING COUNT(*) > 1
) canonical ON canonical.nombre = duplicate.nombre
WHERE duplicate.id_cliente <> canonical.retained_id;

UPDATE cambios_registros target
SET id_cliente = duplicate.retained_id
FROM duplicate_clientes duplicate
WHERE target.id_cliente = duplicate.duplicate_id;

UPDATE salidas_facturas target
SET cliente_id = duplicate.retained_id
FROM duplicate_clientes duplicate
WHERE target.cliente_id = duplicate.duplicate_id;

UPDATE salidas_cliente_sucursales target
SET cliente_id = duplicate.retained_id
FROM duplicate_clientes duplicate
WHERE target.cliente_id = duplicate.duplicate_id;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY nombre ORDER BY id_cliente, ctid) AS duplicate_number
  FROM almacen09_clientes
),
deleted AS (
  DELETE FROM almacen09_clientes target
  USING ranked duplicate
  WHERE target.ctid = duplicate.ctid
    AND duplicate.duplicate_number > 1
  RETURNING target.id_cliente
)
INSERT INTO cambios_catalog_repair_report
SELECT 'almacen09_clientes.nombre', COUNT(*) FROM deleted;

CREATE TEMP TABLE duplicate_direcciones ON COMMIT DROP AS
SELECT duplicate.id_direccion AS duplicate_id, canonical.retained_id
FROM almacen09_direcciones duplicate
JOIN (
  SELECT direccion, MIN(id_direccion) AS retained_id
  FROM almacen09_direcciones
  GROUP BY direccion
  HAVING COUNT(*) > 1
) canonical ON canonical.direccion = duplicate.direccion
WHERE duplicate.id_direccion <> canonical.retained_id;

UPDATE cambios_registros target
SET direccion_id = duplicate.retained_id
FROM duplicate_direcciones duplicate
WHERE target.direccion_id = duplicate.duplicate_id;

UPDATE salidas_facturas target
SET direccion_id = duplicate.retained_id
FROM duplicate_direcciones duplicate
WHERE target.direccion_id = duplicate.duplicate_id;

UPDATE salidas_cliente_sucursales target
SET direccion_id = duplicate.retained_id
FROM duplicate_direcciones duplicate
WHERE target.direccion_id = duplicate.duplicate_id;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY direccion ORDER BY id_direccion, ctid) AS duplicate_number
  FROM almacen09_direcciones
),
deleted AS (
  DELETE FROM almacen09_direcciones target
  USING ranked duplicate
  WHERE target.ctid = duplicate.ctid
    AND duplicate.duplicate_number > 1
  RETURNING target.id_direccion
)
INSERT INTO cambios_catalog_repair_report
SELECT 'almacen09_direcciones.direccion', COUNT(*) FROM deleted;

CREATE TEMP TABLE duplicate_zonas ON COMMIT DROP AS
SELECT duplicate.id_zona AS duplicate_id, canonical.retained_id
FROM almacen09_zonas duplicate
JOIN (
  SELECT nombre, MIN(id_zona) AS retained_id
  FROM almacen09_zonas
  GROUP BY nombre
  HAVING COUNT(*) > 1
) canonical ON canonical.nombre = duplicate.nombre
WHERE duplicate.id_zona <> canonical.retained_id;

UPDATE salidas_facturas target
SET zona_id = duplicate.retained_id
FROM duplicate_zonas duplicate
WHERE target.zona_id = duplicate.duplicate_id;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY nombre ORDER BY id_zona, ctid) AS duplicate_number
  FROM almacen09_zonas
),
deleted AS (
  DELETE FROM almacen09_zonas target
  USING ranked duplicate
  WHERE target.ctid = duplicate.ctid
    AND duplicate.duplicate_number > 1
  RETURNING target.id_zona
)
INSERT INTO cambios_catalog_repair_report
SELECT 'almacen09_zonas.nombre', COUNT(*) FROM deleted;

CREATE TEMP TABLE duplicate_sucursales ON COMMIT DROP AS
SELECT duplicate.id_sucursal AS duplicate_id, canonical.retained_id
FROM almacen09_sucursales duplicate
JOIN (
  SELECT nombre, MIN(id_sucursal) AS retained_id
  FROM almacen09_sucursales
  GROUP BY nombre
  HAVING COUNT(*) > 1
) canonical ON canonical.nombre = duplicate.nombre
WHERE duplicate.id_sucursal <> canonical.retained_id;

UPDATE salidas_facturas target
SET sucursal_id = duplicate.retained_id
FROM duplicate_sucursales duplicate
WHERE target.sucursal_id = duplicate.duplicate_id;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY nombre ORDER BY id_sucursal, ctid) AS duplicate_number
  FROM almacen09_sucursales
),
deleted AS (
  DELETE FROM almacen09_sucursales target
  USING ranked duplicate
  WHERE target.ctid = duplicate.ctid
    AND duplicate.duplicate_number > 1
  RETURNING target.id_sucursal
)
INSERT INTO cambios_catalog_repair_report
SELECT 'almacen09_sucursales.nombre', COUNT(*) FROM deleted;

WITH deleted AS (
  DELETE FROM cambios_razones duplicate
  USING cambios_razones retained
  WHERE duplicate.ctid > retained.ctid
    AND duplicate.razon_texto = retained.razon_texto
  RETURNING duplicate.id_razon
)
INSERT INTO cambios_catalog_repair_report
SELECT 'cambios_razones.razon_texto', COUNT(*) FROM deleted;

-- New index names prevent legacy non-unique indexes from masking this repair.
CREATE UNIQUE INDEX IF NOT EXISTS idx_almacen09_clientes_nombre_unique ON almacen09_clientes (nombre);
CREATE UNIQUE INDEX IF NOT EXISTS idx_almacen09_direcciones_direccion_unique ON almacen09_direcciones (direccion);
CREATE UNIQUE INDEX IF NOT EXISTS idx_almacen09_zonas_nombre_unique ON almacen09_zonas (nombre);
CREATE UNIQUE INDEX IF NOT EXISTS idx_almacen09_sucursales_nombre_unique ON almacen09_sucursales (nombre);
CREATE UNIQUE INDEX IF NOT EXISTS idx_cambios_razones_texto_unique ON cambios_razones (razon_texto);

SELECT setval(pg_get_serial_sequence('almacen09_clientes', 'id_cliente'), COALESCE(MAX(id_cliente), 1), COUNT(*) > 0) FROM almacen09_clientes;
SELECT setval(pg_get_serial_sequence('almacen09_direcciones', 'id_direccion'), COALESCE(MAX(id_direccion), 1), COUNT(*) > 0) FROM almacen09_direcciones;
SELECT setval(pg_get_serial_sequence('almacen09_zonas', 'id_zona'), COALESCE(MAX(id_zona), 1), COUNT(*) > 0) FROM almacen09_zonas;
SELECT setval(pg_get_serial_sequence('almacen09_sucursales', 'id_sucursal'), COALESCE(MAX(id_sucursal), 1), COUNT(*) > 0) FROM almacen09_sucursales;
SELECT setval(pg_get_serial_sequence('cambios_razones', 'id_razon'), COALESCE(MAX(id_razon), 1), COUNT(*) > 0) FROM cambios_razones;

SELECT object_name, deleted_rows
FROM cambios_catalog_repair_report
ORDER BY object_name;

COMMIT;
