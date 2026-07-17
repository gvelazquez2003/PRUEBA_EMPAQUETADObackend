-- Diagnostico rapido para el incidente de catalogo PTEM0001 / PTEM0005.
-- Ejecuta primero este archivo en phpPgAdmin.

SELECT
  'productos_actual' AS fuente,
  id_producto,
  codigo_producto,
  descripcion,
  unidad_primaria,
  paquetes,
  cestas,
  sobre_piso,
  activo,
  codigo_barras
FROM productos
WHERE UPPER(TRIM(codigo_producto)) IN ('PTEM0001', 'PTEM0005')
   OR UPPER(descripcion) LIKE '%HAMBURGUESA%POP%'
   OR UPPER(descripcion) LIKE '%CUADRADO%PEQUE%'
ORDER BY codigo_producto, id_producto;

DO $$
BEGIN
  IF TO_REGCLASS('public.backup_productos_catalogo_repair_20260716') IS NOT NULL THEN
    CREATE TEMP TABLE tmp_productos_backup_incidente ON COMMIT DROP AS
    SELECT
      backed_up_at,
      id_producto,
      codigo_producto,
      descripcion,
      unidad_primaria,
      paquetes,
      cestas,
      sobre_piso,
      activo,
      codigo_barras
    FROM backup_productos_catalogo_repair_20260716
    WHERE UPPER(TRIM(codigo_producto)) IN ('PTEM0001', 'PTEM0005')
       OR UPPER(descripcion) LIKE '%HAMBURGUESA%POP%'
       OR UPPER(descripcion) LIKE '%CUADRADO%PEQUE%';
  ELSE
    CREATE TEMP TABLE tmp_productos_backup_incidente (
      backed_up_at TIMESTAMPTZ,
      id_producto INT,
      codigo_producto TEXT,
      descripcion TEXT,
      unidad_primaria TEXT,
      paquetes INT,
      cestas INT,
      sobre_piso INT,
      activo BOOLEAN,
      codigo_barras TEXT
    ) ON COMMIT DROP;
  END IF;
END $$;

SELECT
  'backup_si_existe' AS fuente,
  *
FROM tmp_productos_backup_incidente
ORDER BY backed_up_at DESC, codigo_producto, id_producto;

SELECT
  'empaquetados_detalle' AS tabla,
  ed.id_producto,
  p.codigo_producto,
  p.descripcion,
  COUNT(*) AS filas,
  MIN(ec.fecha_hora) AS primera_fecha,
  MAX(ec.fecha_hora) AS ultima_fecha
FROM empaquetados_detalle ed
JOIN empaquetados_cabecera ec ON ec.id_cabecera = ed.id_cabecera
LEFT JOIN productos p ON p.id_producto = ed.id_producto
WHERE UPPER(TRIM(COALESCE(p.codigo_producto, ''))) IN ('PTEM0001', 'PTEM0005')
   OR ed.id_producto IN (
     SELECT id_producto
     FROM productos
     WHERE UPPER(TRIM(codigo_producto)) IN ('PTEM0001', 'PTEM0005')
   )
GROUP BY ed.id_producto, p.codigo_producto, p.descripcion
ORDER BY tabla, p.codigo_producto, ed.id_producto;

DO $$
BEGIN
  IF TO_REGCLASS('public.productos_audit') IS NOT NULL THEN
    CREATE TEMP TABLE tmp_productos_audit_incidente ON COMMIT DROP AS
    SELECT
      changed_at,
      operation,
      db_user,
      app_name,
      client_addr,
      old_row->>'codigo_producto' AS old_codigo,
      old_row->>'descripcion' AS old_descripcion,
      old_row->>'activo' AS old_activo,
      new_row->>'codigo_producto' AS new_codigo,
      new_row->>'descripcion' AS new_descripcion,
      new_row->>'activo' AS new_activo
    FROM productos_audit
    WHERE UPPER(TRIM(COALESCE(old_row->>'codigo_producto', new_row->>'codigo_producto', ''))) IN ('PTEM0001', 'PTEM0005')
       OR UPPER(COALESCE(old_row->>'descripcion', '')) LIKE '%HAMBURGUESA%POP%'
       OR UPPER(COALESCE(new_row->>'descripcion', '')) LIKE '%HAMBURGUESA%POP%'
       OR UPPER(COALESCE(old_row->>'descripcion', '')) LIKE '%CUADRADO%PEQUE%'
       OR UPPER(COALESCE(new_row->>'descripcion', '')) LIKE '%CUADRADO%PEQUE%';
  ELSE
    CREATE TEMP TABLE tmp_productos_audit_incidente (
      changed_at TIMESTAMPTZ,
      operation TEXT,
      db_user TEXT,
      app_name TEXT,
      client_addr TEXT,
      old_codigo TEXT,
      old_descripcion TEXT,
      old_activo TEXT,
      new_codigo TEXT,
      new_descripcion TEXT,
      new_activo TEXT
    ) ON COMMIT DROP;
  END IF;
END $$;

SELECT
  'audit_si_existe' AS fuente,
  *
FROM tmp_productos_audit_incidente
ORDER BY changed_at DESC NULLS LAST;
