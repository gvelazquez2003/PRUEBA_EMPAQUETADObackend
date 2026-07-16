-- Repara colisiones de id_producto causadas por productos POP creados sobre IDs historicos.
--
-- Regla del incidente:
-- - Los productos POP son nuevos.
-- - Los productos normales conservan sus IDs historicos para no romper registros antiguos.
-- - Los POP que compartan id_producto con normales reciben IDs nuevos.
-- - Duplicados por codigo se archivan, no se borran; las referencias se apuntan al producto activo correcto.
--
-- Ejecutar en phpPgAdmin.

BEGIN;

LOCK TABLE productos IN ACCESS EXCLUSIVE MODE;

CREATE TABLE IF NOT EXISTS backup_productos_pop_id_repair_20260716 AS
SELECT NOW() AS backed_up_at, p.*
FROM productos p
WHERE FALSE;

INSERT INTO backup_productos_pop_id_repair_20260716
SELECT NOW() AS backed_up_at, p.*
FROM productos p;

ALTER TABLE productos
  ADD COLUMN IF NOT EXISTS activo BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE productos
  ADD COLUMN IF NOT EXISTS codigo_barras TEXT;

UPDATE productos
   SET codigo_producto = UPPER(TRIM(codigo_producto)),
       codigo_barras = UPPER(TRIM(COALESCE(NULLIF(codigo_barras, ''), codigo_producto)))
 WHERE codigo_producto IS NOT NULL;

CREATE TEMP TABLE tmp_pop_repair_report (
  paso TEXT NOT NULL,
  filas BIGINT NOT NULL
) ON COMMIT DROP;

DO $$
DECLARE
  seq_name TEXT;
BEGIN
  SELECT pg_get_serial_sequence('productos', 'id_producto') INTO seq_name;

  IF seq_name IS NULL THEN
    CREATE SEQUENCE IF NOT EXISTS productos_id_producto_seq;
    ALTER TABLE productos
      ALTER COLUMN id_producto SET DEFAULT nextval('productos_id_producto_seq');
    seq_name := 'productos_id_producto_seq';
  END IF;

  PERFORM setval(
    seq_name::regclass,
    GREATEST(COALESCE((SELECT MAX(id_producto) FROM productos), 0), 1),
    TRUE
  );
END $$;

CREATE TEMP TABLE tmp_pop_conflicts ON COMMIT DROP AS
SELECT
  p.ctid AS row_ctid,
  p.id_producto AS old_id_producto,
  p.codigo_producto,
  p.descripcion
FROM productos p
WHERE UPPER(COALESCE(p.descripcion, '')) LIKE '%POP%'
  AND EXISTS (
    SELECT 1
    FROM productos normal
    WHERE normal.id_producto = p.id_producto
      AND normal.ctid <> p.ctid
      AND UPPER(COALESCE(normal.descripcion, '')) NOT LIKE '%POP%'
  );

DO $$
DECLARE
  seq_name TEXT;
  changed_rows BIGINT := 0;
BEGIN
  SELECT pg_get_serial_sequence('productos', 'id_producto') INTO seq_name;

  UPDATE productos p
     SET id_producto = nextval(seq_name::regclass)
    FROM tmp_pop_conflicts c
   WHERE p.ctid = c.row_ctid;

  GET DIAGNOSTICS changed_rows = ROW_COUNT;
  INSERT INTO tmp_pop_repair_report VALUES ('pop_reasignados_por_colision_con_normales', changed_rows);
END $$;

CREATE TEMP TABLE tmp_duplicate_ids ON COMMIT DROP AS
SELECT
  ctid AS row_ctid,
  id_producto,
  codigo_producto,
  descripcion,
  ROW_NUMBER() OVER (
    PARTITION BY id_producto
    ORDER BY
      (UPPER(COALESCE(descripcion, '')) LIKE '%POP%') ASC,
      COALESCE(activo, TRUE) DESC,
      COALESCE(cestas, 0) DESC,
      COALESCE(sobre_piso, 0) DESC,
      COALESCE(paquetes, 0) DESC,
      codigo_producto ASC,
      ctid ASC
  ) AS duplicate_number
FROM productos;

DO $$
DECLARE
  seq_name TEXT;
  changed_rows BIGINT := 0;
BEGIN
  SELECT pg_get_serial_sequence('productos', 'id_producto') INTO seq_name;

  UPDATE productos p
     SET id_producto = nextval(seq_name::regclass)
    FROM tmp_duplicate_ids d
   WHERE p.ctid = d.row_ctid
     AND d.duplicate_number > 1;

  GET DIAGNOSTICS changed_rows = ROW_COUNT;
  INSERT INTO tmp_pop_repair_report VALUES ('ids_duplicados_reasignados', changed_rows);
END $$;

CREATE TEMP TABLE tmp_productos_por_codigo ON COMMIT DROP AS
WITH base AS (
  SELECT
    ctid AS row_ctid,
    id_producto,
    UPPER(TRIM(codigo_producto)) AS codigo_norm,
    descripcion,
    COALESCE(activo, TRUE) AS activo,
    COALESCE(cestas, 0) AS cestas,
    COALESCE(sobre_piso, 0) AS sobre_piso,
    COALESCE(paquetes, 0) AS paquetes,
    UPPER(COALESCE(descripcion, '')) LIKE '%POP%' AS es_pop
  FROM productos
  WHERE TRIM(COALESCE(codigo_producto, '')) <> ''
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY codigo_norm
      ORDER BY
        activo DESC,
        cestas DESC,
        sobre_piso DESC,
        paquetes DESC,
        id_producto ASC,
        row_ctid ASC
    ) AS duplicate_number,
    FIRST_VALUE(id_producto) OVER (
      PARTITION BY codigo_norm
      ORDER BY
        activo DESC,
        cestas DESC,
        sobre_piso DESC,
        paquetes DESC,
        id_producto ASC,
        row_ctid ASC
    ) AS keep_id
  FROM base
)
SELECT * FROM ranked;

CREATE TEMP TABLE tmp_codigo_id_map ON COMMIT DROP AS
SELECT
  id_producto AS drop_id,
  keep_id
FROM tmp_productos_por_codigo
WHERE duplicate_number > 1
  AND id_producto <> keep_id;

DO $$
DECLARE
  table_name TEXT;
  updated_rows BIGINT;
  total_rows BIGINT := 0;
BEGIN
  FOR table_name IN
    SELECT c.table_name
    FROM information_schema.columns c
    WHERE c.table_schema = 'public'
      AND c.column_name = 'id_producto'
      AND c.table_name <> 'productos'
  LOOP
    EXECUTE FORMAT(
      'UPDATE %I t SET id_producto = m.keep_id FROM tmp_codigo_id_map m WHERE t.id_producto = m.drop_id',
      table_name
    );
    GET DIAGNOSTICS updated_rows = ROW_COUNT;
    total_rows := total_rows + updated_rows;
  END LOOP;

  INSERT INTO tmp_pop_repair_report VALUES ('referencias_actualizadas_por_codigo_duplicado', total_rows);
END $$;

WITH archived AS (
  UPDATE productos p
     SET activo = FALSE,
         -- codigo_producto/codigo_barras pueden ser VARCHAR(20); usar un marcador corto.
         codigo_producto = CONCAT('DUP-', p.id_producto),
         codigo_barras = CONCAT('DUP-', p.id_producto)
    FROM tmp_productos_por_codigo r
   WHERE p.ctid = r.row_ctid
     AND r.duplicate_number > 1
  RETURNING p.id_producto
)
INSERT INTO tmp_pop_repair_report
SELECT 'filas_duplicadas_por_codigo_archivadas', COUNT(*)
FROM archived;

-- Correcciones puntuales conocidas del catalogo base.
UPDATE productos
   SET descripcion = 'PAN DE HAMBURGUESA 85 GR 6 UND',
       unidad_primaria = COALESCE(NULLIF(TRIM(unidad_primaria), ''), 'PAQ'),
       codigo_producto = 'PTEM0001',
       codigo_barras = 'PTEM0001',
       activo = TRUE
 WHERE UPPER(TRIM(codigo_producto)) = 'PTEM0001';

UPDATE productos
   SET descripcion = 'PAN DE PERRO 63 GR 8 UND',
       unidad_primaria = COALESCE(NULLIF(TRIM(unidad_primaria), ''), 'PAQ'),
       codigo_producto = 'PTEM0002',
       codigo_barras = 'PTEM0002',
       activo = TRUE
 WHERE UPPER(TRIM(codigo_producto)) = 'PTEM0002';

UPDATE productos
   SET descripcion = 'PAN TIPO DELI 110 GR 4 UND',
       unidad_primaria = COALESCE(NULLIF(TRIM(unidad_primaria), ''), 'PAQ'),
       codigo_producto = 'PTEM0004',
       codigo_barras = 'PTEM0004',
       activo = TRUE
 WHERE UPPER(TRIM(codigo_producto)) = 'PTEM0004';

INSERT INTO productos (
  codigo_producto,
  codigo_barras,
  descripcion,
  unidad_primaria,
  paquetes,
  cestas,
  sobre_piso,
  activo
)
SELECT
  'PTEM0005',
  'PTEM0005',
  U&'PAN CUADRADO PEQUE\00D1O 575 GR 17 UND',
  'PAQ',
  0,
  0,
  0,
  TRUE
WHERE NOT EXISTS (
  SELECT 1
  FROM productos
  WHERE UPPER(TRIM(codigo_producto)) = 'PTEM0005'
);

UPDATE productos
   SET descripcion = U&'PAN CUADRADO PEQUE\00D1O 575 GR 17 UND',
       unidad_primaria = COALESCE(NULLIF(TRIM(unidad_primaria), ''), 'PAQ'),
       codigo_producto = 'PTEM0005',
       codigo_barras = 'PTEM0005',
       activo = TRUE
 WHERE UPPER(TRIM(codigo_producto)) = 'PTEM0005';

-- Correcciones puntuales conocidas de los nuevos POP.
UPDATE productos
   SET descripcion = 'PAN POP BATATA DE HAMBURGUESA 85 GR 6 UND',
       codigo_barras = 'PTEM0218',
       activo = TRUE
 WHERE UPPER(TRIM(codigo_producto)) = 'PTEM0218';

UPDATE productos
   SET descripcion = 'PAN POP BATATA DE PERRO 63 GR 8 UND',
       codigo_barras = 'PTEM0214',
       activo = TRUE
 WHERE UPPER(TRIM(codigo_producto)) = 'PTEM0214';

UPDATE productos
   SET descripcion = 'PAN POP BATATA CUADRADO 500 GR 17 UND',
       codigo_barras = 'PTEM0213',
       activo = TRUE
 WHERE UPPER(TRIM(codigo_producto)) = 'PTEM0213';

UPDATE productos
   SET descripcion = 'PAN POP BLANCO CUADRADO 500 GR 17 UND',
       codigo_barras = 'PTEM0211',
       activo = TRUE
 WHERE UPPER(TRIM(codigo_producto)) = 'PTEM0211';

UPDATE productos
   SET descripcion = 'PAN POP BLANCO DE PERRO 63 GR 8 UND',
       codigo_barras = 'PTEM0212',
       activo = TRUE
 WHERE UPPER(TRIM(codigo_producto)) = 'PTEM0212';

DO $$
DECLARE
  seq_name TEXT;
BEGIN
  SELECT pg_get_serial_sequence('productos', 'id_producto') INTO seq_name;

  IF seq_name IS NOT NULL THEN
    PERFORM setval(
      seq_name::regclass,
      GREATEST(COALESCE((SELECT MAX(id_producto) FROM productos), 0), 1),
      TRUE
    );
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS repair_productos_id_unique
  ON productos (id_producto);

CREATE UNIQUE INDEX IF NOT EXISTS idx_productos_codigo_producto_unique
  ON productos (codigo_producto);

CREATE UNIQUE INDEX IF NOT EXISTS idx_productos_codigo_upper_unique
  ON productos (UPPER(TRIM(codigo_producto)));

SELECT paso, filas
FROM tmp_pop_repair_report
ORDER BY paso;

SELECT
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
WHERE UPPER(TRIM(codigo_producto)) IN (
  'PTEM0001',
  'PTEM0002',
  'PTEM0004',
  'PTEM0005',
  'PTEM0211',
  'PTEM0212',
  'PTEM0213',
  'PTEM0214',
  'PTEM0218'
)
ORDER BY codigo_producto, id_producto;

SELECT
  'duplicados_id_restantes' AS revision,
  COUNT(*)::BIGINT AS filas
FROM (
  SELECT id_producto
  FROM productos
  GROUP BY id_producto
  HAVING COUNT(*) > 1
) d
UNION ALL
SELECT
  'duplicados_codigo_activo_restantes' AS revision,
  COUNT(*)::BIGINT AS filas
FROM (
  SELECT UPPER(TRIM(codigo_producto)) AS codigo_producto
  FROM productos
  WHERE COALESCE(activo, TRUE) = TRUE
  GROUP BY UPPER(TRIM(codigo_producto))
  HAVING COUNT(*) > 1
) d;

COMMIT;
