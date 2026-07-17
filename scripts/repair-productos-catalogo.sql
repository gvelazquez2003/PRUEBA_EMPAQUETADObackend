BEGIN;

LOCK TABLE productos IN SHARE ROW EXCLUSIVE MODE;

CREATE TABLE IF NOT EXISTS backup_productos_catalogo_repair_20260716 AS
SELECT NOW() AS backed_up_at, p.*
FROM productos p
WHERE FALSE;

INSERT INTO backup_productos_catalogo_repair_20260716
SELECT NOW() AS backed_up_at, p.*
FROM productos p;

ALTER TABLE productos
  ADD COLUMN IF NOT EXISTS activo BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE productos
  ADD COLUMN IF NOT EXISTS codigo_barras TEXT;

UPDATE productos
   SET codigo_producto = UPPER(TRIM(codigo_producto)),
       codigo_barras = UPPER(TRIM(COALESCE(codigo_barras, codigo_producto)))
 WHERE codigo_producto IS NOT NULL
   AND (
     codigo_producto <> UPPER(TRIM(codigo_producto))
     OR COALESCE(codigo_barras, '') = ''
   );

UPDATE productos
   SET codigo_producto = CONCAT('SIN-CODIGO-', COALESCE(id_producto::TEXT, ctid::TEXT)),
       codigo_barras = CONCAT('SIN-CODIGO-', COALESCE(id_producto::TEXT, ctid::TEXT))
 WHERE TRIM(COALESCE(codigo_producto, '')) = '';

CREATE TEMP TABLE tmp_productos_repair_report (
  paso TEXT NOT NULL,
  filas BIGINT NOT NULL
) ON COMMIT DROP;

CREATE TEMP TABLE tmp_productos_codigo_ranked ON COMMIT DROP AS
WITH base AS (
  SELECT
    ctid AS row_ctid,
    id_producto,
    UPPER(TRIM(codigo_producto)) AS codigo_norm,
    descripcion,
    unidad_primaria,
    COALESCE(paquetes, 0) AS paquetes,
    COALESCE(cestas, 0) AS cestas,
    COALESCE(sobre_piso, 0) AS sobre_piso,
    COALESCE(activo, TRUE) AS activo
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
    ) AS keep_id,
    FIRST_VALUE(row_ctid) OVER (
      PARTITION BY codigo_norm
      ORDER BY
        activo DESC,
        cestas DESC,
        sobre_piso DESC,
        paquetes DESC,
        id_producto ASC,
        row_ctid ASC
    ) AS keep_ctid
  FROM base
)
SELECT * FROM ranked;

CREATE TEMP TABLE tmp_productos_id_map ON COMMIT DROP AS
SELECT DISTINCT
  id_producto AS drop_id,
  keep_id
FROM tmp_productos_codigo_ranked
WHERE duplicate_number > 1
  AND id_producto <> keep_id;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'empaquetados_detalle' AND column_name = 'id_producto'
  ) THEN
    UPDATE empaquetados_detalle d
       SET id_producto = m.keep_id
      FROM tmp_productos_id_map m
     WHERE d.id_producto = m.drop_id;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'mermas_detalle' AND column_name = 'id_producto'
  ) THEN
    UPDATE mermas_detalle d
       SET id_producto = m.keep_id
      FROM tmp_productos_id_map m
     WHERE d.id_producto = m.drop_id;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'control_inventario_guardia' AND column_name = 'id_producto'
  ) THEN
    UPDATE control_inventario_guardia d
       SET id_producto = m.keep_id
      FROM tmp_productos_id_map m
     WHERE d.id_producto = m.drop_id;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'almacen09_salidas_detalle' AND column_name = 'id_producto'
  ) THEN
    UPDATE almacen09_salidas_detalle d
       SET id_producto = m.keep_id
      FROM tmp_productos_id_map m
     WHERE d.id_producto = m.drop_id;
  END IF;
END $$;

INSERT INTO tmp_productos_repair_report
SELECT 'referencias_id_producto_actualizadas', COUNT(*)
FROM tmp_productos_id_map;

WITH merged AS (
  SELECT
    codigo_norm,
    keep_ctid,
    BOOL_OR(activo) AS activo,
    MAX(paquetes) AS paquetes,
    MAX(cestas) AS cestas,
    MAX(sobre_piso) AS sobre_piso
  FROM tmp_productos_codigo_ranked
  GROUP BY codigo_norm, keep_ctid
)
UPDATE productos p
   SET codigo_producto = m.codigo_norm,
       codigo_barras = m.codigo_norm,
       activo = m.activo,
       paquetes = GREATEST(COALESCE(p.paquetes, 0), m.paquetes),
       cestas = GREATEST(COALESCE(p.cestas, 0), m.cestas),
       sobre_piso = GREATEST(COALESCE(p.sobre_piso, 0), m.sobre_piso)
  FROM merged m
 WHERE p.ctid = m.keep_ctid;

WITH deleted AS (
  DELETE FROM productos p
  USING tmp_productos_codigo_ranked r
  WHERE p.ctid = r.row_ctid
    AND r.duplicate_number > 1
  RETURNING p.codigo_producto
)
INSERT INTO tmp_productos_repair_report
SELECT 'duplicados_codigo_eliminados', COUNT(*)
FROM deleted;

CREATE TEMP TABLE tmp_productos_id_ranked ON COMMIT DROP AS
SELECT
  ctid AS row_ctid,
  id_producto,
  ROW_NUMBER() OVER (PARTITION BY id_producto ORDER BY ctid) AS duplicate_number
FROM productos;

DO $$
DECLARE
  seq_name TEXT;
  changed_rows BIGINT := 0;
BEGIN
  SELECT pg_get_serial_sequence('productos', 'id_producto') INTO seq_name;

  IF seq_name IS NOT NULL THEN
    UPDATE productos p
       SET id_producto = nextval(seq_name::regclass)
      FROM tmp_productos_id_ranked r
     WHERE p.ctid = r.row_ctid
       AND r.duplicate_number > 1;

    GET DIAGNOSTICS changed_rows = ROW_COUNT;

    INSERT INTO tmp_productos_repair_report
    VALUES ('ids_producto_duplicados_reasignados', changed_rows);
  ELSE
    INSERT INTO tmp_productos_repair_report
    VALUES ('ids_producto_duplicados_reasignados_sin_sequence', 0);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS repair_productos_id_unique
  ON productos (id_producto);

CREATE UNIQUE INDEX IF NOT EXISTS idx_productos_codigo_producto_unique
  ON productos (codigo_producto);

CREATE UNIQUE INDEX IF NOT EXISTS idx_productos_codigo_upper_unique
  ON productos (UPPER(TRIM(codigo_producto)));

DO $$
DECLARE
  seq_name TEXT;
BEGIN
  SELECT pg_get_serial_sequence('productos', 'id_producto') INTO seq_name;

  IF seq_name IS NOT NULL THEN
    PERFORM setval(
      seq_name::regclass,
      COALESCE((SELECT MAX(id_producto) FROM productos), 1),
      (SELECT COUNT(*) > 0 FROM productos)
    );
  END IF;
END $$;

UPDATE productos
   SET codigo_barras = UPPER(TRIM(codigo_producto))
 WHERE UPPER(TRIM(COALESCE(codigo_barras, ''))) <> UPPER(TRIM(COALESCE(codigo_producto, '')));

SELECT paso, filas
FROM tmp_productos_repair_report
ORDER BY paso;

SELECT
  'duplicados_codigo_restantes' AS revision,
  COUNT(*)::BIGINT AS filas
FROM (
  SELECT UPPER(TRIM(codigo_producto)) AS codigo
  FROM productos
  WHERE TRIM(COALESCE(codigo_producto, '')) <> ''
  GROUP BY UPPER(TRIM(codigo_producto))
  HAVING COUNT(*) > 1
) d
UNION ALL
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
  'productos_activos' AS revision,
  COUNT(*)::BIGINT AS filas
FROM productos
WHERE COALESCE(activo, TRUE) = TRUE;

COMMIT;
