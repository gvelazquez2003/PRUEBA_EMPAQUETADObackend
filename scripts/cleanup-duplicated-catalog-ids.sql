BEGIN;

CREATE TEMP TABLE catalog_cleanup_report (
  table_name TEXT NOT NULL,
  deleted_rows BIGINT NOT NULL
) ON COMMIT DROP;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_producto ORDER BY ctid) AS duplicate_number
  FROM productos
),
deleted AS (
  DELETE FROM productos t
  USING ranked d
  WHERE t.ctid = d.ctid
    AND d.duplicate_number > 1
  RETURNING t.id_producto
)
INSERT INTO catalog_cleanup_report
SELECT 'productos', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_destino ORDER BY ctid) AS duplicate_number
  FROM destinos
),
deleted AS (
  DELETE FROM destinos t
  USING ranked d
  WHERE t.ctid = d.ctid
    AND d.duplicate_number > 1
  RETURNING t.id_destino
)
INSERT INTO catalog_cleanup_report
SELECT 'destinos', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_responsable ORDER BY ctid) AS duplicate_number
  FROM responsables
),
deleted AS (
  DELETE FROM responsables t
  USING ranked d
  WHERE t.ctid = d.ctid
    AND d.duplicate_number > 1
  RETURNING t.id_responsable
)
INSERT INTO catalog_cleanup_report
SELECT 'responsables', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_sede ORDER BY ctid) AS duplicate_number
  FROM sedes
),
deleted AS (
  DELETE FROM sedes t
  USING ranked d
  WHERE t.ctid = d.ctid
    AND d.duplicate_number > 1
  RETURNING t.id_sede
)
INSERT INTO catalog_cleanup_report
SELECT 'sedes', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_motivo ORDER BY ctid) AS duplicate_number
  FROM motivos_merma
),
deleted AS (
  DELETE FROM motivos_merma t
  USING ranked d
  WHERE t.ctid = d.ctid
    AND d.duplicate_number > 1
  RETURNING t.id_motivo
)
INSERT INTO catalog_cleanup_report
SELECT 'motivos_merma', COUNT(*) FROM deleted;

CREATE UNIQUE INDEX IF NOT EXISTS repair_productos_id_unique ON productos (id_producto);
CREATE UNIQUE INDEX IF NOT EXISTS repair_destinos_id_unique ON destinos (id_destino);
CREATE UNIQUE INDEX IF NOT EXISTS repair_responsables_id_unique ON responsables (id_responsable);
CREATE UNIQUE INDEX IF NOT EXISTS repair_sedes_id_unique ON sedes (id_sede);
CREATE UNIQUE INDEX IF NOT EXISTS repair_motivos_id_unique ON motivos_merma (id_motivo);

SELECT setval(pg_get_serial_sequence('productos', 'id_producto'), COALESCE(MAX(id_producto), 1), COUNT(*) > 0) FROM productos;
SELECT setval(pg_get_serial_sequence('destinos', 'id_destino'), COALESCE(MAX(id_destino), 1), COUNT(*) > 0) FROM destinos;
SELECT setval(pg_get_serial_sequence('responsables', 'id_responsable'), COALESCE(MAX(id_responsable), 1), COUNT(*) > 0) FROM responsables;
SELECT setval(pg_get_serial_sequence('sedes', 'id_sede'), COALESCE(MAX(id_sede), 1), COUNT(*) > 0) FROM sedes;
SELECT setval(pg_get_serial_sequence('motivos_merma', 'id_motivo'), COALESCE(MAX(id_motivo), 1), COUNT(*) > 0) FROM motivos_merma;

SELECT table_name, deleted_rows
FROM catalog_cleanup_report
ORDER BY table_name;

COMMIT;
