-- Elimina copias duplicadas de public.clientes por RIF/C.I. + direccion.
-- Ejecutar en phpPgAdmin DESPUES de revisar 01-auditar-clientes-duplicados-rif-direccion.sql.
--
-- Seguridad:
-- - Crea respaldo de las filas que se van a borrar.
-- - Usa transaccion.
-- - Conserva una fila por cada par normalizado RIF/C.I. + direccion.
-- - No toca filas sin RIF/C.I. ni filas con direccion vacia o '0'.

BEGIN;

LOCK TABLE public.clientes IN ACCESS EXCLUSIVE MODE;

CREATE TABLE IF NOT EXISTS public.backup_clientes_dup_rif_direccion_20260731 AS
SELECT
  NOW() AS backed_up_at,
  c.*
FROM public.clientes c
WHERE FALSE;

CREATE TEMP TABLE tmp_clientes_duplicados_por_borrar ON COMMIT DROP AS
WITH base AS (
  SELECT
    ctid AS row_key,
    REGEXP_REPLACE(
      UPPER(TRIM(CAST(id_cliente AS TEXT))),
      '[^A-Z0-9]',
      '',
      'g'
    ) AS rif_norm,
    REGEXP_REPLACE(
      UPPER(TRIM(COALESCE(direccion, ''))),
      '\s+',
      ' ',
      'g'
    ) AS direccion_norm,
    id_cliente,
    descripcion,
    tipo_cliente,
    direccion,
    ruta,
    transporte,
    zona,
    vendedor
  FROM public.clientes
),
candidatos AS (
  SELECT *
  FROM base
  WHERE rif_norm <> ''
    AND direccion_norm NOT IN ('', '0')
),
ranked AS (
  SELECT
    *,
    COUNT(*) OVER (PARTITION BY rif_norm, direccion_norm) AS total_copias,
    ROW_NUMBER() OVER (
      PARTITION BY rif_norm, direccion_norm
      ORDER BY
        (
          CASE WHEN NULLIF(TRIM(COALESCE(descripcion, '')), '') IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN NULLIF(TRIM(COALESCE(tipo_cliente, '')), '') IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN NULLIF(TRIM(COALESCE(ruta, '')), '') IS NOT NULL AND UPPER(TRIM(COALESCE(ruta, ''))) <> 'REVISAR MANUALMENTE' THEN 2 ELSE 0 END +
          CASE WHEN NULLIF(TRIM(COALESCE(transporte, '')), '') IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN NULLIF(TRIM(COALESCE(zona, '')), '') IS NOT NULL THEN 1 ELSE 0 END +
          CASE WHEN NULLIF(TRIM(COALESCE(vendedor, '')), '') IS NOT NULL THEN 1 ELSE 0 END
        ) DESC,
        row_key ASC
    ) AS orden_conservacion
  FROM candidatos
)
SELECT
  row_key,
  rif_norm,
  direccion_norm,
  total_copias,
  id_cliente,
  descripcion,
  direccion
FROM ranked
WHERE total_copias > 1
  AND orden_conservacion > 1;

INSERT INTO public.backup_clientes_dup_rif_direccion_20260731
SELECT
  NOW() AS backed_up_at,
  c.*
FROM public.clientes c
JOIN tmp_clientes_duplicados_por_borrar d
  ON c.ctid = d.row_key;

WITH borrados AS (
  DELETE FROM public.clientes c
  USING tmp_clientes_duplicados_por_borrar d
  WHERE c.ctid = d.row_key
  RETURNING d.rif_norm, d.direccion_norm, d.id_cliente, d.descripcion, d.direccion
)
SELECT
  'filas_borradas' AS resultado,
  COUNT(*)::BIGINT AS filas
FROM borrados;

WITH base AS (
  SELECT
    REGEXP_REPLACE(
      UPPER(TRIM(CAST(id_cliente AS TEXT))),
      '[^A-Z0-9]',
      '',
      'g'
    ) AS rif_norm,
    REGEXP_REPLACE(
      UPPER(TRIM(COALESCE(direccion, ''))),
      '\s+',
      ' ',
      'g'
    ) AS direccion_norm
  FROM public.clientes
),
duplicados_restantes AS (
  SELECT rif_norm, direccion_norm
  FROM base
  WHERE rif_norm <> ''
    AND direccion_norm NOT IN ('', '0')
  GROUP BY rif_norm, direccion_norm
  HAVING COUNT(*) > 1
)
SELECT
  'grupos_duplicados_restantes' AS revision,
  COUNT(*)::BIGINT AS grupos
FROM duplicados_restantes;

COMMIT;
