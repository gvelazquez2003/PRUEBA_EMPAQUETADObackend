-- Auditoria de clientes duplicados por RIF/C.I. + direccion.
-- Ejecutar en phpPgAdmin. Este script NO modifica datos.
--
-- Criterio:
-- - id_cliente se normaliza quitando espacios y caracteres no alfanumericos.
-- - direccion se normaliza con mayusculas y espacios repetidos reducidos.
-- - Se ignoran filas sin RIF/C.I. y filas sin direccion util ('', '0').

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
    CAST(id_cliente AS TEXT) AS id_cliente,
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
  'resumen' AS seccion,
  COUNT(DISTINCT rif_norm || '|' || direccion_norm)::BIGINT AS grupos_duplicados,
  COUNT(*)::BIGINT AS filas_en_grupos,
  SUM(CASE WHEN orden_conservacion > 1 THEN 1 ELSE 0 END)::BIGINT AS filas_que_se_borrarian
FROM ranked
WHERE total_copias > 1;

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
    CAST(id_cliente AS TEXT) AS id_cliente,
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
  rif_norm AS rif_normalizado,
  direccion_norm AS direccion_normalizada,
  total_copias,
  CASE WHEN orden_conservacion = 1 THEN 'CONSERVAR' ELSE 'BORRAR_COPIA' END AS accion_sugerida,
  row_key::TEXT AS row_key,
  id_cliente,
  descripcion,
  tipo_cliente,
  direccion,
  ruta,
  transporte,
  zona,
  vendedor
FROM ranked
WHERE total_copias > 1
ORDER BY rif_norm, direccion_norm, orden_conservacion, descripcion;
