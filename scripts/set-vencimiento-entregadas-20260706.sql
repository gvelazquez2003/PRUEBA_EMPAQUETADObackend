BEGIN;

ALTER TABLE salidas_facturas
  ADD COLUMN IF NOT EXISTS fecha_vencimiento DATE;

WITH hojas AS (
  SELECT DISTINCT ON (id_hoja)
    id_hoja,
    COALESCE(facturas, '[]'::jsonb) AS facturas
  FROM hojas_ruta_exportadas
  ORDER BY id_hoja, created_at DESC
),
ruta_facturas AS (
  SELECT
    CASE
      WHEN inv.factura->>'id_factura' ~ '^[0-9]+$' THEN (inv.factura->>'id_factura')::bigint
      ELSE NULL
    END AS id_factura,
    CONCAT(
      h.id_hoja,
      ':',
      COALESCE(NULLIF(inv.factura->>'numero_control', ''), NULLIF(inv.factura->>'id_factura', ''), inv.orden::text),
      ':',
      COALESCE(NULLIF(inv.factura->>'numero_factura', ''), NULLIF(inv.factura->>'id_factura', ''), inv.orden::text),
      '::hoja:',
      h.id_hoja,
      '::',
      CASE
        WHEN TRIM(COALESCE(inv.factura->>'direccion_texto', '')) = '' THEN ''
        WHEN LOWER(REGEXP_REPLACE(TRIM(inv.factura->>'direccion_texto'), '\s+', ' ', 'g')) LIKE '%venezuela%' THEN TRIM(inv.factura->>'direccion_texto')
        WHEN LOWER(REGEXP_REPLACE(TRIM(inv.factura->>'direccion_texto'), '\s+', ' ', 'g')) LIKE '%caracas%' THEN CONCAT(TRIM(inv.factura->>'direccion_texto'), ', Venezuela')
        ELSE CONCAT(TRIM(inv.factura->>'direccion_texto'), ', Caracas, Venezuela')
      END
    ) AS client_key,
    LOWER(TRIM(COALESCE(inv.factura->>'entregado', 'false'))) = 'true' AS entregado_en_factura
  FROM hojas h
  CROSS JOIN LATERAL jsonb_array_elements(h.facturas) WITH ORDINALITY AS inv(factura, orden)
),
facturas_entregadas AS (
  SELECT DISTINCT rf.id_factura
  FROM ruta_facturas rf
  LEFT JOIN delivery_status ds ON ds.client_key = rf.client_key
  WHERE rf.id_factura IS NOT NULL
    AND (
      COALESCE(ds.delivered, FALSE)
      OR (
        COALESCE(ds.partial, FALSE)
        AND COALESCE(jsonb_array_length(COALESCE(ds.partial_detail, '[]'::jsonb)), 0) > 0
      )
      OR rf.entregado_en_factura
    )
),
actualizadas AS (
  UPDATE salidas_facturas sf
     SET fecha_vencimiento = DATE '2026-07-06'
    FROM facturas_entregadas fe
   WHERE sf.id_factura = fe.id_factura
     AND sf.fecha_vencimiento IS NULL
     AND LOWER(TRIM(COALESCE(sf.transporte_nombre, ''))) <> 'retiro'
  RETURNING
    sf.id_factura,
    sf.documento,
    sf.numero_factura,
    sf.numero_control,
    sf.cliente_nombre,
    sf.vendedor_nombre,
    sf.fecha_emision,
    sf.fecha_vencimiento
)
SELECT
  COUNT(*) AS facturas_actualizadas,
  DATE '2026-07-06' AS fecha_vencimiento_asignada
FROM actualizadas;

COMMIT;
