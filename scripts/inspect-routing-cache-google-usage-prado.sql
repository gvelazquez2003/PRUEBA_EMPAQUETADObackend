-- Diagnostico no destructivo para exportacion/ruteo Prado.
-- 1) Revisa cuantas consultas de Google Geocoding fallback ha usado el backend.
-- 2) Muestra coordenadas cacheadas para direcciones tipicas de la ruta PRADO.
-- 3) Incluye al final un DELETE comentado para forzar regeocodificacion si ves pines malos.

SELECT
  provider,
  COUNT(*) FILTER (WHERE created_at >= date_trunc('day', NOW())) AS usadas_hoy,
  COUNT(*) FILTER (WHERE created_at >= date_trunc('month', NOW())) AS usadas_mes,
  COUNT(*) AS usadas_total,
  COUNT(*) FILTER (WHERE success) AS exitosas_total,
  COUNT(*) FILTER (WHERE NOT success) AS fallidas_total
FROM routing_geocoding_usage
GROUP BY provider
ORDER BY provider;

SELECT
  client_key,
  provider,
  status,
  partial_match,
  location_type,
  latitude,
  longitude,
  checked_at,
  formatted_address,
  reason,
  address
FROM address_validations
WHERE address ILIKE ANY (ARRAY[
  '%ALAMEDA%',
  '%CAURIMARE%',
  '%EXPRESO BARUTA%',
  '%BELLO MONTE%',
  '%SANTA ROSA%',
  '%MIRADOR%'
])
ORDER BY checked_at DESC, address;

-- Si ves coordenadas obviamente malas en el SELECT anterior, descomenta esto
-- para limpiar solo esas validaciones y luego presiona "Actualizar ruta" de nuevo.
--
-- DELETE FROM address_validations
-- WHERE address ILIKE ANY (ARRAY[
--   '%ALAMEDA%',
--   '%CAURIMARE%',
--   '%EXPRESO BARUTA%',
--   '%BELLO MONTE%',
--   '%SANTA ROSA%',
--   '%MIRADOR%'
-- ]);
