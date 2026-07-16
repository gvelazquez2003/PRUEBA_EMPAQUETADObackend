DELETE FROM address_validations
WHERE status = 'not_found'
   OR latitude IS NULL
   OR longitude IS NULL
   OR NOT (
     latitude BETWEEN -90 AND 90
     AND longitude BETWEEN -180 AND 180
   )
   -- Limpia pines obviamente fuera de la zona operativa Caracas/Miranda/centro.
   -- Ejemplo del error visto: longitud -64.x para una ruta de Chacao.
   OR NOT (
     latitude BETWEEN 9.0 AND 11.5
     AND longitude BETWEEN -68.8 AND -65.2
   );

SELECT COUNT(*) AS validaciones_restantes
FROM address_validations;
