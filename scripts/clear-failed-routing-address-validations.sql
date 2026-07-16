DELETE FROM address_validations
WHERE status = 'not_found'
   OR latitude IS NULL
   OR longitude IS NULL
   OR NOT (
     latitude BETWEEN -90 AND 90
     AND longitude BETWEEN -180 AND 180
   );

SELECT COUNT(*) AS validaciones_restantes
FROM address_validations;
