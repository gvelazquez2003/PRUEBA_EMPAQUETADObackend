INSERT INTO motivos_merma (nombre)
SELECT 'QUEMADO'
WHERE NOT EXISTS (
  SELECT 1
  FROM motivos_merma
  WHERE LOWER(TRIM(nombre)) = LOWER(TRIM('QUEMADO'))
);
