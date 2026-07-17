-- Reparacion puntual del catalogo.
-- Objetivo:
-- 1) PTEM0001 debe volver a ser PAN DE HAMBURGUESA 85 GR 6 UND.
-- 2) PTEM0005 debe existir y estar activo como PAN CUADRADO PEQUEÑO 575 GR 17 UND.
--
-- Ejecuta primero diagnose-productos-ptem0001-ptem0005.sql.

BEGIN;

ALTER TABLE productos
  ADD COLUMN IF NOT EXISTS activo BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE productos
  ADD COLUMN IF NOT EXISTS codigo_barras TEXT;

UPDATE productos
   SET descripcion = 'PAN DE HAMBURGUESA 85 GR 6 UND',
       unidad_primaria = COALESCE(NULLIF(TRIM(unidad_primaria), ''), 'PAQ'),
       codigo_producto = 'PTEM0001',
       codigo_barras = 'PTEM0001',
       activo = TRUE
 WHERE UPPER(TRIM(codigo_producto)) = 'PTEM0001';

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
WHERE UPPER(TRIM(codigo_producto)) IN ('PTEM0001', 'PTEM0005')
ORDER BY codigo_producto, id_producto;

COMMIT;
