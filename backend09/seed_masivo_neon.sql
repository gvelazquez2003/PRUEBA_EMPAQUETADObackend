CREATE TABLE IF NOT EXISTS conteo_errores (
  id SERIAL PRIMARY KEY,
  codigo_lote VARCHAR(50),
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS almacen_lotes_procesados (
  codigo_lote VARCHAR(50) PRIMARY KEY,
  estado VARCHAR(20) NOT NULL DEFAULT 'validado',
  processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
  resumen_validacion JSONB
);

CREATE TABLE IF NOT EXISTS destinos (
  id_destino SERIAL PRIMARY KEY,
  nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS sedes (
  id_sede SERIAL PRIMARY KEY,
  nombre VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS responsables (
  id_responsable SERIAL PRIMARY KEY,
  nombre_completo VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS productos (
  id_producto SERIAL PRIMARY KEY,
  codigo_producto VARCHAR(20) UNIQUE NOT NULL,
  descripcion VARCHAR(200) NOT NULL,
  unidad_primaria VARCHAR(50) NOT NULL,
  paquetes INT DEFAULT 0,
  cestas INT DEFAULT 0,
  sobre_piso INT DEFAULT 0
);

CREATE TABLE IF NOT EXISTS empaquetados_cabecera (
  id_cabecera SERIAL PRIMARY KEY,
  fecha_hora TIMESTAMP NOT NULL,
  id_destino INT REFERENCES destinos(id_destino),
  numero_registro VARCHAR(50),
  id_responsable INT REFERENCES responsables(id_responsable),
  id_sede INT REFERENCES sedes(id_sede)
);

CREATE TABLE IF NOT EXISTS empaquetados_detalle (
  id_detalle SERIAL PRIMARY KEY,
  id_cabecera INT REFERENCES empaquetados_cabecera(id_cabecera) ON DELETE CASCADE,
  id_producto INT REFERENCES productos(id_producto),
  cantidad INT NOT NULL,
  numero_lote VARCHAR(50) NOT NULL
);

BEGIN;

INSERT INTO destinos (nombre)
SELECT 'DESPACHO'
WHERE NOT EXISTS (SELECT 1 FROM destinos WHERE UPPER(TRIM(nombre)) = 'DESPACHO');

INSERT INTO sedes (nombre)
SELECT 'SEDE PRINCIPAL'
WHERE NOT EXISTS (SELECT 1 FROM sedes WHERE UPPER(TRIM(nombre)) = 'SEDE PRINCIPAL');

INSERT INTO responsables (nombre_completo)
SELECT 'USUARIO PRUEBA MASIVA'
WHERE NOT EXISTS (SELECT 1 FROM responsables WHERE UPPER(TRIM(nombre_completo)) = 'USUARIO PRUEBA MASIVA');

INSERT INTO productos (codigo_producto, descripcion, unidad_primaria, paquetes, cestas, sobre_piso)
VALUES
  ('PMASS0001', 'PRODUCTO MASIVO 1', 'PAQ', 10, 1, 1),
  ('PMASS0002', 'PRODUCTO MASIVO 2', 'PAQ', 10, 1, 1),
  ('PMASS0003', 'PRODUCTO MASIVO 3', 'PAQ', 10, 1, 1),
  ('PMASS0004', 'PRODUCTO MASIVO 4', 'PAQ', 10, 1, 1),
  ('PMASS0005', 'PRODUCTO MASIVO 5', 'PAQ', 10, 1, 1)
ON CONFLICT (codigo_producto) DO NOTHING;

WITH target_cab AS (
  SELECT id_cabecera
  FROM empaquetados_cabecera
  WHERE numero_registro LIKE 'MASS26-%'
), deleted_errores AS (
  DELETE FROM conteo_errores
  WHERE codigo_lote IN (SELECT 'CAB-' || id_cabecera::text FROM target_cab)
     OR codigo_lote LIKE 'MASS-26%'
  RETURNING 1
), deleted_alm AS (
  DELETE FROM almacen_lotes_procesados
  WHERE codigo_lote IN (SELECT 'CAB-' || id_cabecera::text FROM target_cab)
     OR codigo_lote LIKE 'MASS-26%'
  RETURNING 1
), deleted_det AS (
  DELETE FROM empaquetados_detalle
  WHERE id_cabecera IN (SELECT id_cabecera FROM target_cab)
  RETURNING 1
)
DELETE FROM empaquetados_cabecera
WHERE id_cabecera IN (SELECT id_cabecera FROM target_cab);

WITH params AS (
  SELECT 1200::int AS total_cabeceras, 5::int AS items_por_cabecera
), refs AS (
  SELECT
    (SELECT id_destino FROM destinos ORDER BY id_destino LIMIT 1) AS id_destino,
    (SELECT id_responsable FROM responsables ORDER BY id_responsable LIMIT 1) AS id_responsable,
    (SELECT id_sede FROM sedes ORDER BY id_sede LIMIT 1) AS id_sede
), productos_rank AS (
  SELECT id_producto, row_number() OVER (ORDER BY id_producto) AS rn
  FROM productos
), productos_meta AS (
  SELECT COUNT(*)::int AS total_productos FROM productos_rank
), cab_base AS (
  SELECT
    gs AS idx,
    NOW() - ((gs % 96) || ' hours')::interval AS fecha_hora,
    'MASS26-' || LPAD(gs::text, 6, '0') AS numero_registro
  FROM generate_series(1, (SELECT total_cabeceras FROM params)) AS gs
), inserted_cab AS (
  INSERT INTO empaquetados_cabecera (fecha_hora, id_destino, numero_registro, id_responsable, id_sede)
  SELECT cb.fecha_hora, r.id_destino, cb.numero_registro, r.id_responsable, r.id_sede
  FROM cab_base cb
  CROSS JOIN refs r
  RETURNING id_cabecera, numero_registro, fecha_hora
), cab_join AS (
  SELECT ic.id_cabecera, ic.numero_registro, ic.fecha_hora, cb.idx
  FROM inserted_cab ic
  JOIN cab_base cb ON cb.numero_registro = ic.numero_registro
), detail_source AS (
  SELECT
    cj.id_cabecera,
    cj.idx,
    cj.fecha_hora,
    pr.id_producto,
    (5 + ((cj.idx + gs) % 12))::int AS cantidad,
    'MASS-26-' || LPAD(cj.idx::text, 6, '0') AS numero_lote
  FROM cab_join cj
  CROSS JOIN generate_series(1, (SELECT items_por_cabecera FROM params)) AS gs
  JOIN productos_meta pm ON pm.total_productos > 0
  JOIN productos_rank pr
    ON pr.rn = (((cj.idx + gs - 2) % pm.total_productos) + 1)
), inserted_det AS (
  INSERT INTO empaquetados_detalle (id_cabecera, id_producto, cantidad, numero_lote)
  SELECT id_cabecera, id_producto, cantidad, numero_lote
  FROM detail_source
  RETURNING id_cabecera
), resumen_lote AS (
  SELECT
    ds.id_cabecera,
    cj.idx,
    cj.fecha_hora,
    jsonb_agg(
      jsonb_build_object(
        'id_producto', ds.id_producto,
        'cantidad_validada', ds.cantidad,
        'numero_lote', ds.numero_lote
      )
      ORDER BY ds.id_producto
    ) AS resumen_validacion
  FROM detail_source ds
  JOIN cab_join cj ON cj.id_cabecera = ds.id_cabecera
  GROUP BY ds.id_cabecera, cj.idx, cj.fecha_hora
)
INSERT INTO almacen_lotes_procesados (codigo_lote, estado, processed_at, resumen_validacion)
SELECT
  'CAB-' || rl.id_cabecera::text AS codigo_lote,
  CASE WHEN rl.idx % 15 = 0 THEN 'error' ELSE 'validado' END AS estado,
  rl.fecha_hora + interval '10 minutes' AS processed_at,
  rl.resumen_validacion
FROM resumen_lote rl
ON CONFLICT (codigo_lote)
DO UPDATE SET
  estado = EXCLUDED.estado,
  processed_at = EXCLUDED.processed_at,
  resumen_validacion = EXCLUDED.resumen_validacion;

INSERT INTO conteo_errores (codigo_lote, created_at)
SELECT codigo_lote, processed_at
FROM almacen_lotes_procesados
WHERE codigo_lote LIKE 'CAB-%'
  AND estado = 'error'
  AND codigo_lote IN (
    SELECT 'CAB-' || id_cabecera::text
    FROM empaquetados_cabecera
    WHERE numero_registro LIKE 'MASS26-%'
  );

COMMIT;

SELECT
  (SELECT COUNT(*) FROM empaquetados_cabecera WHERE numero_registro LIKE 'MASS26-%') AS empaquetados_cabecera_masivo,
  (SELECT COUNT(*) FROM empaquetados_detalle ed JOIN empaquetados_cabecera ec ON ec.id_cabecera = ed.id_cabecera WHERE ec.numero_registro LIKE 'MASS26-%') AS empaquetados_detalle_masivo,
  (SELECT COUNT(*) FROM almacen_lotes_procesados WHERE codigo_lote LIKE 'CAB-%') AS lotes_procesados_total,
  (SELECT COUNT(*) FROM almacen_lotes_procesados alp WHERE alp.codigo_lote IN (SELECT 'CAB-' || ec.id_cabecera::text FROM empaquetados_cabecera ec WHERE ec.numero_registro LIKE 'MASS26-%')) AS lotes_procesados_masivo,
  (SELECT COUNT(*) FROM conteo_errores WHERE codigo_lote IN (SELECT 'CAB-' || ec.id_cabecera::text FROM empaquetados_cabecera ec WHERE ec.numero_registro LIKE 'MASS26-%')) AS errores_masivos;

SELECT
  pg_size_pretty(pg_relation_size('empaquetados_cabecera')) AS empaquetados_cabecera_size,
  pg_size_pretty(pg_relation_size('empaquetados_detalle')) AS empaquetados_detalle_size,
  pg_size_pretty(pg_relation_size('almacen_lotes_procesados')) AS lotes_procesados_size,
  pg_size_pretty(pg_relation_size('conteo_errores')) AS conteo_errores_size,
  pg_size_pretty(pg_total_relation_size('almacen_lotes_procesados')) AS lotes_procesados_total,
  pg_size_pretty(pg_total_relation_size('empaquetados_detalle')) AS empaquetados_detalle_total;
