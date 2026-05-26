BEGIN;

CREATE TEMP TABLE duplicate_cleanup_report (
  module TEXT NOT NULL,
  deleted_rows BIGINT NOT NULL
) ON COMMIT DROP;

-- Catalogos restaurados con IDs repetidos multiplican los resultados al hacer JOIN.
WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_producto ORDER BY ctid) AS duplicate_number
  FROM productos
),
deleted AS (
  DELETE FROM productos t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id_producto
)
INSERT INTO duplicate_cleanup_report
SELECT 'Catalogo - productos por ID', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_destino ORDER BY ctid) AS duplicate_number
  FROM destinos
),
deleted AS (
  DELETE FROM destinos t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id_destino
)
INSERT INTO duplicate_cleanup_report
SELECT 'Catalogo - destinos por ID', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_responsable ORDER BY ctid) AS duplicate_number
  FROM responsables
),
deleted AS (
  DELETE FROM responsables t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id_responsable
)
INSERT INTO duplicate_cleanup_report
SELECT 'Catalogo - responsables por ID', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_sede ORDER BY ctid) AS duplicate_number
  FROM sedes
),
deleted AS (
  DELETE FROM sedes t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id_sede
)
INSERT INTO duplicate_cleanup_report
SELECT 'Catalogo - sedes por ID', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_motivo ORDER BY ctid) AS duplicate_number
  FROM motivos_merma
),
deleted AS (
  DELETE FROM motivos_merma t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id_motivo
)
INSERT INTO duplicate_cleanup_report
SELECT 'Catalogo - motivos por ID', COUNT(*) FROM deleted;

-- Una restauracion sin claves primarias puede copiar la misma fila con el mismo ID.
WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_detalle ORDER BY ctid) AS duplicate_number
  FROM empaquetados_detalle
),
deleted AS (
  DELETE FROM empaquetados_detalle t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id_detalle
)
INSERT INTO duplicate_cleanup_report
SELECT 'Empaquetado - filas por ID', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_cabecera ORDER BY ctid) AS duplicate_number
  FROM empaquetados_cabecera
),
deleted AS (
  DELETE FROM empaquetados_cabecera t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id_cabecera
)
INSERT INTO duplicate_cleanup_report
SELECT 'Empaquetado - cabeceras por ID', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_detalle ORDER BY ctid) AS duplicate_number
  FROM mermas_detalle
),
deleted AS (
  DELETE FROM mermas_detalle t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id_detalle
)
INSERT INTO duplicate_cleanup_report
SELECT 'Merma - filas por ID', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_merma ORDER BY ctid) AS duplicate_number
  FROM mermas_cabecera
),
deleted AS (
  DELETE FROM mermas_cabecera t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id_merma
)
INSERT INTO duplicate_cleanup_report
SELECT 'Merma - cabeceras por ID', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_control ORDER BY ctid) AS duplicate_number
  FROM control_inventario_guardia
),
deleted AS (
  DELETE FROM control_inventario_guardia t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id_control
)
INSERT INTO duplicate_cleanup_report
SELECT 'Control de Inventario - filas por ID', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_cambio ORDER BY ctid) AS duplicate_number
  FROM cambios_registros
),
deleted AS (
  DELETE FROM cambios_registros t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id_cambio
)
INSERT INTO duplicate_cleanup_report
SELECT 'Cambios - filas por ID', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_detalle ORDER BY ctid) AS duplicate_number
  FROM almacen09_salidas_detalle
),
deleted AS (
  DELETE FROM almacen09_salidas_detalle t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id_detalle
)
INSERT INTO duplicate_cleanup_report
SELECT 'Salidas Factura - detalle por ID', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_factura ORDER BY ctid) AS duplicate_number
  FROM salidas_facturas
),
deleted AS (
  DELETE FROM salidas_facturas t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id_factura
)
INSERT INTO duplicate_cleanup_report
SELECT 'Salidas Factura - cabeceras por ID', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id_hoja ORDER BY ctid) AS duplicate_number
  FROM hojas_ruta_exportadas
),
deleted AS (
  DELETE FROM hojas_ruta_exportadas t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id_hoja
)
INSERT INTO duplicate_cleanup_report
SELECT 'Hojas de Ruta - filas por ID', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT ctid, ROW_NUMBER() OVER (PARTITION BY id ORDER BY ctid) AS duplicate_number
  FROM conteo_errores
),
deleted AS (
  DELETE FROM conteo_errores t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.id
)
INSERT INTO duplicate_cleanup_report
SELECT 'Errores de Conteo - filas por ID', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT
    ctid,
    ROW_NUMBER() OVER (
      PARTITION BY codigo_lote
      ORDER BY processed_at DESC NULLS LAST, ctid DESC
    ) AS duplicate_number
  FROM almacen_lotes_procesados
),
deleted AS (
  DELETE FROM almacen_lotes_procesados t
  USING ranked d
  WHERE t.ctid = d.ctid AND d.duplicate_number > 1
  RETURNING t.codigo_lote
)
INSERT INTO duplicate_cleanup_report
SELECT 'Almacen09 - lotes por codigo', COUNT(*) FROM deleted;

CREATE TEMP TABLE duplicate_empaquetados ON COMMIT DROP AS
WITH ranked AS (
  SELECT
    ec.id_cabecera,
    ROW_NUMBER() OVER (
      PARTITION BY
        ec.fecha_hora,
        ec.id_destino,
        COALESCE(ec.numero_registro, ''),
        ec.id_responsable,
        ec.id_sede,
        (
          SELECT COALESCE(
            jsonb_agg(
              jsonb_build_array(ed.id_producto, ed.cantidad, UPPER(TRIM(ed.numero_lote)))
              ORDER BY ed.id_producto, UPPER(TRIM(ed.numero_lote)), ed.cantidad
            ),
            '[]'::jsonb
          )
          FROM empaquetados_detalle ed
          WHERE ed.id_cabecera = ec.id_cabecera
        )
      ORDER BY ec.id_cabecera
    ) AS duplicate_number
  FROM empaquetados_cabecera ec
)
SELECT id_cabecera
FROM ranked
WHERE duplicate_number > 1;

DELETE FROM empaquetados_detalle
WHERE id_cabecera IN (SELECT id_cabecera FROM duplicate_empaquetados);

WITH deleted AS (
  DELETE FROM empaquetados_cabecera
  WHERE id_cabecera IN (SELECT id_cabecera FROM duplicate_empaquetados)
  RETURNING id_cabecera
)
INSERT INTO duplicate_cleanup_report
SELECT 'Empaquetado', COUNT(*) FROM deleted;

CREATE TEMP TABLE duplicate_mermas ON COMMIT DROP AS
WITH ranked AS (
  SELECT
    mc.id_merma,
    ROW_NUMBER() OVER (
      PARTITION BY
        mc.fecha_hora,
        mc.id_responsable,
        mc.id_sede,
        (
          SELECT COALESCE(
            jsonb_agg(
              jsonb_build_array(
                md.id_producto,
                md.cantidad,
                md.id_motivo,
                UPPER(TRIM(md.numero_lote))
              )
              ORDER BY md.id_producto, md.id_motivo, UPPER(TRIM(md.numero_lote)), md.cantidad
            ),
            '[]'::jsonb
          )
          FROM mermas_detalle md
          WHERE md.id_merma = mc.id_merma
        )
      ORDER BY mc.id_merma
    ) AS duplicate_number
  FROM mermas_cabecera mc
)
SELECT id_merma
FROM ranked
WHERE duplicate_number > 1;

DELETE FROM mermas_detalle
WHERE id_merma IN (SELECT id_merma FROM duplicate_mermas);

WITH deleted AS (
  DELETE FROM mermas_cabecera
  WHERE id_merma IN (SELECT id_merma FROM duplicate_mermas)
  RETURNING id_merma
)
INSERT INTO duplicate_cleanup_report
SELECT 'Merma', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT
    id_control,
    ROW_NUMBER() OVER (
      PARTITION BY
        TRIM(almacenista),
        TRIM(COALESCE(responsable, '')),
        TRIM(turno_actual),
        TRIM(momento_conteo),
        id_producto,
        cantidad_fisica_contada,
        UPPER(TRIM(COALESCE(numero_lote, ''))),
        fecha_conteo,
        fecha_elaboracion,
        TRIM(almacen),
        cestas,
        DATE_TRUNC('second', created_at)
      ORDER BY id_control
    ) AS duplicate_number
  FROM control_inventario_guardia
),
deleted AS (
  DELETE FROM control_inventario_guardia
  WHERE id_control IN (SELECT id_control FROM ranked WHERE duplicate_number > 1)
  RETURNING id_control
)
INSERT INTO duplicate_cleanup_report
SELECT 'Control de Inventario', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT
    id_cambio,
    ROW_NUMBER() OVER (
      PARTITION BY
        id_cliente,
        LOWER(TRIM(nombre_cliente)),
        direccion_id,
        LOWER(TRIM(COALESCE(direccion_texto, ''))),
        LOWER(TRIM(COALESCE(ruta_nombre, ''))),
        LOWER(TRIM(responsable)),
        producto,
        DATE_TRUNC('second', created_at)
      ORDER BY id_cambio
    ) AS duplicate_number
  FROM cambios_registros
),
deleted AS (
  DELETE FROM cambios_registros
  WHERE id_cambio IN (SELECT id_cambio FROM ranked WHERE duplicate_number > 1)
  RETURNING id_cambio
)
INSERT INTO duplicate_cleanup_report
SELECT 'Cambios', COUNT(*) FROM deleted;

CREATE TEMP TABLE duplicate_salidas ON COMMIT DROP AS
WITH ranked AS (
  SELECT
    sf.id_factura,
    ROW_NUMBER() OVER (
      PARTITION BY
        sf.numero_control,
        sf.documento,
        sf.numero_factura,
        sf.fecha_emision,
        sf.cliente_id,
        LOWER(TRIM(COALESCE(sf.cliente_nombre, ''))),
        sf.vendedor_id,
        LOWER(TRIM(COALESCE(sf.vendedor_nombre, ''))),
        sf.zona_id,
        LOWER(TRIM(COALESCE(sf.zona_nombre, ''))),
        LOWER(TRIM(COALESCE(sf.ruta_nombre, ''))),
        LOWER(TRIM(COALESCE(sf.transporte_nombre, ''))),
        sf.direccion_id,
        LOWER(TRIM(COALESCE(sf.direccion_texto, ''))),
        (
          SELECT COALESCE(
            jsonb_agg(
              jsonb_build_array(sd.id_producto, sd.id_cambio, sd.codigo_producto, sd.numero_lote, sd.cantidad)
              ORDER BY sd.id_producto, sd.id_cambio, sd.codigo_producto, sd.numero_lote, sd.cantidad
            ),
            '[]'::jsonb
          )
          FROM almacen09_salidas_detalle sd
          WHERE sd.id_factura = sf.id_factura
        )
      ORDER BY sf.id_factura
    ) AS duplicate_number
  FROM salidas_facturas sf
)
SELECT id_factura
FROM ranked
WHERE duplicate_number > 1;

DELETE FROM almacen09_salidas_detalle
WHERE id_factura IN (SELECT id_factura FROM duplicate_salidas);

WITH deleted AS (
  DELETE FROM salidas_facturas
  WHERE id_factura IN (SELECT id_factura FROM duplicate_salidas)
  RETURNING id_factura
)
INSERT INTO duplicate_cleanup_report
SELECT 'Salidas Factura', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT
    id_hoja,
    ROW_NUMBER() OVER (
      PARTITION BY
        LOWER(TRIM(ruta_nombre)),
        fecha_entrega,
        fecha_busqueda_desde,
        fecha_busqueda_hasta,
        LOWER(TRIM(COALESCE(conductor, ''))),
        LOWER(TRIM(COALESCE(numero_camion, ''))),
        total_despachos,
        total_cestas,
        LOWER(TRIM(COALESCE(usuario, ''))),
        md5(COALESCE(facturas::text, '[]')),
        md5(COALESCE(hoja_html, '')),
        COALESCE(nombre_archivo, ''),
        DATE_TRUNC('second', created_at)
      ORDER BY id_hoja
    ) AS duplicate_number
  FROM hojas_ruta_exportadas
),
deleted AS (
  DELETE FROM hojas_ruta_exportadas
  WHERE id_hoja IN (SELECT id_hoja FROM ranked WHERE duplicate_number > 1)
  RETURNING id_hoja
)
INSERT INTO duplicate_cleanup_report
SELECT 'Hojas de Ruta', COUNT(*) FROM deleted;

WITH ranked AS (
  SELECT
    id,
    ROW_NUMBER() OVER (
      PARTITION BY
        COALESCE(usuario, ''),
        COALESCE(codigo_lote, ''),
        COALESCE(lote_producto, ''),
        COALESCE(codigo_producto, ''),
        COALESCE(nombre_producto, ''),
        cantidad_esperada,
        cantidad_recibida,
        DATE_TRUNC('second', created_at)
      ORDER BY id
    ) AS duplicate_number
  FROM conteo_errores
),
deleted AS (
  DELETE FROM conteo_errores
  WHERE id IN (SELECT id FROM ranked WHERE duplicate_number > 1)
  RETURNING id
)
INSERT INTO duplicate_cleanup_report
SELECT 'Errores de Conteo', COUNT(*) FROM deleted;

CREATE UNIQUE INDEX IF NOT EXISTS repair_empaquetados_cabecera_id_unique ON empaquetados_cabecera (id_cabecera);
CREATE UNIQUE INDEX IF NOT EXISTS repair_empaquetados_detalle_id_unique ON empaquetados_detalle (id_detalle);
CREATE UNIQUE INDEX IF NOT EXISTS repair_productos_id_unique ON productos (id_producto);
CREATE UNIQUE INDEX IF NOT EXISTS repair_destinos_id_unique ON destinos (id_destino);
CREATE UNIQUE INDEX IF NOT EXISTS repair_responsables_id_unique ON responsables (id_responsable);
CREATE UNIQUE INDEX IF NOT EXISTS repair_sedes_id_unique ON sedes (id_sede);
CREATE UNIQUE INDEX IF NOT EXISTS repair_motivos_id_unique ON motivos_merma (id_motivo);
CREATE UNIQUE INDEX IF NOT EXISTS repair_mermas_cabecera_id_unique ON mermas_cabecera (id_merma);
CREATE UNIQUE INDEX IF NOT EXISTS repair_mermas_detalle_id_unique ON mermas_detalle (id_detalle);
CREATE UNIQUE INDEX IF NOT EXISTS repair_control_inventario_id_unique ON control_inventario_guardia (id_control);
CREATE UNIQUE INDEX IF NOT EXISTS repair_cambios_id_unique ON cambios_registros (id_cambio);
CREATE UNIQUE INDEX IF NOT EXISTS repair_salidas_facturas_id_unique ON salidas_facturas (id_factura);
CREATE UNIQUE INDEX IF NOT EXISTS repair_salidas_detalle_id_unique ON almacen09_salidas_detalle (id_detalle);
CREATE UNIQUE INDEX IF NOT EXISTS repair_hojas_ruta_id_unique ON hojas_ruta_exportadas (id_hoja);
CREATE UNIQUE INDEX IF NOT EXISTS repair_conteo_errores_id_unique ON conteo_errores (id);
CREATE UNIQUE INDEX IF NOT EXISTS repair_almacen_lotes_codigo_unique ON almacen_lotes_procesados (codigo_lote);

SELECT setval(pg_get_serial_sequence('empaquetados_cabecera', 'id_cabecera'), COALESCE(MAX(id_cabecera), 1), COUNT(*) > 0) FROM empaquetados_cabecera;
SELECT setval(pg_get_serial_sequence('empaquetados_detalle', 'id_detalle'), COALESCE(MAX(id_detalle), 1), COUNT(*) > 0) FROM empaquetados_detalle;
SELECT setval(pg_get_serial_sequence('productos', 'id_producto'), COALESCE(MAX(id_producto), 1), COUNT(*) > 0) FROM productos;
SELECT setval(pg_get_serial_sequence('destinos', 'id_destino'), COALESCE(MAX(id_destino), 1), COUNT(*) > 0) FROM destinos;
SELECT setval(pg_get_serial_sequence('responsables', 'id_responsable'), COALESCE(MAX(id_responsable), 1), COUNT(*) > 0) FROM responsables;
SELECT setval(pg_get_serial_sequence('sedes', 'id_sede'), COALESCE(MAX(id_sede), 1), COUNT(*) > 0) FROM sedes;
SELECT setval(pg_get_serial_sequence('motivos_merma', 'id_motivo'), COALESCE(MAX(id_motivo), 1), COUNT(*) > 0) FROM motivos_merma;
SELECT setval(pg_get_serial_sequence('mermas_cabecera', 'id_merma'), COALESCE(MAX(id_merma), 1), COUNT(*) > 0) FROM mermas_cabecera;
SELECT setval(pg_get_serial_sequence('mermas_detalle', 'id_detalle'), COALESCE(MAX(id_detalle), 1), COUNT(*) > 0) FROM mermas_detalle;
SELECT setval(pg_get_serial_sequence('control_inventario_guardia', 'id_control'), COALESCE(MAX(id_control), 1), COUNT(*) > 0) FROM control_inventario_guardia;
SELECT setval(pg_get_serial_sequence('cambios_registros', 'id_cambio'), COALESCE(MAX(id_cambio), 1), COUNT(*) > 0) FROM cambios_registros;
SELECT setval(pg_get_serial_sequence('salidas_facturas', 'id_factura'), COALESCE(MAX(id_factura), 1), COUNT(*) > 0) FROM salidas_facturas;
SELECT setval(pg_get_serial_sequence('almacen09_salidas_detalle', 'id_detalle'), COALESCE(MAX(id_detalle), 1), COUNT(*) > 0) FROM almacen09_salidas_detalle;
SELECT setval(pg_get_serial_sequence('hojas_ruta_exportadas', 'id_hoja'), COALESCE(MAX(id_hoja), 1), COUNT(*) > 0) FROM hojas_ruta_exportadas;
SELECT setval(pg_get_serial_sequence('conteo_errores', 'id'), COALESCE(MAX(id), 1), COUNT(*) > 0) FROM conteo_errores;

SELECT module, deleted_rows
FROM duplicate_cleanup_report
ORDER BY module;

COMMIT;
