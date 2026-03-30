import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { Pool } from 'pg';

dotenv.config();

const app = express();
const port = Number(process.env.PORT || 3001);
const adminKey = process.env.ADMIN_KEY || '#FANDETATA';
const legacyAdminKey = '#FANDETATA';
const STOCK_RESET_DATE = String(process.env.STOCK_RESET_DATE || '2026-03-30').trim();

function normalizeOrigin(value) {
  const raw = String(value || '').trim().replace(/^['"]|['"]$/g, '');
  if (!raw) return '';
  if (raw === '*' || raw === 'https://*' || raw === 'http://*') return raw;
  try {
    return new URL(raw).origin;
  } catch (_) {
    return raw.replace(/[\/?#].*$/, '').replace(/\/+$/, '');
  }
}

const rawCorsOrigin = String(process.env.CORS_ORIGIN || '').trim();
const corsOrigins = rawCorsOrigin
  ? rawCorsOrigin
      .split(',')
      .map((item) => normalizeOrigin(item))
      .filter(Boolean)
  : ['*'];
const allowAnyOrigin = corsOrigins.includes('*') || corsOrigins.includes('https://*') || corsOrigins.includes('http://*');

app.use(
  cors({
    origin: (origin, callback) => {
      if (allowAnyOrigin || !origin) return callback(null, true);
      const normalizedOrigin = normalizeOrigin(origin);
      const isAllowed = corsOrigins.includes(normalizedOrigin);
      return callback(isAllowed ? null : new Error('Not allowed by CORS'), isAllowed);
    },
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
  })
);
app.options('*', cors());

app.use((req, res, next) => {
  const requestOrigin = req.headers.origin;
  if (allowAnyOrigin && requestOrigin) {
    res.setHeader('Access-Control-Allow-Origin', requestOrigin);
    res.setHeader('Vary', 'Origin');
  }
  res.setHeader('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  next();
});
app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

function combineFechaHora(fecha, hora) {
  if (!fecha) return null;
  const safeHora = hora && String(hora).trim() ? String(hora).trim() : '00:00';
  return `${fecha} ${safeHora}:00`;
}

function isValidAdminKey(sentKey) {
  const normalized = String(sentKey || '').trim();
  if (!normalized) return false;
  return normalized === String(adminKey || '').trim() || normalized === legacyAdminKey;
}

async function ensureAlmacen09Tables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS conteo_errores (
      id SERIAL PRIMARY KEY,
      codigo_lote VARCHAR(50),
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS almacen_lotes_procesados (
      codigo_lote VARCHAR(50) PRIMARY KEY,
      estado VARCHAR(20) NOT NULL DEFAULT 'validado',
      processed_at TIMESTAMP NOT NULL DEFAULT NOW(),
      resumen_validacion JSONB
    )
  `);

  // Backward-compatible migration: older deployments may already have the table without newer columns.
  await pool.query(`
    ALTER TABLE almacen_lotes_procesados
    ADD COLUMN IF NOT EXISTS estado VARCHAR(20) NOT NULL DEFAULT 'validado'
  `);
  await pool.query(`
    ALTER TABLE almacen_lotes_procesados
    ADD COLUMN IF NOT EXISTS processed_at TIMESTAMP NOT NULL DEFAULT NOW()
  `);
  await pool.query(`
    ALTER TABLE almacen_lotes_procesados
    ADD COLUMN IF NOT EXISTS resumen_validacion JSONB
  `);
}

async function ensureProductosSoftDelete() {
  await pool.query(`
    ALTER TABLE productos
    ADD COLUMN IF NOT EXISTS activo BOOLEAN NOT NULL DEFAULT TRUE
  `);
}

async function ensureHistoricoResultadosTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS historico_resultados_consolidado (
      id_historico BIGSERIAL PRIMARY KEY,
      fecha DATE,
      fecha_empaquetado TIMESTAMP,
      fecha_almacen09 TIMESTAMP,
      codigo_producto VARCHAR(30),
      producto TEXT NOT NULL,
      cantidad INTEGER,
      entregado_a VARCHAR(120),
      numero_registro VARCHAR(50),
      responsable VARCHAR(120),
      sede VARCHAR(160),
      numero_lote VARCHAR(80),
      source_hash VARCHAR(64) UNIQUE NOT NULL,
      origen_historico VARCHAR(20) NOT NULL DEFAULT 'csv',
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_historico_resultados_fecha_empaquetado
    ON historico_resultados_consolidado (fecha_empaquetado DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_historico_resultados_fecha_almacen09
    ON historico_resultados_consolidado (fecha_almacen09 DESC)
  `);
}

async function registrarErrorConteo(codigoLote) {
  try {
    await pool.query('INSERT INTO conteo_errores (codigo_lote) VALUES ($1)', [codigoLote || null]);
  } catch (error) {
    console.error('Error registrando conteo_errores:', error);
  }
}

app.get('/health', async (_req, res) => {
  try {
    await pool.query('SELECT 1');
    res.json({ ok: true, status: 'healthy' });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get('/destinos', async (_req, res) => {
  try {
    const result = await pool.query('SELECT id_destino, nombre FROM destinos ORDER BY nombre');
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/sedes', async (_req, res) => {
  try {
    const result = await pool.query(
      `SELECT id_sede, nombre
       FROM sedes
       WHERE UPPER(TRIM(COALESCE(nombre, ''))) <> 'SEDE PRINCIPAL'
       ORDER BY nombre`
    );
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/responsables', async (_req, res) => {
  try {
    const result = await pool.query(
      `SELECT id_responsable, nombre_completo
       FROM responsables
       WHERE UPPER(TRIM(COALESCE(nombre_completo, ''))) <> 'USUARIO PRUEBA MASIVA'
       ORDER BY nombre_completo`
    );
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/productos', async (_req, res) => {
  try {
    const result = await pool.query(`
      SELECT
        id_producto,
        codigo_producto,
        descripcion AS nombre_producto,
        descripcion,
        unidad_primaria,
        paquetes AS paquetes_por_cesta,
        sobre_piso
      FROM productos
      WHERE COALESCE(activo, TRUE) = TRUE
      ORDER BY codigo_producto ASC
    `);
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/productos', async (req, res) => {
  const { codigo, descripcion, unidad, paquetes, sobre_piso, adminKey: bodyKey } = req.body;
  const sentKey = bodyKey || req.query?.adminKey || req.headers['x-admin-key'];
  if (!isValidAdminKey(sentKey)) {
    return res.status(403).json({ ok: false, error: 'adminKey inválido' });
  }
  if (!codigo || !descripcion) {
    return res.status(400).json({ ok: false, error: 'codigo y descripcion son obligatorios' });
  }

  try {
    const result = await pool.query(
      `INSERT INTO productos (codigo_producto, descripcion, unidad_primaria, paquetes, sobre_piso, activo)
       VALUES ($1, $2, $3, $4, $5, TRUE)
       ON CONFLICT (codigo_producto) DO UPDATE
       SET descripcion = EXCLUDED.descripcion,
           unidad_primaria = EXCLUDED.unidad_primaria,
           paquetes = EXCLUDED.paquetes,
           sobre_piso = EXCLUDED.sobre_piso,
           activo = TRUE
       RETURNING id_producto, codigo_producto, descripcion, unidad_primaria, paquetes, sobre_piso`,
      [
        String(codigo).trim().toUpperCase(),
        String(descripcion).trim().toUpperCase(),
        String(unidad || 'PAQ').trim().toUpperCase(),
        Number.isFinite(Number(paquetes)) ? Number(paquetes) : 0,
        Number.isFinite(Number(sobre_piso)) ? Number(sobre_piso) : 0,
      ]
    );
    res.status(201).json({ ok: true, product: result.rows[0] });
  } catch (error) {
    if (error.code === '23505') {
      return res.status(409).json({ ok: false, error: 'Código de producto ya existe' });
    }
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.delete('/productos/:codigo', async (req, res) => {
  const { codigo } = req.params;
  const sentKey = req.body?.adminKey || req.query?.adminKey || req.headers['x-admin-key'];
  if (!isValidAdminKey(sentKey)) {
    return res.status(403).json({ ok: false, error: 'adminKey inválido' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const productResult = await client.query(
      'SELECT id_producto, codigo_producto, COALESCE(activo, TRUE) AS activo FROM productos WHERE LOWER(codigo_producto) = LOWER($1) LIMIT 1',
      [codigo]
    );
    if (!productResult.rowCount) {
      await client.query('ROLLBACK');
      return res.status(404).json({ ok: false, error: 'Producto no encontrado' });
    }

    const idProducto = productResult.rows[0].id_producto;
    const wasActive = Boolean(productResult.rows[0].activo);
    const updateResult = await client.query(
      'UPDATE productos SET activo = FALSE WHERE id_producto = $1 RETURNING id_producto, codigo_producto',
      [idProducto]
    );

    await client.query('COMMIT');

    res.json({
      ok: true,
      archived: {
        producto: updateResult.rowCount,
        wasActive,
      },
    });
  } catch (error) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    res.status(500).json({ ok: false, error: error.message });
  } finally {
    client.release();
  }
});

app.post('/api/empaquetados', async (req, res) => {
  const { cabecera, detalle } = req.body || {};
  if (!cabecera || !Array.isArray(detalle) || !detalle.length) {
    return res.status(400).json({ ok: false, error: 'cabecera y detalle son obligatorios' });
  }

  const timestamp = combineFechaHora(cabecera.fecha, cabecera.hora);
  if (!timestamp || !cabecera.id_destino || !cabecera.id_responsable || !cabecera.id_sede) {
    return res.status(400).json({ ok: false, error: 'Faltan campos en cabecera' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const headerResult = await client.query(
      `INSERT INTO empaquetados_cabecera (fecha_hora, id_destino, numero_registro, id_responsable, id_sede)
       VALUES ($1, $2, $3, $4, $5)
       RETURNING id_cabecera`,
      [
        timestamp,
        Number(cabecera.id_destino),
        String(cabecera.numero_registro || '').trim(),
        Number(cabecera.id_responsable),
        Number(cabecera.id_sede),
      ]
    );

    const idCabecera = headerResult.rows[0].id_cabecera;
    for (const item of detalle) {
      if (!item.id_producto || !item.cantidad || !item.numero_lote) {
        throw new Error('Cada item requiere id_producto, cantidad y numero_lote');
      }
      await client.query(
        `INSERT INTO empaquetados_detalle (id_cabecera, id_producto, cantidad, numero_lote)
         VALUES ($1, $2, $3, $4)`,
        [idCabecera, Number(item.id_producto), Number(item.cantidad), String(item.numero_lote).trim().toUpperCase()]
      );
    }

    await client.query('COMMIT');
    res.status(201).json({ ok: true, id_cabecera: idCabecera });
  } catch (error) {
    await client.query('ROLLBACK');
    res.status(500).json({ ok: false, error: error.message });
  } finally {
    client.release();
  }
});

app.get('/api/registros', async (req, res) => {
  const tipo = String(req.query.tipo || 'Consolidado').trim().toLowerCase();
  const limit = Math.min(Math.max(Number(req.query.limit || 200), 1), 5000);
  const desde = String(req.query.desde || '').trim();
  const hasta = String(req.query.hasta || '').trim();
  const semana = String(req.query.semana || '').trim();
  const fecha = String(req.query.fecha || '').trim();
  const mes = String(req.query.mes || '').trim();
  const anio = String(req.query.anio || '').trim();
  const almacenTsVzExpr = `(alp.processed_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Caracas')`;
  const almacenCabCodigoExpr = `UPPER(TRIM(split_part(alp.codigo_lote, '::', 1)))`;

  const hasDesde = /^\d{4}-\d{2}-\d{2}$/.test(desde);
  const hasHasta = /^\d{4}-\d{2}-\d{2}$/.test(hasta);
  const hasSemana = /^\d{4}-W(0[1-9]|[1-4][0-9]|5[0-3])$/.test(semana);
  const hasFecha = /^\d{4}-\d{2}-\d{2}$/.test(fecha);
  const hasMes = /^\d{4}-\d{2}$/.test(mes);
  const hasMesNumero = /^(0[1-9]|1[0-2])$/.test(mes);
  const hasAnio = /^\d{4}$/.test(anio);

  try {
    if (tipo === 'consolidado') {
      const wherePartsActual = [];
      const wherePartsHistorico = [];
      const params = [];
      if (hasDesde) {
        params.push(desde);
        wherePartsActual.push(`DATE(ec.fecha_hora) >= $${params.length}`);
        wherePartsHistorico.push(`DATE(COALESCE(hr.fecha_empaquetado, hr.fecha::timestamp)) >= $${params.length}`);
      }
      if (hasHasta) {
        params.push(hasta);
        wherePartsActual.push(`DATE(ec.fecha_hora) <= $${params.length}`);
        wherePartsHistorico.push(`DATE(COALESCE(hr.fecha_empaquetado, hr.fecha::timestamp)) <= $${params.length}`);
      }
      if (hasSemana) {
        params.push(semana);
        wherePartsActual.push(`TO_CHAR(ec.fecha_hora, 'IYYY-"W"IW') = $${params.length}`);
        wherePartsHistorico.push(`TO_CHAR(COALESCE(hr.fecha_empaquetado, hr.fecha::timestamp), 'IYYY-"W"IW') = $${params.length}`);
      }
      if (hasFecha) {
        params.push(fecha);
        wherePartsActual.push(`DATE(ec.fecha_hora) = $${params.length}`);
        wherePartsHistorico.push(`DATE(COALESCE(hr.fecha_empaquetado, hr.fecha::timestamp)) = $${params.length}`);
      }
      if (hasMes) {
        params.push(mes);
        wherePartsActual.push(`TO_CHAR(ec.fecha_hora, 'YYYY-MM') = $${params.length}`);
        wherePartsHistorico.push(`TO_CHAR(COALESCE(hr.fecha_empaquetado, hr.fecha::timestamp), 'YYYY-MM') = $${params.length}`);
      }
      if (hasMesNumero) {
        params.push(mes);
        wherePartsActual.push(`TO_CHAR(ec.fecha_hora, 'MM') = $${params.length}`);
        wherePartsHistorico.push(`TO_CHAR(COALESCE(hr.fecha_empaquetado, hr.fecha::timestamp), 'MM') = $${params.length}`);
      }
      if (hasAnio) {
        params.push(anio);
        wherePartsActual.push(`TO_CHAR(ec.fecha_hora, 'YYYY') = $${params.length}`);
        wherePartsHistorico.push(`TO_CHAR(COALESCE(hr.fecha_empaquetado, hr.fecha::timestamp), 'YYYY') = $${params.length}`);
      }
      params.push(limit);
      const whereClauseActual = wherePartsActual.length ? `WHERE ${wherePartsActual.join(' AND ')}` : '';
      const whereClauseHistorico = wherePartsHistorico.length ? `WHERE ${wherePartsHistorico.join(' AND ')}` : '';

      const result = await pool.query(
        `WITH actual AS (
          SELECT
            ed.id_detalle AS "__ROW_ID",
            TO_CHAR(ec.fecha_hora, 'YYYY-MM-DD') AS "FECHA",
            TO_CHAR(ec.fecha_hora, 'DD/MM/YYYY HH24:MI') AS "Fecha Empaquetado",
            CASE
              WHEN alp.estado = 'validado' AND alp.processed_at IS NOT NULL
                THEN TO_CHAR(${almacenTsVzExpr}, 'DD/MM/YYYY HH24:MI')
              ELSE NULL
            END AS "Fecha Almacen09",
            p.codigo_producto AS "CODIGO PRODUCTO",
            p.descripcion AS "PRODUCTO",
            ed.cantidad AS "CANTIDAD",
            d.nombre AS "ENTREGADO A",
            ec.numero_registro AS "NUMERO REGISTRO",
            r.nombre_completo AS "RESPONSABLE",
            s.nombre AS "SEDE",
            ed.numero_lote AS "NUMERO DE LOTE",
            ec.fecha_hora AS "__ORDER_TS"
          FROM empaquetados_detalle ed
          JOIN empaquetados_cabecera ec ON ec.id_cabecera = ed.id_cabecera
          JOIN productos p ON p.id_producto = ed.id_producto
          JOIN destinos d ON d.id_destino = ec.id_destino
          JOIN responsables r ON r.id_responsable = ec.id_responsable
          JOIN sedes s ON s.id_sede = ec.id_sede
          LEFT JOIN almacen_lotes_procesados alp ON ${almacenCabCodigoExpr} = UPPER(TRIM(CONCAT('CAB-', ec.id_cabecera)))
          ${whereClauseActual}
        ),
        historico AS (
          SELECT
            NULL::int AS "__ROW_ID",
            TO_CHAR(COALESCE(hr.fecha_empaquetado, hr.fecha::timestamp), 'YYYY-MM-DD') AS "FECHA",
            CASE
              WHEN hr.fecha_empaquetado IS NOT NULL THEN TO_CHAR(hr.fecha_empaquetado, 'DD/MM/YYYY HH24:MI')
              WHEN hr.fecha IS NOT NULL THEN TO_CHAR(hr.fecha::timestamp, 'DD/MM/YYYY HH24:MI')
              ELSE NULL
            END AS "Fecha Empaquetado",
            CASE
              WHEN hr.fecha_almacen09 IS NOT NULL THEN TO_CHAR(hr.fecha_almacen09, 'DD/MM/YYYY HH24:MI')
              ELSE NULL
            END AS "Fecha Almacen09",
            hr.codigo_producto AS "CODIGO PRODUCTO",
            hr.producto AS "PRODUCTO",
            hr.cantidad AS "CANTIDAD",
            hr.entregado_a AS "ENTREGADO A",
            hr.numero_registro AS "NUMERO REGISTRO",
            hr.responsable AS "RESPONSABLE",
            hr.sede AS "SEDE",
            hr.numero_lote AS "NUMERO DE LOTE",
            COALESCE(hr.fecha_empaquetado, hr.fecha::timestamp) AS "__ORDER_TS"
          FROM historico_resultados_consolidado hr
          ${whereClauseHistorico}
        ),
        unificado AS (
          SELECT * FROM actual
          UNION ALL
          SELECT * FROM historico
        )
        SELECT
          "__ROW_ID",
          "FECHA",
          "Fecha Empaquetado",
          "Fecha Almacen09",
          "CODIGO PRODUCTO",
          "PRODUCTO",
          "CANTIDAD",
          "ENTREGADO A",
          "NUMERO REGISTRO",
          "RESPONSABLE",
          "SEDE",
          "NUMERO DE LOTE"
        FROM unificado
        ORDER BY "__ORDER_TS" DESC NULLS LAST
        LIMIT $${params.length}`,
        params
      );

      const headers = result.rows.length ? Object.keys(result.rows[0]) : [];
      return res.json({ ok: true, sheet: 'Consolidado', headers, rows: result.rows, total: result.rows.length });
    }

    if (tipo === 'almacen09') {
      const whereParts = [`alp.estado = 'validado'`];
      const params = [];
      if (hasDesde) {
        params.push(desde);
        whereParts.push(`DATE(${almacenTsVzExpr}) >= $${params.length}`);
      }
      if (hasHasta) {
        params.push(hasta);
        whereParts.push(`DATE(${almacenTsVzExpr}) <= $${params.length}`);
      }
      if (hasSemana) {
        params.push(semana);
        whereParts.push(`TO_CHAR(${almacenTsVzExpr}, 'IYYY-"W"IW') = $${params.length}`);
      }
      if (hasFecha) {
        params.push(fecha);
        whereParts.push(`DATE(${almacenTsVzExpr}) = $${params.length}`);
      }
      if (hasMes) {
        params.push(mes);
        whereParts.push(`TO_CHAR(${almacenTsVzExpr}, 'YYYY-MM') = $${params.length}`);
      }
      if (hasMesNumero) {
        params.push(mes);
        whereParts.push(`TO_CHAR(${almacenTsVzExpr}, 'MM') = $${params.length}`);
      }
      if (hasAnio) {
        params.push(anio);
        whereParts.push(`TO_CHAR(${almacenTsVzExpr}, 'YYYY') = $${params.length}`);
      }
      params.push(limit);

      const result = await pool.query(
        `WITH empa_reg AS (
           SELECT
             CONCAT('CAB-', ec.id_cabecera) AS codigo_lote,
             ec.numero_registro,
             STRING_AGG(DISTINCT UPPER(TRIM(ed.numero_lote)), ' | ' ORDER BY UPPER(TRIM(ed.numero_lote))) AS lote_referencia,
             SUM(ed.cantidad)::int AS cantidad_empaquetado,
             MAX(ec.fecha_hora) AS fecha_empaquetado,
             STRING_AGG(DISTINCT p.descripcion, ' | ' ORDER BY p.descripcion) AS productos
           FROM empaquetados_detalle ed
           JOIN empaquetados_cabecera ec ON ec.id_cabecera = ed.id_cabecera
           JOIN destinos d ON d.id_destino = ec.id_destino
           JOIN productos p ON p.id_producto = ed.id_producto
           WHERE TRIM(COALESCE(ed.numero_lote, '')) <> ''
             AND UPPER(TRIM(COALESCE(d.nombre, ''))) <> 'K FOOD'
           GROUP BY ec.id_cabecera, ec.numero_registro
         ),
         alm_lote AS (
           SELECT
             alp.codigo_lote,
             COALESCE(SUM((elem.value->>'cantidad_validada')::int), 0)::int AS cantidad_almacen
           FROM almacen_lotes_procesados alp
           LEFT JOIN LATERAL jsonb_array_elements(COALESCE(alp.resumen_validacion, '[]'::jsonb)) elem(value) ON true
           GROUP BY alp.codigo_lote
         )
         SELECT
           NULL::int AS "__ROW_ID",
           'Almacen09' AS "ORIGEN",
           TO_CHAR(${almacenTsVzExpr}, 'YYYY-MM-DD HH24:MI:SS') AS "Marca temporal",
           TO_CHAR(${almacenTsVzExpr}, 'YYYY-MM-DD') AS "FECHA",
           COALESCE(el.lote_referencia, alp.codigo_lote) AS "NUMERO DE LOTE",
           COALESCE(el.productos, CONCAT('REGISTRO ', COALESCE(el.numero_registro, '-'))) AS "PRODUCTO",
           COALESCE(el.cantidad_empaquetado, 0) AS "CANTIDAD EMPAQUETADO",
           TO_CHAR(${almacenTsVzExpr}, 'YYYY-MM-DD HH24:MI') AS "FECHA EMPAQUETADO",
           COALESCE(al.cantidad_almacen, 0) AS "CANTIDAD ALMACEN",
           TO_CHAR(${almacenTsVzExpr}, 'YYYY-MM-DD HH24:MI') AS "FECHA ENTRADA",
           alp.estado AS "ESTADO"
         FROM almacen_lotes_procesados alp
         LEFT JOIN empa_reg el ON UPPER(TRIM(el.codigo_lote)) = ${almacenCabCodigoExpr}
         LEFT JOIN alm_lote al ON al.codigo_lote = alp.codigo_lote
         WHERE ${whereParts.join(' AND ')}
         ORDER BY alp.processed_at DESC
         LIMIT $${params.length}`,
        params
      );

      const headers = result.rows.length ? Object.keys(result.rows[0]) : [];
      return res.json({ ok: true, sheet: 'Almacen09', headers, rows: result.rows, total: result.rows.length });
    }

    if (tipo === 'general') {
      const whereEmpa = [];
      const whereAlm = [`alp.estado = 'validado'`];
      const params = [];
      if (hasDesde) {
        params.push(desde);
        whereEmpa.push(`DATE(ec.fecha_hora) >= $${params.length}`);
        whereAlm.push(`DATE(${almacenTsVzExpr}) >= $${params.length}`);
      }
      if (hasHasta) {
        params.push(hasta);
        whereEmpa.push(`DATE(ec.fecha_hora) <= $${params.length}`);
        whereAlm.push(`DATE(${almacenTsVzExpr}) <= $${params.length}`);
      }
      if (hasSemana) {
        params.push(semana);
        whereEmpa.push(`TO_CHAR(ec.fecha_hora, 'IYYY-"W"IW') = $${params.length}`);
        whereAlm.push(`TO_CHAR(${almacenTsVzExpr}, 'IYYY-"W"IW') = $${params.length}`);
      }
      if (hasFecha) {
        params.push(fecha);
        whereEmpa.push(`DATE(ec.fecha_hora) = $${params.length}`);
        whereAlm.push(`DATE(${almacenTsVzExpr}) = $${params.length}`);
      }
      if (hasMes) {
        params.push(mes);
        whereEmpa.push(`TO_CHAR(ec.fecha_hora, 'YYYY-MM') = $${params.length}`);
        whereAlm.push(`TO_CHAR(${almacenTsVzExpr}, 'YYYY-MM') = $${params.length}`);
      }
      if (hasMesNumero) {
        params.push(mes);
        whereEmpa.push(`TO_CHAR(ec.fecha_hora, 'MM') = $${params.length}`);
        whereAlm.push(`TO_CHAR(${almacenTsVzExpr}, 'MM') = $${params.length}`);
      }
      if (hasAnio) {
        params.push(anio);
        whereEmpa.push(`TO_CHAR(ec.fecha_hora, 'YYYY') = $${params.length}`);
        whereAlm.push(`TO_CHAR(${almacenTsVzExpr}, 'YYYY') = $${params.length}`);
      }
      params.push(limit);

      const result = await pool.query(
        `WITH empa AS (
           SELECT
             ed.id_detalle AS "__ROW_ID",
             'Empaquetado' AS "ORIGEN",
             TO_CHAR(ec.fecha_hora, 'YYYY-MM-DD HH24:MI:SS') AS "Marca temporal",
             TO_CHAR(ec.fecha_hora, 'YYYY-MM-DD') AS "FECHA",
             ed.numero_lote AS "NUMERO DE LOTE",
             p.descripcion AS "PRODUCTO",
             ed.cantidad AS "CANTIDAD EMPAQUETADO",
             TO_CHAR(ec.fecha_hora, 'YYYY-MM-DD HH24:MI') AS "FECHA EMPAQUETADO",
             NULL::int AS "CANTIDAD ALMACEN",
             NULL::text AS "FECHA ENTRADA",
             'registrado'::text AS "ESTADO"
           FROM empaquetados_detalle ed
           JOIN empaquetados_cabecera ec ON ec.id_cabecera = ed.id_cabecera
            JOIN destinos d ON d.id_destino = ec.id_destino
           JOIN productos p ON p.id_producto = ed.id_producto
            ${([`UPPER(TRIM(COALESCE(d.nombre, ''))) <> 'K FOOD'`, ...whereEmpa]).length ? `WHERE ${[`UPPER(TRIM(COALESCE(d.nombre, ''))) <> 'K FOOD'`, ...whereEmpa].join(' AND ')}` : ''}
         ),
           empa_reg AS (
           SELECT
               CONCAT('CAB-', ec.id_cabecera) AS codigo_lote,
               ec.numero_registro,
             STRING_AGG(DISTINCT UPPER(TRIM(ed.numero_lote)), ' | ' ORDER BY UPPER(TRIM(ed.numero_lote))) AS lote_referencia,
             SUM(ed.cantidad)::int AS cantidad_empaquetado,
             MAX(ec.fecha_hora) AS fecha_empaquetado,
             STRING_AGG(DISTINCT p.descripcion, ' | ' ORDER BY p.descripcion) AS productos
           FROM empaquetados_detalle ed
           JOIN empaquetados_cabecera ec ON ec.id_cabecera = ed.id_cabecera
             JOIN destinos d ON d.id_destino = ec.id_destino
           JOIN productos p ON p.id_producto = ed.id_producto
           WHERE TRIM(COALESCE(ed.numero_lote, '')) <> ''
               AND UPPER(TRIM(COALESCE(d.nombre, ''))) <> 'K FOOD'
             GROUP BY ec.id_cabecera, ec.numero_registro
         ),
         alm_lote AS (
           SELECT
             alp.codigo_lote,
             COALESCE(SUM((elem.value->>'cantidad_validada')::int), 0)::int AS cantidad_almacen
           FROM almacen_lotes_procesados alp
           LEFT JOIN LATERAL jsonb_array_elements(COALESCE(alp.resumen_validacion, '[]'::jsonb)) elem(value) ON true
           GROUP BY alp.codigo_lote
         ),
         alm AS (
           SELECT
             NULL::int AS "__ROW_ID",
             'Almacen09' AS "ORIGEN",
             TO_CHAR(${almacenTsVzExpr}, 'YYYY-MM-DD HH24:MI:SS') AS "Marca temporal",
             TO_CHAR(${almacenTsVzExpr}, 'YYYY-MM-DD') AS "FECHA",
             COALESCE(el.lote_referencia, alp.codigo_lote) AS "NUMERO DE LOTE",
             COALESCE(el.productos, CONCAT('REGISTRO ', COALESCE(el.numero_registro, '-'))) AS "PRODUCTO",
             COALESCE(el.cantidad_empaquetado, 0) AS "CANTIDAD EMPAQUETADO",
             TO_CHAR(${almacenTsVzExpr}, 'YYYY-MM-DD HH24:MI') AS "FECHA EMPAQUETADO",
             COALESCE(al.cantidad_almacen, 0) AS "CANTIDAD ALMACEN",
             TO_CHAR(${almacenTsVzExpr}, 'YYYY-MM-DD HH24:MI') AS "FECHA ENTRADA",
             alp.estado AS "ESTADO"
           FROM almacen_lotes_procesados alp
           LEFT JOIN empa_reg el ON UPPER(TRIM(el.codigo_lote)) = ${almacenCabCodigoExpr}
           LEFT JOIN alm_lote al ON al.codigo_lote = alp.codigo_lote
           WHERE ${whereAlm.join(' AND ')}
         )
         SELECT * FROM (
           SELECT * FROM empa
           UNION ALL
           SELECT * FROM alm
         ) x
         ORDER BY "Marca temporal" DESC
         LIMIT $${params.length}`,
        params
      );

      const headers = result.rows.length ? Object.keys(result.rows[0]) : [];
      return res.json({ ok: true, sheet: 'General', headers, rows: result.rows, total: result.rows.length });
    }

    const whereParts = [];
    const params = [];
    if (hasDesde) {
      params.push(desde);
      whereParts.push(`DATE(ec.fecha_hora) >= $${params.length}`);
    }
    if (hasHasta) {
      params.push(hasta);
      whereParts.push(`DATE(ec.fecha_hora) <= $${params.length}`);
    }
    if (hasSemana) {
      params.push(semana);
      whereParts.push(`TO_CHAR(ec.fecha_hora, 'IYYY-"W"IW') = $${params.length}`);
    }
    if (hasFecha) {
      params.push(fecha);
      whereParts.push(`DATE(ec.fecha_hora) = $${params.length}`);
    }
    if (hasMes) {
      params.push(mes);
      whereParts.push(`TO_CHAR(ec.fecha_hora, 'YYYY-MM') = $${params.length}`);
    }
    if (hasMesNumero) {
      params.push(mes);
      whereParts.push(`TO_CHAR(ec.fecha_hora, 'MM') = $${params.length}`);
    }
    if (hasAnio) {
      params.push(anio);
      whereParts.push(`TO_CHAR(ec.fecha_hora, 'YYYY') = $${params.length}`);
    }
    params.push(limit);
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';

    const result = await pool.query(
      `SELECT
        ed.id_detalle AS "__ROW_ID",
        TO_CHAR(ec.fecha_hora, 'YYYY-MM-DD HH24:MI:SS') AS "Marca temporal",
        TO_CHAR(ec.fecha_hora, 'YYYY-MM-DD') AS "FECHA",
        p.descripcion AS "PRODUCTO",
        ed.cantidad AS "CANTIDAD",
        d.nombre AS "ENTREGADO A",
        ec.numero_registro AS "NUMERO REGISTRO",
        r.nombre_completo AS "RESPONSABLE",
        s.nombre AS "SEDE",
        ed.numero_lote AS "NUMERO DE LOTE"
      FROM empaquetados_detalle ed
      JOIN empaquetados_cabecera ec ON ec.id_cabecera = ed.id_cabecera
      JOIN productos p ON p.id_producto = ed.id_producto
      JOIN destinos d ON d.id_destino = ec.id_destino
      JOIN responsables r ON r.id_responsable = ec.id_responsable
      JOIN sedes s ON s.id_sede = ec.id_sede
      ${whereClause}
      ORDER BY ec.fecha_hora DESC, ed.id_detalle DESC
      LIMIT $${params.length}`,
      params
    );
    const headers = result.rows.length ? Object.keys(result.rows[0]) : [];
    res.json({ ok: true, sheet: 'Empaquetado', headers, rows: result.rows, total: result.rows.length });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.post('/api/registros/delete', async (req, res) => {
  const idsRaw = Array.isArray(req.body?.ids) ? req.body.ids : [];
  const ids = idsRaw.map((id) => Number(id)).filter((id) => Number.isInteger(id) && id > 0);

  if (!ids.length) {
    return res.status(400).json({ ok: false, error: 'ids es obligatorio y debe contener valores válidos' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const deleted = await client.query(
      `DELETE FROM empaquetados_detalle
       WHERE id_detalle = ANY($1::int[])
       RETURNING id_cabecera`,
      [ids]
    );

    const cabeceras = [...new Set(deleted.rows.map((row) => Number(row.id_cabecera)).filter(Boolean))];
    if (cabeceras.length) {
      await client.query(
        `DELETE FROM empaquetados_cabecera ec
         WHERE ec.id_cabecera = ANY($1::int[])
           AND NOT EXISTS (
             SELECT 1
             FROM empaquetados_detalle ed
             WHERE ed.id_cabecera = ec.id_cabecera
           )`,
        [cabeceras]
      );
    }

    await client.query('COMMIT');
    return res.json({ ok: true, deleted: deleted.rowCount || 0 });
  } catch (error) {
    await client.query('ROLLBACK');
    return res.status(500).json({ ok: false, error: error.message });
  } finally {
    client.release();
  }
});

app.get('/api/almacen09/lotes', async (_req, res) => {
  try {
    const result = await pool.query(
      `WITH detalle_agregado AS (
         SELECT
           CONCAT('CAB-', ec.id_cabecera) AS codigo_lote,
           ec.numero_registro,
           UPPER(TRIM(ed.numero_lote)) AS lote_referencia,
           ed.id_producto,
           SUM(ed.cantidad)::int AS cantidad,
           MAX(ec.fecha_hora) AS fecha_hora
         FROM empaquetados_detalle ed
         JOIN empaquetados_cabecera ec ON ec.id_cabecera = ed.id_cabecera
         JOIN destinos d ON d.id_destino = ec.id_destino
         WHERE TRIM(COALESCE(ed.numero_lote, '')) <> ''
           AND UPPER(TRIM(COALESCE(d.nombre, ''))) <> 'K FOOD'
         GROUP BY ec.id_cabecera, ec.numero_registro, UPPER(TRIM(ed.numero_lote)), ed.id_producto
       ),
       pendientes AS (
         SELECT da.*
         FROM detalle_agregado da
         LEFT JOIN almacen_lotes_procesados alp ON alp.codigo_lote = da.codigo_lote
         WHERE alp.codigo_lote IS NULL
       )
       SELECT
         p.codigo_lote,
         MAX(p.numero_registro) AS numero_registro,
         STRING_AGG(DISTINCT p.lote_referencia, ' | ' ORDER BY p.lote_referencia) AS lote_referencia,
         TO_CHAR(MAX(p.fecha_hora), 'DD/MM/YYYY HH24:MI') AS created_at,
         JSON_AGG(
           JSON_BUILD_OBJECT(
             'line_key', CONCAT(p.id_producto, '::', p.lote_referencia),
             'id', p.id_producto,
             'codigo', pr.codigo_producto,
             'descripcion', pr.descripcion,
             'lote_producto', p.lote_referencia,
             'cantidad', p.cantidad,
             'cestas_calculadas',
               CASE
                 WHEN COALESCE(pr.paquetes, 0) > 0
                   THEN CEIL(p.cantidad::numeric / pr.paquetes) + COALESCE(pr.sobre_piso, 0)
                 ELSE NULL
               END
           )
           ORDER BY p.lote_referencia, pr.codigo_producto
         ) AS productos
       FROM pendientes p
       JOIN productos pr ON pr.id_producto = p.id_producto
       GROUP BY p.codigo_lote
       ORDER BY MAX(p.fecha_hora) ASC`
    );

    res.json(result.rows);
  } catch (error) {
    res.status(500).send('Error al listar lotes pendientes');
  }
});

app.post('/api/almacen09/validar-conteo', async (req, res) => {
  const { codigo_lote, productos_y_cantidades } = req.body || {};

  if (!codigo_lote || !Array.isArray(productos_y_cantidades) || !productos_y_cantidades.length) {
    return res.status(400).send('Datos incompletos');
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const codigoRegistro = String(codigo_lote || '').trim().toUpperCase();
    const cabeceraMatch = /^CAB-(\d+)$/.exec(codigoRegistro);
    if (!cabeceraMatch) {
      await client.query('ROLLBACK');
      return res.status(400).send('Código de registro inválido');
    }
    const idCabecera = Number(cabeceraMatch[1]);

    const productosResult = await client.query(
      `SELECT
         CONCAT(ed.id_producto, '::', UPPER(TRIM(ed.numero_lote))) AS line_key,
         ed.id_producto AS id,
         p.codigo_producto AS codigo,
         p.descripcion,
         SUM(ed.cantidad)::int AS cantidad,
         CASE
           WHEN COALESCE(p.paquetes, 0) > 0
             THEN CEIL(SUM(ed.cantidad)::numeric / p.paquetes) + COALESCE(p.sobre_piso, 0)
           ELSE NULL
         END AS cestas_calculadas,
         STRING_AGG(DISTINCT UPPER(TRIM(ed.numero_lote)), ' | ' ORDER BY UPPER(TRIM(ed.numero_lote))) AS lote_producto
       FROM empaquetados_detalle ed
       JOIN empaquetados_cabecera ec ON ec.id_cabecera = ed.id_cabecera
       JOIN destinos d ON d.id_destino = ec.id_destino
       JOIN productos p ON p.id_producto = ed.id_producto
       WHERE ec.id_cabecera = $1
         AND TRIM(COALESCE(ed.numero_lote, '')) <> ''
         AND UPPER(TRIM(COALESCE(d.nombre, ''))) <> 'K FOOD'
       GROUP BY ed.id_producto, UPPER(TRIM(ed.numero_lote)), p.codigo_producto, p.descripcion, p.paquetes, p.sobre_piso
       ORDER BY UPPER(TRIM(ed.numero_lote)), p.codigo_producto`,
      [idCabecera]
    );

    if (productosResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).send('Lote no encontrado');
    }

    const cantidadesPorProductoId = new Map();
    const cantidadesPorCodigo = new Map();
    const cantidadesPorLinea = new Map();
    for (const item of productos_y_cantidades) {
      const cantidad = Number(item && item.cantidad);
      if (Number.isNaN(cantidad)) continue;

      const lineKey = item && item.line_key ? String(item.line_key).trim().toUpperCase() : '';
      if (lineKey) {
        cantidadesPorLinea.set(lineKey, cantidad);
        continue;
      }

      const productoId = Number(item && item.id);
      if (Number.isFinite(productoId) && productoId > 0) {
        cantidadesPorProductoId.set(productoId, cantidad);
        continue;
      }

      const codigo = item && item.codigo ? String(item.codigo).trim() : '';
      if (codigo) cantidadesPorCodigo.set(codigo, cantidad);
    }

    let hayMismatch = false;
    const productosValidados = [];
    for (const producto of productosResult.rows) {
      const recibido = cantidadesPorLinea.has(producto.line_key)
        ? cantidadesPorLinea.get(producto.line_key)
        : cantidadesPorProductoId.has(producto.id)
          ? cantidadesPorProductoId.get(producto.id)
          : cantidadesPorCodigo.get(producto.codigo);

      if (recibido === undefined || Number.isNaN(recibido) || Number(recibido) !== Number(producto.cantidad)) {
        hayMismatch = true;
        break;
      }

      productosValidados.push({
        id: producto.id,
        codigo: producto.codigo,
        cantidad: producto.cantidad,
        recibido,
      });
    }

    if (hayMismatch) {
      await client.query('ROLLBACK');
      await registrarErrorConteo(codigoRegistro);
      return res.status(400).send('ERROR: Las cantidades no coinciden con el registro de Empaquetado. CUENTE DE NUEVO');
    }

    const loteCodigo = codigoRegistro;
    const insertProcesado = await client.query(
      `INSERT INTO almacen_lotes_procesados (codigo_lote, estado)
       VALUES ($1, 'validado')
       ON CONFLICT (codigo_lote) DO NOTHING`,
      [loteCodigo]
    );

    if (!insertProcesado.rowCount) {
      await client.query('ROLLBACK');
      return res.status(409).send('Este lote ya fue procesado previamente.');
    }

    await client.query(
      `UPDATE almacen_lotes_procesados
       SET resumen_validacion = $2::jsonb,
           estado = 'validado',
           processed_at = NOW()
       WHERE codigo_lote = $1`,
      [
        loteCodigo,
        JSON.stringify(
          productosValidados.map((producto) => ({
            id_producto: Number(producto.id),
            codigo_producto: String(producto.codigo || '').trim().toUpperCase(),
            cantidad_validada: Number(producto.recibido),
          }))
        ),
      ]
    );

    await client.query('COMMIT');
    return res.json({ ok: true, message: 'Lote validado y registrado en PostgreSQL.' });
  } catch (error) {
    await client.query('ROLLBACK');
    return res.status(500).send(error.message || 'Error al validar lote');
  } finally {
    client.release();
  }
});

app.post('/api/almacen09/borrar-lotes', async (req, res) => {
  const { key } = req.body || {};
  if (!isValidAdminKey(key)) {
    return res.status(401).send('Clave inválida');
  }

  try {
    const result = await pool.query(
      `WITH detalle_agregado AS (
         SELECT CONCAT('CAB-', ec.id_cabecera) AS codigo_lote
         FROM empaquetados_detalle ed
         JOIN empaquetados_cabecera ec ON ec.id_cabecera = ed.id_cabecera
         JOIN destinos d ON d.id_destino = ec.id_destino
         WHERE TRIM(COALESCE(ed.numero_lote, '')) <> ''
           AND UPPER(TRIM(COALESCE(d.nombre, ''))) <> 'K FOOD'
         GROUP BY ec.id_cabecera
       ),
       pendientes AS (
         SELECT da.codigo_lote
         FROM detalle_agregado da
         LEFT JOIN almacen_lotes_procesados alp ON alp.codigo_lote = da.codigo_lote
         WHERE alp.codigo_lote IS NULL
       )
       INSERT INTO almacen_lotes_procesados (codigo_lote, estado)
       SELECT codigo_lote, 'descartado' FROM pendientes
       ON CONFLICT (codigo_lote) DO UPDATE SET estado = 'descartado', processed_at = NOW()
       RETURNING codigo_lote`
    );

    return res.json({ ok: true, total: result.rowCount || 0 });
  } catch (error) {
    return res.status(500).send('Error al descartar lotes');
  }
});

app.post('/api/almacen09/borrar-registros', async (req, res) => {
  const { key, codigos_lote } = req.body || {};
  if (!isValidAdminKey(key)) {
    return res.status(401).send('Clave inválida');
  }

  const codigos = Array.isArray(codigos_lote)
    ? codigos_lote.map((value) => String(value || '').trim().toUpperCase()).filter(Boolean)
    : [];

  if (!codigos.length) {
    return res.status(400).send('Codigos de lote requeridos');
  }

  try {
    const result = await pool.query(
      `INSERT INTO almacen_lotes_procesados (codigo_lote, estado)
       SELECT code, 'descartado'
       FROM UNNEST($1::text[]) AS t(code)
       ON CONFLICT (codigo_lote) DO UPDATE SET estado = 'descartado', processed_at = NOW()`,
      [codigos]
    );

    return res.json({ ok: true, total: result.rowCount || codigos.length });
  } catch (error) {
    return res.status(500).send('Error al descartar registros');
  }
});

app.get('/api/almacen09/errores-conteo', async (req, res) => {
  const { date, key } = req.query || {};
  if (!isValidAdminKey(key)) {
    return res.status(401).send('Clave inválida');
  }

  const targetDate = date ? String(date) : null;

  try {
    const params = [];
    let where = 'created_at::date = CURRENT_DATE';
    if (targetDate) {
      where = 'created_at::date = $1';
      params.push(targetDate);
    }

    const result = await pool.query(
      `WITH errores AS (
         SELECT id, codigo_lote, created_at
         FROM conteo_errores
         WHERE ${where}
       ),
       lotes_referencia AS (
         SELECT
           CONCAT('CAB-', ec.id_cabecera) AS codigo_lote,
           STRING_AGG(DISTINCT UPPER(TRIM(ed.numero_lote)), ' | ' ORDER BY UPPER(TRIM(ed.numero_lote))) AS lote_referencia
         FROM empaquetados_cabecera ec
         JOIN empaquetados_detalle ed ON ed.id_cabecera = ec.id_cabecera
         JOIN destinos d ON d.id_destino = ec.id_destino
         WHERE TRIM(COALESCE(ed.numero_lote, '')) <> ''
           AND UPPER(TRIM(COALESCE(d.nombre, ''))) <> 'K FOOD'
         GROUP BY ec.id_cabecera
       )
       SELECT
         e.id,
         e.codigo_lote,
         COALESCE(lr.lote_referencia, e.codigo_lote) AS codigo_mostrado,
         e.created_at
       FROM errores e
       LEFT JOIN lotes_referencia lr ON lr.codigo_lote = e.codigo_lote
       ORDER BY e.created_at DESC`,
      params
    );

    return res.json({ ok: true, total: result.rows.length, items: result.rows });
  } catch (error) {
    return res.status(500).send('Error al consultar errores');
  }
});

app.get('/api/almacen09/stock-actual', async (req, res) => {
  const desdeRaw = String(req.query?.desde || STOCK_RESET_DATE).trim();
  const desde = /^\d{4}-\d{2}-\d{2}$/.test(desdeRaw) ? desdeRaw : STOCK_RESET_DATE;

  try {
    const result = await pool.query(
      `SELECT
         p.codigo_producto,
         p.descripcion AS producto,
         UPPER(TRIM(ed.numero_lote)) AS numero_lote,
         TO_CHAR(DATE(ec.fecha_hora), 'YYYY-MM-DD') AS fecha_empaquetado,
         SUM(ed.cantidad)::int AS cantidad
       FROM almacen_lotes_procesados alp
       JOIN empaquetados_cabecera ec
         ON UPPER(TRIM(CONCAT('CAB-', ec.id_cabecera))) = UPPER(TRIM(SPLIT_PART(alp.codigo_lote, '::', 1)))
       JOIN destinos d ON d.id_destino = ec.id_destino
       JOIN empaquetados_detalle ed ON ed.id_cabecera = ec.id_cabecera
       JOIN productos p ON p.id_producto = ed.id_producto
       WHERE alp.estado = 'validado'
         AND DATE(alp.processed_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Caracas') >= $1
         AND TRIM(COALESCE(ed.numero_lote, '')) <> ''
         AND UPPER(TRIM(COALESCE(d.nombre, ''))) <> 'K FOOD'
       GROUP BY p.codigo_producto, p.descripcion, UPPER(TRIM(ed.numero_lote)), DATE(ec.fecha_hora)
       ORDER BY p.codigo_producto, UPPER(TRIM(ed.numero_lote)), DATE(ec.fecha_hora)`,
      [desde]
    );

    return res.json({ ok: true, desde, rows: result.rows, total: result.rows.length });
  } catch (error) {
    return res.status(500).json({ ok: false, error: 'Error al calcular stock actual desde Almacén09' });
  }
});

Promise.all([ensureAlmacen09Tables(), ensureProductosSoftDelete(), ensureHistoricoResultadosTable()])
  .then(() => {
    app.listen(port, () => {
      console.log(`Servidor escuchando en puerto ${port}`);
    });
  })
  .catch((error) => {
    console.error('No se pudieron preparar las tablas base:', error);
    process.exit(1);
  });
