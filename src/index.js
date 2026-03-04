import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { Pool } from 'pg';

dotenv.config();

const app = express();
const port = Number(process.env.PORT || 3001);
const adminKey = process.env.ADMIN_KEY || 'PASANTIAS90';

const rawCorsOrigin = String(process.env.CORS_ORIGIN || '').trim();
const corsOrigins = rawCorsOrigin
  ? rawCorsOrigin
      .split(',')
      .map((item) => item.trim().replace(/^['"]|['"]$/g, ''))
      .filter(Boolean)
  : ['*'];
const allowAnyOrigin = corsOrigins.includes('*') || corsOrigins.includes('https://*') || corsOrigins.includes('http://*');

app.use(
  cors({
    origin: (origin, callback) => {
      if (allowAnyOrigin || !origin) return callback(null, true);
      const isAllowed = corsOrigins.includes(origin);
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
    const result = await pool.query('SELECT id_sede, nombre FROM sedes ORDER BY nombre');
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.get('/responsables', async (_req, res) => {
  try {
    const result = await pool.query('SELECT id_responsable, nombre_completo FROM responsables ORDER BY nombre_completo');
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
      ORDER BY codigo_producto ASC
    `);
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/productos', async (req, res) => {
  const { codigo, descripcion, unidad, paquetes, sobre_piso, adminKey: sentKey } = req.body;
  if (sentKey !== adminKey) {
    return res.status(403).json({ ok: false, error: 'adminKey inválido' });
  }
  if (!codigo || !descripcion) {
    return res.status(400).json({ ok: false, error: 'codigo y descripcion son obligatorios' });
  }

  try {
    const result = await pool.query(
      `INSERT INTO productos (codigo_producto, descripcion, unidad_primaria, paquetes, sobre_piso)
       VALUES ($1, $2, $3, $4, $5)
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
  const sentKey = req.body?.adminKey || req.query?.adminKey;
  if (sentKey !== adminKey) {
    return res.status(403).json({ ok: false, error: 'adminKey inválido' });
  }

  try {
    const result = await pool.query(
      'DELETE FROM productos WHERE LOWER(codigo_producto) = LOWER($1) RETURNING id_producto',
      [codigo]
    );
    if (!result.rowCount) {
      return res.status(404).json({ ok: false, error: 'Producto no encontrado' });
    }
    res.json({ ok: true });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
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
  const tipo = String(req.query.tipo || 'General').trim().toLowerCase();
  const limit = Math.min(Math.max(Number(req.query.limit || 200), 1), 500);
  const fecha = String(req.query.fecha || '').trim();
  const mes = String(req.query.mes || '').trim();

  const hasFecha = /^\d{4}-\d{2}-\d{2}$/.test(fecha);
  const hasMes = /^\d{4}-\d{2}$/.test(mes);

  try {
    if (tipo === 'almacen09') {
      const whereParts = [`alp.estado = 'validado'`];
      const params = [];
      if (hasFecha) {
        params.push(fecha);
        whereParts.push(`DATE(alp.processed_at) = $${params.length}`);
      }
      if (hasMes) {
        params.push(mes);
        whereParts.push(`TO_CHAR(alp.processed_at, 'YYYY-MM') = $${params.length}`);
      }
      params.push(limit);

      const result = await pool.query(
        `WITH empa_reg AS (
           SELECT
             CONCAT('CAB-', ec.id_cabecera) AS codigo_lote,
             ec.numero_registro,
             SUM(ed.cantidad)::int AS cantidad_empaquetado,
             MAX(ec.fecha_hora) AS fecha_empaquetado,
             STRING_AGG(DISTINCT UPPER(TRIM(ed.numero_lote)), ' | ' ORDER BY UPPER(TRIM(ed.numero_lote))) AS lotes,
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
           alp.processed_at AS "Marca temporal",
           TO_CHAR(alp.processed_at, 'YYYY-MM-DD') AS "FECHA",
           COALESCE(el.lotes, alp.codigo_lote) AS "NUMERO DE LOTE",
           COALESCE(el.productos, CONCAT('REGISTRO ', COALESCE(el.numero_registro, '-'))) AS "PRODUCTO",
           COALESCE(el.cantidad_empaquetado, 0) AS "CANTIDAD EMPAQUETADO",
           TO_CHAR(el.fecha_empaquetado, 'YYYY-MM-DD HH24:MI') AS "FECHA EMPAQUETADO",
           COALESCE(al.cantidad_almacen, 0) AS "CANTIDAD ALMACEN",
           TO_CHAR(alp.processed_at, 'YYYY-MM-DD HH24:MI') AS "FECHA ENTRADA",
           alp.estado AS "ESTADO"
         FROM almacen_lotes_procesados alp
         LEFT JOIN empa_reg el ON el.codigo_lote = alp.codigo_lote
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
      if (hasFecha) {
        params.push(fecha);
        whereEmpa.push(`DATE(ec.fecha_hora) = $${params.length}`);
        whereAlm.push(`DATE(alp.processed_at) = $${params.length}`);
      }
      if (hasMes) {
        params.push(mes);
        whereEmpa.push(`TO_CHAR(ec.fecha_hora, 'YYYY-MM') = $${params.length}`);
        whereAlm.push(`TO_CHAR(alp.processed_at, 'YYYY-MM') = $${params.length}`);
      }
      params.push(limit);

      const result = await pool.query(
        `WITH empa AS (
           SELECT
             ed.id_detalle AS "__ROW_ID",
             'Empaquetado' AS "ORIGEN",
             ec.fecha_hora AS "Marca temporal",
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
             SUM(ed.cantidad)::int AS cantidad_empaquetado,
             MAX(ec.fecha_hora) AS fecha_empaquetado,
               STRING_AGG(DISTINCT UPPER(TRIM(ed.numero_lote)), ' | ' ORDER BY UPPER(TRIM(ed.numero_lote))) AS lotes,
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
             alp.processed_at AS "Marca temporal",
             TO_CHAR(alp.processed_at, 'YYYY-MM-DD') AS "FECHA",
             COALESCE(el.lotes, alp.codigo_lote) AS "NUMERO DE LOTE",
             COALESCE(el.productos, CONCAT('REGISTRO ', COALESCE(el.numero_registro, '-'))) AS "PRODUCTO",
             COALESCE(el.cantidad_empaquetado, 0) AS "CANTIDAD EMPAQUETADO",
             TO_CHAR(el.fecha_empaquetado, 'YYYY-MM-DD HH24:MI') AS "FECHA EMPAQUETADO",
             COALESCE(al.cantidad_almacen, 0) AS "CANTIDAD ALMACEN",
             TO_CHAR(alp.processed_at, 'YYYY-MM-DD HH24:MI') AS "FECHA ENTRADA",
             alp.estado AS "ESTADO"
           FROM almacen_lotes_procesados alp
           LEFT JOIN empa_reg el ON el.codigo_lote = alp.codigo_lote
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
    if (hasFecha) {
      params.push(fecha);
      whereParts.push(`DATE(ec.fecha_hora) = $${params.length}`);
    }
    if (hasMes) {
      params.push(mes);
      whereParts.push(`TO_CHAR(ec.fecha_hora, 'YYYY-MM') = $${params.length}`);
    }
    params.push(limit);
    const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';

    const result = await pool.query(
      `SELECT
        ed.id_detalle AS "__ROW_ID",
        ec.fecha_hora AS "Marca temporal",
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
           ed.id_producto,
           SUM(ed.cantidad)::int AS cantidad,
           MAX(ec.fecha_hora) AS fecha_hora,
           STRING_AGG(DISTINCT UPPER(TRIM(ed.numero_lote)), ' | ' ORDER BY UPPER(TRIM(ed.numero_lote))) AS lotes
         FROM empaquetados_detalle ed
         JOIN empaquetados_cabecera ec ON ec.id_cabecera = ed.id_cabecera
         JOIN destinos d ON d.id_destino = ec.id_destino
         WHERE TRIM(COALESCE(ed.numero_lote, '')) <> ''
           AND UPPER(TRIM(COALESCE(d.nombre, ''))) <> 'K FOOD'
         GROUP BY ec.id_cabecera, ec.numero_registro, ed.id_producto
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
         MAX(p.fecha_hora) AS created_at,
         JSON_AGG(
           JSON_BUILD_OBJECT(
             'id', p.id_producto,
             'codigo', pr.codigo_producto,
             'descripcion', pr.descripcion,
             'lote_producto', p.lotes,
             'cantidad', p.cantidad,
             'cestas_calculadas',
               CASE
                 WHEN COALESCE(pr.paquetes, 0) > 0
                   THEN CEIL(p.cantidad::numeric / pr.paquetes) + COALESCE(pr.sobre_piso, 0)
                 ELSE NULL
               END
           )
           ORDER BY pr.codigo_producto
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
         AND UPPER(TRIM(COALESCE(d.nombre, ''))) <> 'K FOOD'
       GROUP BY ed.id_producto, p.codigo_producto, p.descripcion, p.paquetes, p.sobre_piso
       ORDER BY p.codigo_producto`,
      [idCabecera]
    );

    if (productosResult.rows.length === 0) {
      await client.query('ROLLBACK');
      return res.status(404).send('Lote no encontrado');
    }

    const cantidadesPorProductoId = new Map();
    const cantidadesPorCodigo = new Map();
    for (const item of productos_y_cantidades) {
      const cantidad = Number(item && item.cantidad);
      if (Number.isNaN(cantidad)) continue;

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
      const recibido = cantidadesPorProductoId.has(producto.id)
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
       SET resumen_validacion = $2::jsonb
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
  if (!key || String(key).trim() !== String(adminKey).trim()) {
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
  if (!key || String(key).trim() !== String(adminKey).trim()) {
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
  if (!key || String(key).trim() !== String(adminKey).trim()) {
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
      `SELECT id, codigo_lote, created_at
       FROM conteo_errores
       WHERE ${where}
       ORDER BY created_at DESC`,
      params
    );

    return res.json({ ok: true, total: result.rows.length, items: result.rows });
  } catch (error) {
    return res.status(500).send('Error al consultar errores');
  }
});

ensureAlmacen09Tables()
  .then(() => {
    app.listen(port, () => {
      console.log(`Servidor escuchando en puerto ${port}`);
    });
  })
  .catch((error) => {
    console.error('No se pudieron preparar las tablas de Almacén 09:', error);
    process.exit(1);
  });
