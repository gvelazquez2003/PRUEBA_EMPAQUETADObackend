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

app.post('/api/mermas', async (req, res) => {
  const { cabecera, detalle } = req.body || {};
  if (!cabecera || !Array.isArray(detalle) || !detalle.length) {
    return res.status(400).json({ ok: false, error: 'cabecera y detalle son obligatorios' });
  }

  const timestamp = combineFechaHora(cabecera.fecha, cabecera.hora);
  if (!timestamp || !cabecera.id_responsable || !cabecera.id_sede) {
    return res.status(400).json({ ok: false, error: 'Faltan campos en cabecera' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const headerResult = await client.query(
      `INSERT INTO mermas_cabecera (fecha_hora, id_responsable, id_sede)
       VALUES ($1, $2, $3)
       RETURNING id_merma_cabecera`,
      [timestamp, Number(cabecera.id_responsable), Number(cabecera.id_sede)]
    );

    const idCabecera = headerResult.rows[0].id_merma_cabecera;
    for (const item of detalle) {
      if (!item.id_producto || !item.cantidad || !item.motivo || !item.numero_lote) {
        throw new Error('Cada item requiere id_producto, cantidad, motivo y numero_lote');
      }
      await client.query(
        `INSERT INTO mermas_detalle (id_merma_cabecera, id_producto, cantidad, motivo, numero_lote)
         VALUES ($1, $2, $3, $4, $5)`,
        [
          idCabecera,
          Number(item.id_producto),
          Number(item.cantidad),
          String(item.motivo).trim().toUpperCase(),
          String(item.numero_lote).trim().toUpperCase(),
        ]
      );
    }

    await client.query('COMMIT');
    res.status(201).json({ ok: true, id_merma_cabecera: idCabecera });
  } catch (error) {
    await client.query('ROLLBACK');
    res.status(500).json({ ok: false, error: error.message });
  } finally {
    client.release();
  }
});

app.get('/api/registros', async (req, res) => {
  const tipo = String(req.query.tipo || 'Empaquetado');
  const limit = Math.min(Math.max(Number(req.query.limit || 200), 1), 500);
  const fecha = String(req.query.fecha || '').trim();
  const mes = String(req.query.mes || '').trim();

  const hasFecha = /^\d{4}-\d{2}-\d{2}$/.test(fecha);
  const hasMes = /^\d{4}-\d{2}$/.test(mes);

  try {
    if (tipo.toLowerCase() === 'merma') {
      const whereParts = [];
      const params = [];
      if (hasFecha) {
        params.push(fecha);
        whereParts.push(`DATE(mc.fecha_hora) = $${params.length}`);
      }
      if (hasMes) {
        params.push(mes);
        whereParts.push(`TO_CHAR(mc.fecha_hora, 'YYYY-MM') = $${params.length}`);
      }
      params.push(limit);
      const whereClause = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
      const result = await pool.query(
        `SELECT
          md.id_merma_detalle AS "__ROW_ID",
          mc.fecha_hora AS "Marca temporal",
          TO_CHAR(mc.fecha_hora, 'YYYY-MM-DD') AS "FECHA",
          p.descripcion AS "PRODUCTO",
          p.unidad_primaria AS "UNIDAD DE MEDIDA",
          s.nombre AS "SEDE",
          md.motivo AS "MOTIVO DE MERMA",
          md.cantidad AS "CANTIDAD DEL MOTIVO DE MERMA",
          md.numero_lote AS "NUMERO DE LOTE",
          r.nombre_completo AS "RESPONSABLE"
        FROM mermas_detalle md
        JOIN mermas_cabecera mc ON mc.id_merma_cabecera = md.id_merma_cabecera
        JOIN productos p ON p.id_producto = md.id_producto
        JOIN responsables r ON r.id_responsable = mc.id_responsable
        JOIN sedes s ON s.id_sede = mc.id_sede
        ${whereClause}
        ORDER BY mc.fecha_hora DESC, md.id_merma_detalle DESC
        LIMIT $${params.length}`,
        params
      );
      const headers = result.rows.length ? Object.keys(result.rows[0]) : [];
      return res.json({ ok: true, sheet: 'Merma', headers, rows: result.rows, total: result.rows.length });
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
  const tipo = String(req.body?.tipo || 'Empaquetado');
  const idsRaw = Array.isArray(req.body?.ids) ? req.body.ids : [];
  const ids = idsRaw.map((id) => Number(id)).filter((id) => Number.isInteger(id) && id > 0);

  if (!ids.length) {
    return res.status(400).json({ ok: false, error: 'ids es obligatorio y debe contener valores válidos' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    if (tipo.toLowerCase() === 'merma') {
      const deleted = await client.query(
        `DELETE FROM mermas_detalle
         WHERE id_merma_detalle = ANY($1::int[])
         RETURNING id_merma_cabecera`,
        [ids]
      );

      const cabeceras = [...new Set(deleted.rows.map((row) => Number(row.id_merma_cabecera)).filter(Boolean))];
      if (cabeceras.length) {
        await client.query(
          `DELETE FROM mermas_cabecera mc
           WHERE mc.id_merma_cabecera = ANY($1::int[])
             AND NOT EXISTS (
               SELECT 1
               FROM mermas_detalle md
               WHERE md.id_merma_cabecera = mc.id_merma_cabecera
             )`,
          [cabeceras]
        );
      }

      await client.query('COMMIT');
      return res.json({ ok: true, deleted: deleted.rowCount || 0 });
    }

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

app.listen(port, () => {
  console.log(`Servidor escuchando en puerto ${port}`);
});
