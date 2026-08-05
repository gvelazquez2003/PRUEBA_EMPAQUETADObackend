import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const XLSX = require('xlsx');

const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const indexPath = path.join(repoRoot, 'src', 'index.js');

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function result(rows = []) {
  return { rows, rowCount: rows.length };
}

function makeResponse(status, body) {
  return {
    status,
    ok: status >= 200 && status < 300,
    async json() {
      if (body === Symbol.for('non-json')) throw new Error('invalid json');
      return body;
    },
  };
}

function dowOf(iso) {
  return new Date(`${String(iso).slice(0, 10)}T00:00:00Z`).getUTCDay();
}

function cloneState(state) {
  return {
    sessions: new Map(state.sessions),
    sedes: clone(state.sedes),
    productos: clone(state.productos),
    ventas: clone(state.ventas),
    predicciones: clone(state.predicciones),
    outbox: clone(state.outbox),
  };
}

class FakePgClient {
  constructor(pool) {
    this.pool = pool;
    this.txState = null;
    this.queries = [];
    this.released = false;
  }

  query(sql, params = []) {
    return this.pool._query(this.txState || this.pool.state, sql, params, this);
  }

  release() {
    this.released = true;
    this.pool.released += 1;
  }
}

class FakePgPool {
  constructor() {
    this.queries = [];
    this.released = 0;
    this.nextOutboxId = 1;
    this.state = {
      sessions: new Map([
        ['admin-token', { id_user: 1, username: 'ADMIN', role: 'administrador', activo: true }],
        ['ventas-token', { id_user: 4, username: 'VENTAS', role: 'ventas', activo: true }],
      ]),
      sedes: [
        { id_sede: 1, nombre: 'SL' },
        { id_sede: 2, nombre: 'LA GUAIRA' },
      ],
      productos: [
        { id_producto: 10, codigo_producto: 'PTEM0010', descripcion: 'Producto UND', unidad_primaria: 'UND', activo: true },
        { id_producto: 20, codigo_producto: 'PTSU0020', descripcion: 'Producto KG', unidad_primaria: 'KG', activo: true },
      ],
      ventas: [],
      predicciones: [],
      outbox: [],
    };
  }

  connect() {
    return Promise.resolve(new FakePgClient(this));
  }

  async query(sql, params = []) {
    return this._query(this.state, sql, params, null);
  }

  _record(sql, params, client) {
    const entry = { sql: String(sql), params: clone(params || []), client: Boolean(client) };
    this.queries.push(entry);
    if (client) client.queries.push(entry);
  }

  async _query(state, sql, params = [], client = null) {
    this._record(sql, params, client);
    const text = String(sql).replace(/\s+/g, ' ').trim();

    if (text === 'BEGIN') {
      client.txState = cloneState(state);
      return result();
    }
    if (text === 'COMMIT') {
      this.state = client.txState;
      client.txState = null;
      return result();
    }
    if (text === 'ROLLBACK') {
      client.txState = null;
      return result();
    }

    if (text.includes('FROM auth_sessions s JOIN auth_users u')) {
      const session = state.sessions.get(params[0]);
      return session ? result([{ token: params[0], ...session, full_name: session.username, vehicle_plate: '', nombre_sede: '' }]) : result();
    }
    if (text.startsWith('UPDATE auth_sessions SET last_seen_at')) return result([{ ok: true }]);

    if (text === 'SELECT id_sede, nombre FROM sedes') {
      return result(clone(state.sedes));
    }
    if (text.startsWith('SELECT id_sede, nombre FROM sedes ORDER BY')) {
      return result(clone(state.sedes).sort((a, b) => a.nombre.localeCompare(b.nombre)));
    }
    if (text === 'SELECT id_producto, codigo_producto FROM productos') {
      return result(clone(state.productos));
    }
    if (text.startsWith('SELECT id_producto, descripcion, unidad_primaria, codigo_producto FROM productos')) {
      return result(clone(state.productos));
    }

    if (text.startsWith('INSERT INTO ventas_diarias')) {
      const [fecha, sedeId, productoId, cantidad, precio] = params;
      const existing = state.ventas.find((v) => v.fecha === fecha && Number(v.sede_id) === Number(sedeId) && Number(v.producto_id) === Number(productoId));
      if (existing) {
        existing.cantidad = cantidad;
        existing.precio_unitario = precio;
        return result([{ creado: false }]);
      }
      state.ventas.push({ fecha, sede_id: sedeId, producto_id: productoId, cantidad, precio_unitario: precio });
      return result([{ creado: true }]);
    }

    if (text.startsWith('SELECT v.fecha,')) {
      const [desde, hasta] = params;
      return result(state.ventas
        .filter((v) => v.fecha >= desde && v.fecha <= hasta)
        .map((v) => {
          const sede = state.sedes.find((s) => Number(s.id_sede) === Number(v.sede_id));
          const producto = state.productos.find((p) => Number(p.id_producto) === Number(v.producto_id));
          return {
            fecha: v.fecha,
            cantidad: v.cantidad,
            precio_unitario: v.precio_unitario,
            id_sede: v.sede_id,
            sede_nombre: sede?.nombre || '',
            codigo_producto: producto?.codigo_producto || '',
            producto: producto?.descripcion || '',
          };
        })
        .sort((a, b) => a.fecha.localeCompare(b.fecha) || a.sede_nombre.localeCompare(b.sede_nombre) || a.codigo_producto.localeCompare(b.codigo_producto)));
    }

    if (text.startsWith('SELECT DISTINCT v.sede_id,')) {
      const [hasta] = params;
      const seen = new Set();
      const rows = [];
      for (const v of state.ventas) {
        if (v.fecha >= hasta) continue;
        const dow = dowOf(v.fecha);
        const key = `${v.sede_id}:${dow}:${v.fecha}`;
        if (seen.has(key)) continue;
        seen.add(key);
        rows.push({ sede_id: v.sede_id, dow, fecha: v.fecha });
      }
      rows.sort((a, b) => Number(a.sede_id) - Number(b.sede_id) || Number(a.dow) - Number(b.dow) || b.fecha.localeCompare(a.fecha));
      return result(rows);
    }

    if (text.startsWith('SELECT v.sede_id, v.producto_id,')) {
      const [hasta] = params;
      return result(state.ventas
        .filter((v) => v.fecha < hasta)
        .map((v) => ({ sede_id: v.sede_id, producto_id: v.producto_id, fecha: v.fecha, cantidad: v.cantidad }))
        .sort((a, b) => Number(a.sede_id) - Number(b.sede_id) || Number(a.producto_id) - Number(b.producto_id) || a.fecha.localeCompare(b.fecha)));
    }

    if (text.startsWith('SELECT COUNT(*)::int AS total FROM predicciones_demanda')) {
      const [desde, hasta] = params;
      return result([{ total: state.predicciones.filter((p) => p.fecha_objetivo >= desde && p.fecha_objetivo <= hasta).length }]);
    }

    if (text.startsWith('DELETE FROM predicciones_demanda')) {
      const [desde, hasta] = params;
      const before = state.predicciones.length;
      state.predicciones = state.predicciones.filter((p) => !(p.fecha_objetivo >= desde && p.fecha_objetivo <= hasta));
      return result([], before - state.predicciones.length);
    }

    if (text.startsWith('INSERT INTO predicciones_demanda')) {
      const [fechaObjetivo, sedeId, productoId, cantidad, version] = params;
      const existing = state.predicciones.find((p) => p.fecha_objetivo === fechaObjetivo && Number(p.sede_id) === Number(sedeId) && Number(p.producto_id) === Number(productoId));
      if (existing) {
        existing.cantidad_proyectada = cantidad;
        existing.version_modelo = version;
      } else {
        state.predicciones.push({ fecha_objetivo: fechaObjetivo, sede_id: sedeId, producto_id: productoId, cantidad_proyectada: cantidad, version_modelo: version });
      }
      return result([{ ok: true }]);
    }

    if (text.startsWith('INSERT INTO solicitudes_sedes_sheets_outbox')) {
      const [eventType, entityType, tipo, referencia, payload] = params;
      const existing = state.outbox.find((o) => o.event_type === eventType && o.referencia_externa === referencia);
      if (existing) {
        existing.payload = JSON.parse(payload);
        existing.estado = 'pendiente';
        existing.ultimo_error = null;
        return result([existing]);
      }
      const row = {
        id_sync: this.nextOutboxId++,
        event_type: eventType,
        entity_type: entityType,
        entity_id: null,
        tipo,
        referencia_externa: referencia,
        payload: JSON.parse(payload),
        estado: 'pendiente',
        intentos: 0,
      };
      state.outbox.push(row);
      return result([row]);
    }

    if (text.includes('WHERE event_type = $1 AND referencia_externa = $2')) {
      const [eventType, referencia] = params;
      return result(state.outbox.filter((o) => o.event_type === eventType && o.referencia_externa === referencia));
    }

    if (text.startsWith('UPDATE solicitudes_sedes_sheets_outbox SET')) {
      const eventId = params[0];
      const estado = params[1];
      const event = state.outbox.find((o) => Number(o.id_sync) === Number(eventId));
      if (!event) return result();
      event.estado = estado;
      event.intentos += 1;
      event.ultimo_error = params[2] || null;
      return result([event]);
    }

    throw new Error(`[fake-pool] consulta no manejada: ${text}`);
  }
}

async function setup(options = {}) {
  const pool = new FakePgPool();
  const fetchCalls = [];
  globalThis.__PDT_TEST_RUNTIME = { skipStart: true, pool };
  process.env.NODE_ENV = 'test';
  process.env.DATABASE_URL = '';
  process.env.SOLICITUDES_SHEETS_SYNC_ENABLED = options.sheetsEnabled === false ? 'false' : 'true';
  process.env.SOLICITUDES_SHEETS_WEBHOOK_URL = 'http://fake-sheets.local/webhook';
  const originalFetch = globalThis.fetch;
  const clientFetch = originalFetch;
  globalThis.fetch = async (url, init) => {
    fetchCalls.push({ url: String(url), body: init?.body ? JSON.parse(init.body) : null });
    return makeResponse(200, { ok: true });
  };
  const moduleUrl = `${pathToFileURL(indexPath).href}?case=${Date.now()}-${Math.random()}`;
  const { app } = await import(moduleUrl);
  const server = await new Promise((resolve) => {
    const instance = app.listen(0, () => resolve(instance));
  });
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  return {
    pool,
    baseUrl,
    clientFetch,
    fetchCalls,
    async request(method, route, body, token) {
      const response = await clientFetch(`${baseUrl}${route}`, {
        method,
        headers: {
          ...(body ? { 'Content-Type': 'application/json' } : {}),
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: body ? JSON.stringify(body) : undefined,
      });
      const payload = await response.json();
      return { status: response.status, payload };
    },
    async requestMultipart(route, buffer, filename, token) {
      const boundary = `----PDT${Date.now()}`;
      const chunks = [];
      chunks.push(Buffer.from(
        `--${boundary}\r\nContent-Disposition: form-data; name="archivo"; filename="${filename}"\r\n` +
        `Content-Type: application/vnd.openxmlformats-officedocument.spreadsheetml.sheet\r\n\r\n`
      ));
      chunks.push(buffer);
      chunks.push(Buffer.from(`\r\n--${boundary}--\r\n`));
      const response = await clientFetch(`${baseUrl}${route}`, {
        method: 'POST',
        headers: {
          'Content-Type': `multipart/form-data; boundary=${boundary}`,
          ...(token ? { Authorization: `Bearer ${token}` } : {}),
        },
        body: Buffer.concat(chunks),
      });
      const payload = await response.json();
      return { status: response.status, payload };
    },
    close() {
      if (typeof server.closeAllConnections === 'function') server.closeAllConnections();
      server.close();
      globalThis.fetch = originalFetch;
      delete globalThis.__PDT_TEST_RUNTIME;
    },
  };
}

function buildVentasWorkbook() {
  const wb = XLSX.utils.book_new();
  const ws = XLSX.utils.json_to_sheet([
    { sede: 'SL', semana: 1, 'dia semana': 'Lun', fecha: '2026-07-20', familia: 'PAN', 'codigo de barra': 'PTEM0010', producto: 'Producto UND', cantidad: 3, '%cantidad': 0.5, 'venta neta': 15 },
    { sede: 'LA GUAIRA', semana: 1, 'dia semana': 'Lun', fecha: '2026-07-20', familia: 'PAN', 'codigo de barra': 'PTSU0020', producto: 'Producto KG', cantidad: 2, '%cantidad': 1, 'venta neta': 20 },
    { sede: 'SEDE INEXISTENTE', semana: 1, 'dia semana': 'Lun', fecha: '2026-07-20', familia: 'PAN', 'codigo de barra': 'PTEM0010', producto: 'Producto UND', cantidad: 1, '%cantidad': 1, 'venta neta': 5 },
    { sede: 'SL', semana: 1, 'dia semana': 'Lun', fecha: '2026-07-20', familia: 'PAN', 'codigo de barra': 'NOEXISTE', producto: 'X', cantidad: 1, '%cantidad': 1, 'venta neta': 5 },
  ]);
  XLSX.utils.book_append_sheet(wb, ws, 'VENTAS');
  return XLSX.write(wb, { type: 'buffer', bookType: 'xlsx' });
}

function seedVentas(state) {
  const mondays = ['2026-06-29', '2026-07-06', '2026-07-13', '2026-07-20', '2026-07-27', '2026-08-03'];
  const cantidades = [100, 5, 4, 3, 2, 1];
  mondays.forEach((fecha, index) => {
    state.ventas.push({ fecha, sede_id: 1, producto_id: 10, cantidad: cantidades[index], precio_unitario: 5 });
  });
  ['2026-07-20', '2026-07-27', '2026-08-03'].forEach((fecha, index) => {
    state.ventas.push({ fecha, sede_id: 1, producto_id: 20, cantidad: 7 + index * 2, precio_unitario: 10 });
  });
  state.ventas.push({ fecha: '2026-08-02', sede_id: 1, producto_id: 10, cantidad: 8, precio_unitario: 5 });
  state.ventas.push({ fecha: '2026-08-03', sede_id: 2, producto_id: 10, cantidad: 50, precio_unitario: 5 });
}

test('ventas import parses Excel, upserts and reports omitted rows (admin only)', async () => {
  const ctx = await setup();
  try {
    assert.equal((await ctx.request('POST', '/api/ventas/import')).status, 401);
    assert.equal((await ctx.request('POST', '/api/ventas/import', {}, 'ventas-token')).status, 403);

    const workbook = buildVentasWorkbook();
    const response = await ctx.requestMultipart('/api/ventas/import', workbook, 'ventas.xlsx', 'admin-token');
    assert.equal(response.status, 200);
    assert.equal(response.payload.registrados, 2);
    assert.equal(response.payload.actualizados, 0);
    assert.equal(response.payload.omitidos_count, 2);
    assert.ok(response.payload.omitidos.some((o) => o.motivo === 'Sede no encontrada'));
    assert.ok(response.payload.omitidos.some((o) => o.motivo === 'Producto no encontrado'));
    assert.deepEqual(response.payload.familias_detectadas, ['PAN']);
    assert.equal(ctx.pool.state.ventas.length, 2);
    assert.equal(ctx.pool.state.ventas[0].precio_unitario, 5);
    assert.equal(ctx.pool.state.ventas[1].precio_unitario, 10);

    const reimport = await ctx.requestMultipart('/api/ventas/import', workbook, 'ventas.xlsx', 'admin-token');
    assert.equal(reimport.status, 200);
    assert.equal(reimport.payload.registrados, 0);
    assert.equal(reimport.payload.actualizados, 2);
    assert.equal(ctx.pool.state.ventas.length, 2);
  } finally {
    ctx.close();
  }
});

test('ventas export returns an xlsx download with week and totals', async () => {
  const ctx = await setup();
  try {
    seedVentas(ctx.pool.state);
    assert.equal((await ctx.request('GET', '/api/ventas/export?desde=2026-08-01&hasta=2026-08-31', null, 'ventas-token')).status, 403);
    const response = await ctx.clientFetch(`${ctx.baseUrl}/api/ventas/export?desde=2026-08-01&hasta=2026-08-31`, {
      method: 'GET',
      headers: { Authorization: 'Bearer admin-token' },
    });
    assert.equal(response.status, 200);
    assert.match(response.headers.get('content-type') || '', /spreadsheetml/);
    const workbook = XLSX.read(Buffer.from(await response.arrayBuffer()), { type: 'buffer' });
    const rows = XLSX.utils.sheet_to_json(workbook.Sheets[workbook.SheetNames[0]]);
    assert.equal(rows.length, 4);
    assert.deepEqual(rows.map((r) => r['Codigo de barra']).sort(), ['PTEM0010', 'PTEM0010', 'PTEM0010', 'PTSU0020']);
    assert.ok(rows.every((r) => /^\d{4}-\d{2}-\d{2}$/.test(r.Fecha)));
    assert.ok(rows.every((r) => Number.isInteger(r.Semana)));
    assert.ok(rows.every((r) => Number.isFinite(r['Venta neta']) && Number.isFinite(r['Venta total'])));
    const empty = await ctx.request('GET', '/api/ventas/export?desde=2025-01-01&hasta=2025-01-31', null, 'admin-token');
    assert.equal(empty.status, 404);
    assert.match(empty.payload.error, /no hay ventas/i);
    assert.equal((await ctx.request('GET', '/api/ventas/export?desde=bad&hasta=2026-08-31', null, 'admin-token')).status, 400);
  } finally {
    ctx.close();
  }
});

test('predicciones validar builds per-sede weekday coverage states', async () => {
  const ctx = await setup();
  try {
    seedVentas(ctx.pool.state);
    assert.equal((await ctx.request('GET', '/api/predicciones/validar?semana_inicio=2026-08-10', null, 'ventas-token')).status, 403);
    const response = await ctx.request('GET', '/api/predicciones/validar?semana_inicio=2026-08-10', null, 'admin-token');
    assert.equal(response.status, 200);
    assert.equal(response.payload.cobertura.length, 14);
    const lunSL = response.payload.cobertura.find((c) => c.sede_nombre === 'SL' && c.dia_semana === 'Lun');
    assert.equal(lunSL.estado, 'listo');
    assert.equal(lunSL.fechas.length, 6);
    const lunGuaira = response.payload.cobertura.find((c) => c.sede_nombre === 'LA GUAIRA' && c.dia_semana === 'Lun');
    assert.equal(lunGuaira.estado, 'baja_confianza');
    assert.equal(lunGuaira.fechas.length, 1);
    const marSL = response.payload.cobertura.find((c) => c.sede_nombre === 'SL' && c.dia_semana === 'Mar');
    assert.equal(marSL.estado, 'sin_datos');
    assert.equal((await ctx.request('GET', '/api/predicciones/validar?semana_inicio=2026-08-11', null, 'admin-token')).status, 400);
  } finally {
    ctx.close();
  }
});

test('predicciones generar uses trimmed mean per sede per product', async () => {
  const ctx = await setup();
  try {
    seedVentas(ctx.pool.state);
    const response = await ctx.request('POST', '/api/predicciones/generar', { semana_inicio: '2026-08-10' }, 'admin-token');
    assert.equal(response.status, 200);
    assert.deepEqual(response.payload.dias, ['Lun', 'Mar', 'Mie', 'Jue', 'Vie', 'Sab', 'Dom']);
    assert.equal(response.payload.filas.length, 3);

    const undSL = response.payload.filas.find((f) => f.sede_nombre === 'SL' && f.codigo === 'PTEM0010');
    assert.equal(undSL.dias.Lun, 3.5);
    assert.equal(undSL.dias.Dom, 8);
    assert.equal(undSL.dias.Mar, null);
    assert.equal(undSL.total, 11.5);

    const kgSL = response.payload.filas.find((f) => f.sede_nombre === 'SL' && f.codigo === 'PTSU0020');
    assert.equal(kgSL.dias.Lun, 9);
    assert.equal(kgSL.dias.Mar, null);

    const undGuaira = response.payload.filas.find((f) => f.sede_nombre === 'LA GUAIRA' && f.codigo === 'PTEM0010');
    assert.equal(undGuaira.dias.Lun, 50);
    assert.equal(undGuaira.dias.Dom, null);
  } finally {
    ctx.close();
  }
});

test('predicciones subir persists, enqueues outbox and handles collision/forzar', async () => {
  const ctx = await setup();
  try {
    seedVentas(ctx.pool.state);
    const response = await ctx.request('POST', '/api/predicciones/subir', { semana_inicio: '2026-08-10', modo: 'sobrescribir' }, 'admin-token');
    assert.equal(response.status, 200);
    assert.equal(response.payload.registros, 4);
    assert.equal(response.payload.sedes, 2);
    assert.equal(ctx.pool.state.predicciones.length, 4);
    assert.equal(ctx.pool.state.outbox.length, 1);
    assert.equal(ctx.pool.state.outbox[0].tipo, 'prediccion');
    assert.equal(ctx.pool.state.outbox[0].event_type, 'sync_prediccion_sheets');
    assert.equal(ctx.pool.state.outbox[0].estado, 'sincronizado');
    const sheetRow = ctx.fetchCalls[0].body.rows[0];
    assert.deepEqual(Object.keys(sheetRow).sort(), ['cantidad_proyectada', 'codigo', 'fecha', 'producto', 'sede']);
    assert.ok(response.payload.sheets.ok);

    const collision = await ctx.request('POST', '/api/predicciones/subir', { semana_inicio: '2026-08-10', modo: 'sobrescribir' }, 'admin-token');
    assert.equal(collision.status, 409);
    assert.equal(collision.payload.collision, true);
    assert.equal(ctx.pool.state.predicciones.length, 4);

    const forzar = await ctx.request('POST', '/api/predicciones/subir', { semana_inicio: '2026-08-10', modo: 'sobrescribir', forzar: true }, 'admin-token');
    assert.equal(forzar.status, 200);
    assert.equal(ctx.pool.state.predicciones.length, 4);
    assert.equal(ctx.pool.state.outbox.length, 1);
  } finally {
    ctx.close();
  }
});
