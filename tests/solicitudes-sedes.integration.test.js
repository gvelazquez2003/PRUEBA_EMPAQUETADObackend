import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

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

class FakePgPool {
  constructor(options = {}) {
    this.options = options;
    this.queries = [];
    this.released = 0;
    this.nextSolicitudId = 1;
    this.nextSolicitudDetalleId = 1;
    this.nextEntregaId = 1;
    this.nextEntregaDetalleId = 1;
    this.nextOutboxId = 1;
    this.locks = new Map();
    this.claimedOutbox = new Set();
    this.state = {
      sessions: new Map([
        ['admin-token', { id_user: 1, username: 'ADMIN', role: 'administrador', activo: true }],
        ['prod-token', { id_user: 2, username: 'PROD', role: 'produccion', activo: true }],
        ['almacen-token', { id_user: 3, username: 'ALMACEN', role: 'almacen', activo: true }],
        ['ventas-token', { id_user: 4, username: 'VENTAS', role: 'ventas', activo: true }],
      ]),
      sedes: [
        { id_sede: 1, nombre: 'SL' },
        { id_sede: 2, nombre: 'LA GUAIRA' },
      ],
      productos: [
        { id_producto: 10, codigo_producto: 'PTEM0010', descripcion: 'Producto UND', unidad_primaria: 'UND', activo: true },
        { id_producto: 20, codigo_producto: 'PTEM0020', descripcion: 'Producto KG', unidad_primaria: 'KG', activo: true },
        { id_producto: 30, codigo_producto: 'PTEM0030', descripcion: 'Producto sin unidad', unidad_primaria: null, activo: true },
        { id_producto: 40, codigo_producto: 'PTEM0040', descripcion: 'Producto inactivo', unidad_primaria: 'UND', activo: false },
      ],
      solicitudes: [],
      solicitudesDetalle: [],
      entregas: [],
      entregasDetalle: [],
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
      client.txState = cloneState(this.state);
      return result();
    }
    if (text === 'COMMIT') {
      this.state = client.txState;
      client.txState = null;
      client.releaseLocks();
      return result();
    }
    if (text === 'ROLLBACK') {
      client.txState = null;
      client.releaseLocks();
      return result();
    }

    if (text.includes('FROM auth_sessions s JOIN auth_users u')) {
      const session = state.sessions.get(params[0]);
      return session ? result([{ token: params[0], ...session, full_name: session.username }]) : result();
    }
    if (text.startsWith('UPDATE auth_sessions SET last_seen_at')) return result([{ ok: true }]);

    if (text.startsWith('SELECT id_sede, nombre FROM sedes WHERE id_sede')) {
      return result(state.sedes.filter((sede) => Number(sede.id_sede) === Number(params[0])));
    }
    if (text.startsWith('SELECT id_sede, nombre FROM sedes ORDER BY')) {
      return result([...state.sedes].sort((a, b) => a.nombre.localeCompare(b.nombre)));
    }

    if (text.includes('FROM productos') && text.includes('WHERE id_producto = ANY')) {
      const ids = new Set(params[0].map(Number));
      return result(state.productos.filter((product) => ids.has(Number(product.id_producto))));
    }
    if (text.startsWith('SELECT id_producto, codigo_producto, descripcion, unidad_primaria')) {
      const limit = Number(params[params.length - 1] || 250);
      const q = params.length > 1 ? String(params[0] || '').replace(/%/g, '').toLowerCase() : '';
      return result(state.productos
        .filter((product) => product.activo !== false)
        .filter((product) => !q || product.codigo_producto.toLowerCase().includes(q) || product.descripcion.toLowerCase().includes(q))
        .slice(0, limit));
    }
    if (text.startsWith('UPDATE productos SET unidad_primaria')) {
      const product = state.productos.find((item) => Number(item.id_producto) === Number(params[0]));
      if (!product) return result();
      product.unidad_primaria = params[1];
      return result([product]);
    }

    if (text.startsWith('INSERT INTO solicitudes_sedes (')) {
      const row = {
        id_solicitud: this.nextSolicitudId++,
        fecha: params[0],
        hora: params[1],
        sede_id: params[2],
        sede_nombre: params[3],
        responsable_nombre: params[4],
        responsable_email: params[5] || '',
        observaciones: params[6] || '',
        estado: 'PENDIENTE',
        referencia_externa: params[7],
        creado_por: params[8],
      };
      state.solicitudes.push(row);
      return result([{ id_solicitud: row.id_solicitud }]);
    }
    if (text.startsWith('INSERT INTO solicitudes_sedes_detalle')) {
      if (this.options.failSolicitudDetailAt && state.solicitudesDetalle.length + 1 === this.options.failSolicitudDetailAt) {
        throw new Error('detalle solicitud fallido');
      }
      const row = {
        id_detalle: this.nextSolicitudDetalleId++,
        solicitud_id: params[0],
        producto_id: params[1],
        codigo_producto: params[2],
        producto: params[3],
        familia: params[4],
        cantidad_solicitada: Number(params[5]),
        unidad_medida: params[6],
      };
      state.solicitudesDetalle.push(row);
      return result([row]);
    }

    if (text.startsWith('SELECT ss.id_solicitud,') && !text.includes('WHERE ss.id_solicitud = $1 LIMIT 1')) {
      return result(listSolicitudes(state, params));
    }
    if (text.startsWith('SELECT e.id_entrega,') && !text.includes('WHERE e.id_entrega = $1 LIMIT 1')) {
      return result(listEntregas(state, params));
    }

    if (text.startsWith('SELECT ss.id_solicitud') && text.includes('WHERE ss.id_solicitud = $1 LIMIT 1')) {
      const solicitud = state.solicitudes.find((item) => Number(item.id_solicitud) === Number(params[0]));
      return solicitud ? result([solicitud]) : result();
    }
    if (text.startsWith('SELECT d.id_detalle')) {
      const solicitudId = Number(params[0]);
      return result(state.solicitudesDetalle
        .filter((detail) => Number(detail.solicitud_id) === solicitudId)
        .map((detail) => ({
          ...detail,
          cantidad_entregada: sumDelivered(state, solicitudId, detail.producto_id),
        })));
    }
    if (text.startsWith('UPDATE solicitudes_sedes SET estado = $2')) {
      const solicitud = state.solicitudes.find((item) => Number(item.id_solicitud) === Number(params[0]));
      if (!solicitud) return result();
      solicitud.estado = params[1];
      return result([{ id_solicitud: solicitud.id_solicitud }]);
    }
    if (text.startsWith('WITH solicitado AS')) {
      const solicitudId = Number(params[0]);
      const details = state.solicitudesDetalle.filter((item) => Number(item.solicitud_id) === solicitudId);
      const totalEntregado = details.reduce((sum, detail) => sum + sumDelivered(state, solicitudId, detail.producto_id), 0);
      const completa = details.length > 0 && details.every((detail) => sumDelivered(state, solicitudId, detail.producto_id) >= Number(detail.cantidad_solicitada));
      return result([{ total_entregado: totalEntregado, completa }]);
    }

    if (text.includes('FROM solicitudes_sedes WHERE id_solicitud') && text.includes('FOR UPDATE')) {
      const solicitudId = Number(params[0]);
      await client.acquireLock(solicitudId);
      const solicitud = state.solicitudes.find((item) => Number(item.id_solicitud) === solicitudId);
      return solicitud ? result([solicitud]) : result();
    }
    if (text.startsWith('INSERT INTO entregas_sedes (')) {
      const row = {
        id_entrega: this.nextEntregaId++,
        solicitud_id: params[0] || null,
        fecha: params[1],
        hora: params[2],
        sede_id: params[3],
        sede_nombre: params[4],
        responsable_nombre: params[5],
        responsable_email: params[6] || '',
        observaciones: params[7] || '',
        referencia_externa: params[8],
        creado_por: params[9],
      };
      state.entregas.push(row);
      return result([{ id_entrega: row.id_entrega }]);
    }
    if (text.startsWith('INSERT INTO entregas_sedes_detalle')) {
      if (this.options.failEntregaDetailAt && state.entregasDetalle.length + 1 === this.options.failEntregaDetailAt) {
        throw new Error('detalle entrega fallido');
      }
      const row = {
        id_detalle: this.nextEntregaDetalleId++,
        entrega_id: params[0],
        producto_id: params[1],
        codigo_producto: params[2],
        producto: params[3],
        familia: params[4],
        cantidad_entregada: Number(params[5]),
        unidad_medida: params[6],
      };
      state.entregasDetalle.push(row);
      return result([row]);
    }
    if (text.startsWith('SELECT e.id_entrega') && text.includes('WHERE e.id_entrega = $1 LIMIT 1')) {
      const entrega = state.entregas.find((item) => Number(item.id_entrega) === Number(params[0]));
      return entrega ? result([entrega]) : result();
    }
    if (text.startsWith('SELECT id_detalle, producto_id, codigo_producto')) {
      return result(state.entregasDetalle.filter((detail) => Number(detail.entrega_id) === Number(params[0])));
    }

    if (text.startsWith('INSERT INTO solicitudes_sedes_sheets_outbox')) {
      if (this.options.failOutbox) throw new Error('outbox fallido');
      const eventType = params[0];
      const reference = params[4];
      let event = state.outbox.find((item) => item.event_type === eventType && item.referencia_externa === reference);
      if (!event) {
        event = {
          id_sync: this.nextOutboxId++,
          event_type: eventType,
          entity_type: params[1],
          entity_id: params[2],
          tipo: params[3],
          referencia_externa: reference,
          payload: typeof params[5] === 'string' ? JSON.parse(params[5]) : params[5],
          estado: 'pendiente',
          intentos: 0,
          ultimo_error: null,
        };
        state.outbox.push(event);
      } else {
        event.payload = typeof params[5] === 'string' ? JSON.parse(params[5]) : params[5];
        event.estado = 'pendiente';
        event.ultimo_error = null;
      }
      return result([event]);
    }
    if (text.startsWith('UPDATE solicitudes_sedes_sheets_outbox') && text.includes('intentos = intentos + 1')) {
      const event = state.outbox.find((item) => Number(item.id_sync) === Number(params[0]));
      if (event) {
        event.estado = params[1];
        event.ultimo_error = params[2] || null;
        event.intentos += 1;
      }
      return result(event ? [event] : []);
    }
    if (text.startsWith('SELECT id_sync, event_type')) {
      const rows = state.outbox
        .filter((event) => ['pendiente', 'error'].includes(event.estado))
        .filter((event) => !this.claimedOutbox.has(Number(event.id_sync)))
        .slice(0, Number(params[0]));
      rows.forEach((event) => this.claimedOutbox.add(Number(event.id_sync)));
      return result(rows);
    }
    if (text.startsWith('UPDATE solicitudes_sedes_sheets_outbox') && text.includes("estado = 'procesando'")) {
      const ids = new Set(params[0].map(Number));
      state.outbox.forEach((event) => {
        if (ids.has(Number(event.id_sync))) event.estado = 'procesando';
      });
      return result();
    }

    throw new Error(`Fake DB no maneja SQL: ${text}`);
  }
}

class FakePgClient {
  constructor(pool) {
    this.pool = pool;
    this.txState = null;
    this.queries = [];
    this.locked = [];
    this.released = false;
  }

  query(sql, params = []) {
    return this.pool._query(this.txState || this.pool.state, sql, params, this);
  }

  release() {
    this.released = true;
    this.releaseLocks();
    this.pool.released += 1;
  }

  async acquireLock(id) {
    while (this.pool.locks.has(id) && this.pool.locks.get(id) !== this) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    this.pool.locks.set(id, this);
    this.locked.push(id);
  }

  releaseLocks() {
    for (const id of this.locked.splice(0)) {
      if (this.pool.locks.get(id) === this) this.pool.locks.delete(id);
    }
  }
}

function cloneState(state) {
  return {
    sessions: new Map(state.sessions),
    sedes: clone(state.sedes),
    productos: clone(state.productos),
    solicitudes: clone(state.solicitudes),
    solicitudesDetalle: clone(state.solicitudesDetalle),
    entregas: clone(state.entregas),
    entregasDetalle: clone(state.entregasDetalle),
    outbox: clone(state.outbox),
  };
}

function sumDelivered(state, solicitudId, productId) {
  const entregas = state.entregas.filter((entrega) => Number(entrega.solicitud_id) === Number(solicitudId));
  const ids = new Set(entregas.map((entrega) => Number(entrega.id_entrega)));
  return state.entregasDetalle
    .filter((detail) => ids.has(Number(detail.entrega_id)) && Number(detail.producto_id) === Number(productId))
    .reduce((sum, detail) => sum + Number(detail.cantidad_entregada || 0), 0);
}

function listSolicitudes(state, params) {
  const limit = Number(params[params.length - 2] || 50);
  const offset = Number(params[params.length - 1] || 0);
  const rows = [...state.solicitudes].reverse().slice(offset, offset + limit);
  return rows.map((row) => ({ ...row, total: state.solicitudes.length }));
}

function listEntregas(state, params) {
  const limit = Number(params[params.length - 2] || 50);
  const offset = Number(params[params.length - 1] || 0);
  const rows = [...state.entregas].reverse().slice(offset, offset + limit);
  return rows.map((row) => ({ ...row, total: state.entregas.length }));
}

async function setup(options = {}) {
  const pool = new FakePgPool(options.poolOptions || {});
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
    if (options.sheetsResponse === 'non-json') return makeResponse(200, Symbol.for('non-json'));
    if (options.sheetsResponse === 'http-500') return makeResponse(500, { ok: false, error: 'boom' });
    if (options.sheetsResponse === 'timeout') throw new Error('timeout simulado');
    if (options.sheetsResponse === 'already') return makeResponse(200, { ok: true, already_processed: true });
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
    close() {
      server.close();
      globalThis.fetch = originalFetch;
      delete globalThis.__PDT_TEST_RUNTIME;
    },
  };
}

function validSolicitud(productos = [
  { producto_id: 10, cantidad_solicitada: 10, unidad_medida: 'UND' },
  { producto_id: 20, cantidad_solicitada: 5, unidad_medida: 'KG' },
]) {
  return {
    fecha: '2026-08-03',
    hora: '07:30',
    sede_id: 1,
    responsable_nombre: 'Responsable Test',
    productos,
  };
}

function validEntrega(solicitudId, productos = [
  { producto_id: 10, cantidad_entregada: 4, unidad_medida: 'UND' },
]) {
  return {
    solicitud_id: solicitudId,
    fecha: '2026-08-03',
    hora: '09:00',
    sede_id: 1,
    responsable_nombre: 'Almacen Test',
    productos,
  };
}

test('auth and permissions are enforced by real endpoints', async () => {
  const ctx = await setup();
  try {
    assert.equal((await ctx.request('GET', '/api/solicitudes-sedes/catalogos/sedes')).status, 401);
    assert.equal((await ctx.request('GET', '/api/solicitudes-sedes/catalogos/sedes', null, 'bad-token')).status, 401);
    assert.equal((await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud(), 'ventas-token')).status, 403);
    assert.equal((await ctx.request('GET', '/api/solicitudes-sedes/catalogos/sedes', null, 'admin-token')).status, 200);
    assert.equal((await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud(), 'prod-token')).status, 201);
    assert.equal((await ctx.request('POST', '/api/entregas-sedes', validEntrega(1), 'almacen-token')).status, 201);
    assert.equal((await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud(), 'almacen-token')).status, 403);
  } finally {
    ctx.close();
  }
});

test('catalog endpoints list sedes/products and update product unit with validation', async () => {
  const ctx = await setup();
  try {
    const sedes = await ctx.request('GET', '/api/solicitudes-sedes/catalogos/sedes', null, 'prod-token');
    assert.equal(sedes.status, 200);
    assert.equal(sedes.payload.sedes.length, 2);
    const products = await ctx.request('GET', '/api/solicitudes-sedes/catalogos/productos?limit=10', null, 'prod-token');
    assert.equal(products.status, 200);
    assert.equal(products.payload.productos.some((p) => p.unidad_primaria === null), true);
    assert.equal(products.payload.productos.some((p) => p.id_producto === 40), false);
    assert.equal((await ctx.request('PATCH', '/api/solicitudes-sedes/catalogos/productos/30/unidad', { unidad: 'BAD' }, 'almacen-token')).status, 400);
    const updated = await ctx.request('PATCH', '/api/solicitudes-sedes/catalogos/productos/30/unidad', { unidad: 'LTS' }, 'almacen-token');
    assert.equal(updated.status, 200);
    assert.equal(updated.payload.producto.unidad_primaria, 'LT');
  } finally {
    ctx.close();
  }
});

test('solicitudes create header, details and outbox in one transaction', async () => {
  const ctx = await setup();
  try {
    const response = await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud(), 'prod-token');
    assert.equal(response.status, 201);
    assert.equal(ctx.pool.state.solicitudes.length, 1);
    assert.equal(ctx.pool.state.solicitudesDetalle.length, 2);
    assert.equal(ctx.pool.state.outbox.length, 1);
    const txOrder = ctx.pool.queries.filter((q) => q.client).map((q) => q.sql.replace(/\s+/g, ' ').trim());
    assert.equal(txOrder[0], 'BEGIN');
    assert.match(txOrder[1], /^SELECT id_sede/);
    assert.match(txOrder[3], /^INSERT INTO solicitudes_sedes/);
    assert.match(txOrder[4], /^INSERT INTO solicitudes_sedes_detalle/);
    assert.match(txOrder[5], /^INSERT INTO solicitudes_sedes_detalle/);
    assert.match(txOrder[6], /^INSERT INTO solicitudes_sedes_sheets_outbox/);
    assert.equal(txOrder[7], 'COMMIT');
    assert.equal(ctx.pool.released, 1);
  } finally {
    ctx.close();
  }
});

test('solicitudes reject invalid payloads with controlled 400 responses', async () => {
  const ctx = await setup();
  try {
    assert.equal((await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([]), 'prod-token')).status, 400);
    assert.equal((await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([
      { producto_id: 10, cantidad_solicitada: 1, unidad_medida: 'UND' },
      { producto_id: 10, cantidad_solicitada: 2, unidad_medida: 'UND' },
    ]), 'prod-token')).status, 400);
    assert.equal((await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([{ producto_id: 10, cantidad_solicitada: 0, unidad_medida: 'UND' }]), 'prod-token')).status, 400);
    assert.equal((await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([{ producto_id: 10, cantidad_solicitada: -1, unidad_medida: 'UND' }]), 'prod-token')).status, 400);
    assert.equal((await ctx.request('POST', '/api/solicitudes-sedes', { ...validSolicitud(), sede_id: 999 }, 'prod-token')).status, 400);
    assert.equal((await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([{ producto_id: 999, cantidad_solicitada: 1, unidad_medida: 'UND' }]), 'prod-token')).status, 400);
    assert.equal((await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([{ producto_id: 30, cantidad_solicitada: 1 }]), 'prod-token')).status, 400);
    assert.equal((await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([{ producto_id: 30, cantidad_solicitada: 1, unidad_medida: 'CAJ' }]), 'prod-token')).status, 201);
  } finally {
    ctx.close();
  }
});

test('solicitudes rollback when a detail or outbox fails', async () => {
  const ctxDetail = await setup({ poolOptions: { failSolicitudDetailAt: 2 } });
  try {
    const response = await ctxDetail.request('POST', '/api/solicitudes-sedes', validSolicitud(), 'prod-token');
    assert.equal(response.status, 500);
    assert.equal(ctxDetail.pool.state.solicitudes.length, 0);
    assert.equal(ctxDetail.pool.state.solicitudesDetalle.length, 0);
    assert.equal(ctxDetail.pool.queries.some((q) => q.sql === 'ROLLBACK'), true);
    assert.equal(ctxDetail.pool.released, 1);
  } finally {
    ctxDetail.close();
  }

  const ctxOutbox = await setup({ poolOptions: { failOutbox: true } });
  try {
    const response = await ctxOutbox.request('POST', '/api/solicitudes-sedes', validSolicitud(), 'prod-token');
    assert.equal(response.status, 500);
    assert.equal(ctxOutbox.pool.state.solicitudes.length, 0);
    assert.equal(ctxOutbox.pool.state.outbox.length, 0);
    assert.equal(ctxOutbox.pool.queries.some((q) => q.sql === 'ROLLBACK'), true);
  } finally {
    ctxOutbox.close();
  }
});

test('entregas update solicitud state from partial to completed and prevent negative pending', async () => {
  const ctx = await setup();
  try {
    const solicitud = await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([
      { producto_id: 10, cantidad_solicitada: 10, unidad_medida: 'UND' },
    ]), 'prod-token');
    assert.equal(solicitud.status, 201);
    const id = solicitud.payload.solicitud.id_solicitud;
    const parcial = await ctx.request('POST', '/api/entregas-sedes', validEntrega(id, [
      { producto_id: 10, cantidad_entregada: 4, unidad_medida: 'UND' },
    ]), 'almacen-token');
    assert.equal(parcial.status, 201);
    assert.equal(parcial.payload.solicitud_estado, 'PARCIAL');
    const completa = await ctx.request('POST', '/api/entregas-sedes', validEntrega(id, [
      { producto_id: 10, cantidad_entregada: 9, unidad_medida: 'UND' },
    ]), 'almacen-token');
    assert.equal(completa.status, 201);
    assert.equal(completa.payload.solicitud_estado, 'COMPLETADA');
    const detail = await ctx.request('GET', `/api/solicitudes-sedes/${id}`, null, 'prod-token');
    assert.equal(detail.payload.solicitud.estado, 'COMPLETADA');
    assert.equal(detail.payload.solicitud.productos[0].cantidad_entregada, 13);
    assert.equal(detail.payload.solicitud.productos[0].cantidad_pendiente, 0);
  } finally {
    ctx.close();
  }
});

test('entregas reject missing/cancelled/completed/different-sede solicitudes and rollback details', async () => {
  const ctx = await setup();
  try {
    assert.equal((await ctx.request('POST', '/api/entregas-sedes', validEntrega(999), 'almacen-token')).status, 404);
    const created = await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([{ producto_id: 10, cantidad_solicitada: 2, unidad_medida: 'UND' }]), 'prod-token');
    const id = created.payload.solicitud.id_solicitud;
    assert.equal((await ctx.request('POST', '/api/entregas-sedes', { ...validEntrega(id), sede_id: 2 }, 'almacen-token')).status, 409);
    assert.equal((await ctx.request('PATCH', `/api/solicitudes-sedes/${id}/estado`, { estado: 'CANCELADA' }, 'admin-token')).status, 200);
    assert.equal((await ctx.request('POST', '/api/entregas-sedes', validEntrega(id), 'almacen-token')).status, 409);
  } finally {
    ctx.close();
  }

  const ctxRollback = await setup({ poolOptions: { failEntregaDetailAt: 1 } });
  try {
    const created = await ctxRollback.request('POST', '/api/solicitudes-sedes', validSolicitud([{ producto_id: 10, cantidad_solicitada: 2, unidad_medida: 'UND' }]), 'prod-token');
    const response = await ctxRollback.request('POST', '/api/entregas-sedes', validEntrega(created.payload.solicitud.id_solicitud), 'almacen-token');
    assert.equal(response.status, 500);
    assert.equal(ctxRollback.pool.state.entregas.length, 0);
    assert.equal(ctxRollback.pool.state.entregasDetalle.length, 0);
    assert.equal(ctxRollback.pool.queries.some((q) => q.sql.includes('FOR UPDATE')), true);
    assert.equal(ctxRollback.pool.queries.some((q) => q.sql === 'ROLLBACK'), true);
  } finally {
    ctxRollback.close();
  }
});

test('concurrent entregas keep final state consistent', async () => {
  const ctx = await setup();
  try {
    const created = await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([{ producto_id: 10, cantidad_solicitada: 10, unidad_medida: 'UND' }]), 'prod-token');
    const id = created.payload.solicitud.id_solicitud;
    const [a, b] = await Promise.all([
      ctx.request('POST', '/api/entregas-sedes', validEntrega(id, [{ producto_id: 10, cantidad_entregada: 4, unidad_medida: 'UND' }]), 'almacen-token'),
      ctx.request('POST', '/api/entregas-sedes', validEntrega(id, [{ producto_id: 10, cantidad_entregada: 6, unidad_medida: 'UND' }]), 'almacen-token'),
    ]);
    assert.equal(a.status, 201);
    assert.equal(b.status, 201);
    const detail = await ctx.request('GET', `/api/solicitudes-sedes/${id}`, null, 'admin-token');
    assert.equal(detail.payload.solicitud.estado, 'COMPLETADA');
    assert.equal(detail.payload.solicitud.productos[0].cantidad_entregada, 10);
    assert.equal(ctx.pool.queries.filter((q) => q.sql.includes('FOR UPDATE')).length, 2);
  } finally {
    ctx.close();
  }
});

test('sheets outbox marks success, error, retry success and avoids duplicated references', async () => {
  const ctx = await setup({ sheetsResponse: 'http-500' });
  try {
    const created = await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([{ producto_id: 10, cantidad_solicitada: 1, unidad_medida: 'UND' }]), 'prod-token');
    assert.equal(created.status, 201);
    assert.equal(ctx.pool.state.solicitudes.length, 1);
    assert.equal(ctx.pool.state.outbox.length, 1);
    assert.equal(ctx.pool.state.outbox[0].estado, 'error');
    assert.equal(created.payload.sheets.ok, false);
    assert.equal(JSON.stringify(created.payload).includes('secret'), false);

    ctx.pool.options.failOutbox = false;
    ctx.fetchCalls.length = 0;
    const originalFetch = globalThis.fetch;
    globalThis.fetch = async (url, init) => {
      ctx.fetchCalls.push({ url: String(url), body: JSON.parse(init.body) });
      return makeResponse(200, { ok: true });
    };
    const retry = await ctx.request('POST', '/api/solicitudes-sedes/sheets/retry', { limit: 5 }, 'almacen-token');
    globalThis.fetch = originalFetch;
    assert.equal(retry.status, 200);
    assert.equal(ctx.pool.state.outbox[0].estado, 'sincronizado');
    assert.equal(ctx.pool.state.outbox[0].intentos, 2);
    assert.equal(ctx.pool.state.outbox.length, 1);
    assert.equal((await ctx.request('POST', '/api/solicitudes-sedes/sheets/retry', { limit: 5 }, 'prod-token')).status, 403);
  } finally {
    ctx.close();
  }
});

test('sheets disabled keeps PostgreSQL saved without calling HTTP adapter', async () => {
  const ctx = await setup({ sheetsEnabled: false });
  try {
    const created = await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([{ producto_id: 10, cantidad_solicitada: 1, unidad_medida: 'UND' }]), 'prod-token');
    assert.equal(created.status, 201);
    assert.equal(ctx.pool.state.solicitudes.length, 1);
    assert.equal(ctx.pool.state.outbox.length, 1);
    assert.equal(ctx.pool.state.outbox[0].estado, 'pendiente');
    assert.equal(created.payload.sheets.skipped, true);
    assert.equal(ctx.fetchCalls.length, 0);
    const retry = await ctx.request('POST', '/api/solicitudes-sedes/sheets/retry', { limit: 1 }, 'almacen-token');
    assert.equal(retry.status, 200);
    assert.equal(retry.payload.skipped, true);
    assert.equal(ctx.pool.state.outbox[0].estado, 'pendiente');
  } finally {
    ctx.close();
  }
});

test('sheets adapter accepts already-processed response without duplicating outbox', async () => {
  const ctx = await setup({ sheetsResponse: 'already' });
  try {
    const created = await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([{ producto_id: 10, cantidad_solicitada: 1, unidad_medida: 'UND' }]), 'prod-token');
    assert.equal(created.status, 201);
    assert.equal(created.payload.sheets.ok, true);
    assert.equal(created.payload.sheets.data.already_processed, true);
    assert.equal(ctx.pool.state.outbox.length, 1);
    assert.equal(ctx.pool.state.outbox[0].estado, 'sincronizado');
  } finally {
    ctx.close();
  }
});

test('two retry processors do not claim the same event simultaneously', async () => {
  const ctx = await setup({ sheetsResponse: 'timeout' });
  try {
    await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([{ producto_id: 10, cantidad_solicitada: 1, unidad_medida: 'UND' }]), 'prod-token');
    const [a, b] = await Promise.all([
      ctx.request('POST', '/api/solicitudes-sedes/sheets/retry', { limit: 1 }, 'almacen-token'),
      ctx.request('POST', '/api/solicitudes-sedes/sheets/retry', { limit: 1 }, 'almacen-token'),
    ]);
    assert.equal(a.status, 200);
    assert.equal(b.status, 200);
    assert.equal(a.payload.total + b.payload.total, 1);
  } finally {
    ctx.close();
  }
});

test('get detail, filters, pagination and not found responses', async () => {
  const ctx = await setup({ sheetsResponse: 'non-json' });
  try {
    const created = await ctx.request('POST', '/api/solicitudes-sedes', validSolicitud([{ producto_id: 10, cantidad_solicitada: 1, unidad_medida: 'UND' }]), 'prod-token');
    const solicitudId = created.payload.solicitud.id_solicitud;
    const entrega = await ctx.request('POST', '/api/entregas-sedes', validEntrega(solicitudId), 'almacen-token');
    assert.equal((await ctx.request('GET', `/api/solicitudes-sedes/${solicitudId}`, null, 'admin-token')).payload.solicitud.productos.length, 1);
    assert.equal((await ctx.request('GET', `/api/entregas-sedes/${entrega.payload.entrega.id_entrega}`, null, 'admin-token')).payload.entrega.productos.length, 1);
    const filtered = await ctx.request('GET', '/api/solicitudes-sedes?sede_id=1&fecha_desde=2026-08-01&fecha_hasta=2026-08-31&estado=PARCIAL&limit=999', null, 'admin-token');
    assert.equal(filtered.status, 200);
    assert.equal(filtered.payload.pagination.limit, 200);
    assert.equal((await ctx.request('GET', '/api/solicitudes-sedes/9999', null, 'admin-token')).status, 404);
    assert.equal((await ctx.request('GET', '/api/entregas-sedes/9999', null, 'admin-token')).status, 404);
  } finally {
    ctx.close();
  }
});
