import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { Pool } from 'pg';
import crypto from 'crypto';

dotenv.config();

const app = express();
const port = Number(process.env.PORT || 3001);
const adminKey = process.env.ADMIN_KEY || '#FANDETATA';
const legacyAdminKey = '#FANDETATA';
const STOCK_RESET_DATE = String(process.env.STOCK_RESET_DATE || '2026-03-30').trim();
const AUTH_SESSION_TTL_HOURS = Number(process.env.AUTH_SESSION_TTL_HOURS || 168);
const APP_ROLES = {
  ADMIN: 'administrador',
  EMPAQUETADO: 'empaquetado',
  ALMACEN: 'almacen',
};
const INITIAL_ADMIN_USERNAMES = ['ATovar', 'EValerio', 'LGil'];
const INITIAL_ADMIN_PASSWORD = String(process.env.INITIAL_ADMIN_PASSWORD || 'Admin12345');

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

function originMatchesRule(rule, origin) {
  if (!rule || !origin) return false;
  if (rule === origin) return true;
  if (rule.includes('*')) {
    const escaped = rule.replace(/[.+?^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*');
    return new RegExp(`^${escaped}$`, 'i').test(origin);
  }
  return false;
}

function isAllowedOrigin(origin) {
  if (allowAnyOrigin || !origin) return true;
  const normalizedOrigin = normalizeOrigin(origin);
  if (!normalizedOrigin) return true;

  // "null" origin is common when opening static files directly.
  if (normalizedOrigin === 'null') return true;

  if (corsOrigins.some((rule) => originMatchesRule(rule, normalizedOrigin))) {
    return true;
  }

  try {
    const parsed = new URL(normalizedOrigin);
    const host = String(parsed.hostname || '').toLowerCase();
    if (host === 'localhost' || host === '127.0.0.1') return true;
    if (host.endsWith('.vercel.app')) return true;
  } catch (_) {}

  return false;
}

app.use(
  cors({
    origin: (origin, callback) => {
      const isAllowed = isAllowedOrigin(origin);
      return callback(null, isAllowed);
    },
    methods: ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization'],
  })
);
app.options('*', cors());

app.use((req, res, next) => {
  const requestOrigin = req.headers.origin;
  if (requestOrigin && isAllowedOrigin(requestOrigin)) {
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

function normalizeAuthRole(value) {
  const role = String(value || '').trim().toLowerCase();
  if (role === APP_ROLES.ADMIN) return APP_ROLES.ADMIN;
  if (role === APP_ROLES.EMPAQUETADO) return APP_ROLES.EMPAQUETADO;
  if (role === APP_ROLES.ALMACEN) return APP_ROLES.ALMACEN;
  return '';
}

function normalizeAuthUsername(value) {
  return String(value || '')
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, '')
    .slice(0, 10);
}

function hashPassword(password) {
  const clean = String(password || '');
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(clean, salt, 64).toString('hex');
  return `scrypt:${salt}:${hash}`;
}

function verifyPassword(password, storedHash) {
  const cleanStored = String(storedHash || '').trim();
  if (!cleanStored) return false;

  if (cleanStored.startsWith('plain:')) {
    return cleanStored === `plain:${String(password || '')}`;
  }

  if (!cleanStored.startsWith('scrypt:')) return false;
  const parts = cleanStored.split(':');
  if (parts.length !== 3) return false;

  const salt = parts[1];
  const hash = parts[2];
  const computed = crypto.scryptSync(String(password || ''), salt, 64).toString('hex');

  try {
    return crypto.timingSafeEqual(Buffer.from(hash, 'hex'), Buffer.from(computed, 'hex'));
  } catch (_) {
    return false;
  }
}

function getRequestToken(req) {
  const authHeader = String(req.headers.authorization || '').trim();
  const match = /^Bearer\s+(.+)$/i.exec(authHeader);
  if (match && match[1]) return String(match[1]).trim();

  const queryToken = String(req.query?.token || '').trim();
  if (queryToken) return queryToken;

  const bodyToken = String(req.body?.token || '').trim();
  if (bodyToken) return bodyToken;

  return '';
}

function buildAuthSessionResponse(row) {
  return {
    token: String(row.token || '').trim(),
    username: String(row.username || '').trim(),
    role: normalizeAuthRole(row.role),
    loggedAt: row.loggedAt || row.logged_at || row.created_at || new Date().toISOString(),
    expiresAt: row.expiresAt || row.expires_at || null,
  };
}

async function createSessionForUser(userId, req) {
  const token = crypto.randomBytes(32).toString('hex');
  const hasTtl = Number.isFinite(AUTH_SESSION_TTL_HOURS) && AUTH_SESSION_TTL_HOURS > 0;
  const expiresAt = hasTtl ? new Date(Date.now() + AUTH_SESSION_TTL_HOURS * 60 * 60 * 1000) : null;
  const forwarded = String(req.headers['x-forwarded-for'] || '').split(',')[0].trim();
  const ip = forwarded || req.ip || null;
  const userAgent = String(req.headers['user-agent'] || '').slice(0, 1000) || null;

  await pool.query(
    `INSERT INTO auth_sessions (token, id_user, expires_at, user_agent, ip_address)
     VALUES ($1, $2, $3, $4, $5)`,
    [token, Number(userId), expiresAt, userAgent, ip]
  );

  return { token, expiresAt };
}

async function getSessionContext(token, touch) {
  const cleanToken = String(token || '').trim();
  if (!cleanToken) return null;

  const result = await pool.query(
    `SELECT
       s.token,
       s.id_user,
       s.created_at AS logged_at,
       s.expires_at,
       u.username,
       u.role
     FROM auth_sessions s
     JOIN auth_users u ON u.id_user = s.id_user
     WHERE s.token = $1
       AND s.revoked_at IS NULL
       AND (s.expires_at IS NULL OR s.expires_at > NOW())
       AND u.activo = TRUE
     LIMIT 1`,
    [cleanToken]
  );

  if (!result.rowCount) return null;
  if (touch) {
    await pool.query('UPDATE auth_sessions SET last_seen_at = NOW() WHERE token = $1', [cleanToken]);
  }

  const row = result.rows[0];
  return {
    token: String(row.token || '').trim(),
    userId: Number(row.id_user),
    username: String(row.username || '').trim(),
    role: normalizeAuthRole(row.role),
    loggedAt: row.logged_at,
    expiresAt: row.expires_at || null,
  };
}

async function requireRolesForRequest(req, res, allowedRoles) {
  const token = getRequestToken(req);
  const session = await getSessionContext(token, true);
  if (!session) {
    res.status(401).json({ ok: false, error: 'Sesión inválida o expirada' });
    return null;
  }

  if (Array.isArray(allowedRoles) && allowedRoles.length && !allowedRoles.includes(session.role)) {
    res.status(403).json({ ok: false, error: 'No autorizado para esta operación' });
    return null;
  }

  req.auth = session;
  return session;
}

async function hasAdminAccess(req) {
  const sentKey = req.body?.adminKey || req.query?.adminKey || req.headers['x-admin-key'];
  if (isValidAdminKey(sentKey)) return true;

  const token = getRequestToken(req);
  if (!token) return false;

  const session = await getSessionContext(token, true);
  if (!session || session.role !== APP_ROLES.ADMIN) return false;
  req.auth = session;
  return true;
}

async function ensureAuthTables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS auth_users (
      id_user SERIAL PRIMARY KEY,
      username VARCHAR(10) NOT NULL UNIQUE,
      role VARCHAR(20) NOT NULL CHECK (role IN ('administrador', 'empaquetado', 'almacen')),
      password_hash TEXT NOT NULL,
      activo BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS auth_sessions (
      token VARCHAR(128) PRIMARY KEY,
      id_user INT NOT NULL REFERENCES auth_users(id_user) ON DELETE CASCADE,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
      expires_at TIMESTAMP NULL,
      revoked_at TIMESTAMP NULL,
      user_agent TEXT,
      ip_address VARCHAR(80)
    )
  `);

  await pool.query('CREATE INDEX IF NOT EXISTS idx_auth_users_role ON auth_users(role)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_auth_sessions_user ON auth_sessions(id_user)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_auth_sessions_revoked ON auth_sessions(revoked_at)');
}

async function ensureInitialAdminUsers() {
  for (const rawUsername of INITIAL_ADMIN_USERNAMES) {
    const username = normalizeAuthUsername(rawUsername);
    if (!username) continue;

    const passwordHash = hashPassword(INITIAL_ADMIN_PASSWORD);
    await pool.query(
      `INSERT INTO auth_users (username, role, password_hash, activo)
       VALUES ($1, $2, $3, TRUE)
       ON CONFLICT (username) DO UPDATE
         SET role = EXCLUDED.role,
             password_hash = EXCLUDED.password_hash,
             activo = TRUE,
             updated_at = NOW()`,
      [username, APP_ROLES.ADMIN, passwordHash]
    );
  }
}

app.post('/auth/register', async (req, res) => {
  const auth = await requireRolesForRequest(req, res, [APP_ROLES.ADMIN]);
  if (!auth) return;

  const username = normalizeAuthUsername(req.body?.username);
  const role = normalizeAuthRole(req.body?.role);
  const password = String(req.body?.password || '');

  if (!username || username.length < 2) {
    return res.status(400).json({ ok: false, error: 'username inválido' });
  }
  if (!role) {
    return res.status(400).json({ ok: false, error: 'role inválido' });
  }
  if (password.length < 4) {
    return res.status(400).json({ ok: false, error: 'password inválido' });
  }

  try {
    const passwordHash = hashPassword(password);
    const inserted = await pool.query(
      `INSERT INTO auth_users (username, role, password_hash, activo)
       VALUES ($1, $2, $3, TRUE)
       RETURNING id_user, username, role, created_at`,
      [username, role, passwordHash]
    );

    const user = inserted.rows[0];
    return res.status(201).json({
      ok: true,
      user: {
        username: String(user.username || '').trim(),
        role: normalizeAuthRole(user.role),
        createdAt: user.created_at,
      },
    });
  } catch (error) {
    if (error && error.code === '23505') {
      return res.status(409).json({ ok: false, error: 'Ese usuario ya existe' });
    }
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.post('/auth/login', async (req, res) => {
  const username = normalizeAuthUsername(req.body?.username);
  const role = normalizeAuthRole(req.body?.role);
  const password = String(req.body?.password || '');

  if (!username || !role || password.length < 4) {
    return res.status(400).json({ ok: false, error: 'Credenciales inválidas' });
  }

  try {
    const userResult = await pool.query(
      `SELECT id_user, username, role, password_hash, created_at
       FROM auth_users
       WHERE username = $1
         AND activo = TRUE
       LIMIT 1`,
      [username]
    );

    if (!userResult.rowCount) {
      return res.status(401).json({ ok: false, error: 'Credenciales inválidas' });
    }

    const user = userResult.rows[0];
    if (normalizeAuthRole(user.role) !== role) {
      return res.status(401).json({ ok: false, error: 'Credenciales inválidas' });
    }

    if (!verifyPassword(password, user.password_hash)) {
      return res.status(401).json({ ok: false, error: 'Credenciales inválidas' });
    }

    const sessionData = await createSessionForUser(Number(user.id_user), req);
    const session = {
      token: sessionData.token,
      username: String(user.username || '').trim(),
      role: normalizeAuthRole(user.role),
      loggedAt: new Date().toISOString(),
      expiresAt: sessionData.expiresAt,
    };
    return res.json({ ok: true, session });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.get('/auth/session', async (req, res) => {
  const token = getRequestToken(req);
  if (!token) {
    return res.status(401).json({ ok: false, error: 'Token requerido' });
  }

  try {
    const session = await getSessionContext(token, true);
    if (!session) {
      return res.status(401).json({ ok: false, error: 'Sesión inválida o expirada' });
    }
    return res.json({ ok: true, session: buildAuthSessionResponse(session) });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.post('/auth/logout', async (req, res) => {
  const token = getRequestToken(req);
  if (!token) {
    return res.status(400).json({ ok: false, error: 'Token requerido' });
  }

  try {
    await pool.query(
      `UPDATE auth_sessions
       SET revoked_at = NOW()
       WHERE token = $1
         AND revoked_at IS NULL`,
      [token]
    );
    return res.json({ ok: true });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

async function listRegisteredUsers(_auth, res) {
  try {
    const result = await pool.query(
      `SELECT username, role, created_at
       FROM auth_users
       WHERE activo = TRUE
       ORDER BY
         CASE role
           WHEN 'administrador' THEN 0
           WHEN 'almacen' THEN 1
           WHEN 'empaquetado' THEN 2
           ELSE 9
         END,
         username ASC`
    );
    return res.json({ ok: true, users: result.rows });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
}

app.get('/auth/users', async (req, res) => {
  const auth = await requireRolesForRequest(req, res, [APP_ROLES.ADMIN]);
  if (!auth) return;
  return listRegisteredUsers(auth, res);
});

app.get('/auth/users/registered', async (req, res) => {
  const auth = await requireRolesForRequest(req, res, [APP_ROLES.ADMIN]);
  if (!auth) return;
  return listRegisteredUsers(auth, res);
});

async function deleteAuthUserWithAdmin(auth, targetUser) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const targetResult = await client.query(
      `SELECT id_user, username, role
       FROM auth_users
       WHERE username = $1
         AND activo = TRUE
       LIMIT 1`,
      [targetUser]
    );

    if (!targetResult.rowCount) {
      await client.query('ROLLBACK');
      return { ok: false, status: 404, error: 'Usuario no encontrado' };
    }

    const target = targetResult.rows[0];
    if (normalizeAuthRole(target.role) === APP_ROLES.ADMIN) {
      const adminsResult = await client.query(
        `SELECT COUNT(*)::int AS total
         FROM auth_users
         WHERE activo = TRUE
           AND role = 'administrador'`
      );
      const adminsTotal = Number(adminsResult.rows[0]?.total || 0);
      if (adminsTotal <= 1) {
        await client.query('ROLLBACK');
        return { ok: false, status: 400, error: 'Debe existir al menos un Administrador activo' };
      }
    }

    await client.query('DELETE FROM auth_users WHERE id_user = $1', [Number(target.id_user)]);

    await client.query('COMMIT');
    return {
      ok: true,
      payload: {
        ok: true,
        deleted: { username: String(target.username || '').trim() },
        currentUserDeleted: String(auth.username || '').trim() === String(target.username || '').trim(),
      },
    };
  } catch (error) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    return { ok: false, status: 500, error: error.message };
  } finally {
    client.release();
  }
}

app.delete('/auth/users/:username', async (req, res) => {
  const auth = await requireRolesForRequest(req, res, [APP_ROLES.ADMIN]);
  if (!auth) return;

  const targetUser = normalizeAuthUsername(req.params?.username);
  if (!targetUser) {
    return res.status(400).json({ ok: false, error: 'username inválido' });
  }

  const result = await deleteAuthUserWithAdmin(auth, targetUser);
  if (!result.ok) {
    return res.status(result.status).json({ ok: false, error: result.error });
  }
  return res.json(result.payload);
});

app.post('/auth/users/delete', async (req, res) => {
  const auth = await requireRolesForRequest(req, res, [APP_ROLES.ADMIN]);
  if (!auth) return;

  const targetUser = normalizeAuthUsername(req.body?.username);
  if (!targetUser) {
    return res.status(400).json({ ok: false, error: 'username inválido' });
  }

  const result = await deleteAuthUserWithAdmin(auth, targetUser);
  if (!result.ok) {
    return res.status(result.status).json({ ok: false, error: result.error });
  }
  return res.json(result.payload);
});

async function ensureAlmacen09Tables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS conteo_errores (
      id SERIAL PRIMARY KEY,
      codigo_lote VARCHAR(50),
      lote_producto VARCHAR(120),
      codigo_producto VARCHAR(30),
      nombre_producto TEXT,
      cantidad_esperada INT,
      cantidad_recibida INT,
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);

  // Backward-compatible migration for old deployments that only had codigo_lote + created_at.
  await pool.query(`
    ALTER TABLE conteo_errores
    ADD COLUMN IF NOT EXISTS lote_producto VARCHAR(120)
  `);
  await pool.query(`
    ALTER TABLE conteo_errores
    ADD COLUMN IF NOT EXISTS codigo_producto VARCHAR(30)
  `);
  await pool.query(`
    ALTER TABLE conteo_errores
    ADD COLUMN IF NOT EXISTS nombre_producto TEXT
  `);
  await pool.query(`
    ALTER TABLE conteo_errores
    ADD COLUMN IF NOT EXISTS cantidad_esperada INT
  `);
  await pool.query(`
    ALTER TABLE conteo_errores
    ADD COLUMN IF NOT EXISTS cantidad_recibida INT
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

  await pool.query('CREATE INDEX IF NOT EXISTS idx_conteo_errores_created_at ON conteo_errores(created_at DESC)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_conteo_errores_codigo_lote ON conteo_errores(codigo_lote)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_almacen_lotes_procesados_estado_processed ON almacen_lotes_procesados(estado, processed_at DESC)');
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

async function ensureControlInventarioTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS control_inventario_guardia (
      id_control BIGSERIAL PRIMARY KEY,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      almacenista VARCHAR(120) NOT NULL,
      turno_actual VARCHAR(120) NOT NULL,
      momento_conteo VARCHAR(180) NOT NULL,
      id_producto INT NOT NULL REFERENCES productos(id_producto),
      cantidad_fisica_contada INT NOT NULL,
      fecha_elaboracion DATE NOT NULL,
      almacen VARCHAR(20) NOT NULL
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_control_inventario_guardia_created_at
    ON control_inventario_guardia (created_at DESC)
  `);
}

async function ensureSalidas09Tables() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS almacen09_salidas_facturas (
      id_factura BIGSERIAL PRIMARY KEY,
      numero_factura VARCHAR(80) NOT NULL UNIQUE,
      fecha_emision TIMESTAMP NOT NULL,
      created_at TIMESTAMP NOT NULL DEFAULT NOW(),
      estado VARCHAR(20) NOT NULL DEFAULT 'emitida'
    )
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS almacen09_salidas_detalle (
      id_detalle BIGSERIAL PRIMARY KEY,
      id_factura BIGINT NOT NULL REFERENCES almacen09_salidas_facturas(id_factura) ON DELETE CASCADE,
      id_producto INT NOT NULL REFERENCES productos(id_producto),
      codigo_producto VARCHAR(30) NOT NULL,
      producto TEXT NOT NULL,
      numero_lote VARCHAR(80) NOT NULL,
      maquina VARCHAR(10),
      cantidad INT NOT NULL CHECK (cantidad > 0),
      created_at TIMESTAMP NOT NULL DEFAULT NOW()
    )
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_salidas09_facturas_fecha
    ON almacen09_salidas_facturas (fecha_emision DESC)
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_salidas09_detalle_codigo_lote
    ON almacen09_salidas_detalle (codigo_producto, numero_lote)
  `);
}

async function ensurePerformanceIndexes() {
  await pool.query('CREATE INDEX IF NOT EXISTS idx_productos_activo_codigo ON productos(activo, codigo_producto)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_productos_codigo_upper ON productos(UPPER(TRIM(codigo_producto)))');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_empaquetados_cabecera_fecha_hora ON empaquetados_cabecera(fecha_hora DESC)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_empaquetados_cabecera_numero_registro ON empaquetados_cabecera(numero_registro)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_empaquetados_detalle_cabecera ON empaquetados_detalle(id_cabecera)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_empaquetados_detalle_producto ON empaquetados_detalle(id_producto)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_empaquetados_detalle_lote_upper ON empaquetados_detalle(UPPER(TRIM(numero_lote)))');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_control_inventario_guardia_producto_fecha ON control_inventario_guardia(id_producto, fecha_elaboracion DESC)');
  await pool.query('CREATE INDEX IF NOT EXISTS idx_salidas09_detalle_factura ON almacen09_salidas_detalle(id_factura)');
}

async function dropLegacyUnusedTables() {
  // Tablas legacy/no funcionales que se limpian para mantener Neon enfocado en flujos activos.
  await pool.query('DROP TABLE IF EXISTS lote_productos CASCADE');
  await pool.query('DROP TABLE IF EXISTS lotes CASCADE');
  await pool.query('DROP TABLE IF EXISTS mermas_detalle CASCADE');
  await pool.query('DROP TABLE IF EXISTS mermas_cabecera CASCADE');
}

async function registrarErrorConteo(codigoLote, erroresDetalle) {
  try {
    const cleanCodigoLote = String(codigoLote || '').trim().toUpperCase() || null;
    const detalles = Array.isArray(erroresDetalle) ? erroresDetalle : [];

    if (!detalles.length) {
      await pool.query('INSERT INTO conteo_errores (codigo_lote) VALUES ($1)', [cleanCodigoLote]);
      return;
    }

    for (const detalle of detalles) {
      const loteProducto = String(detalle?.lote_producto || '').trim().toUpperCase() || null;
      const codigoProducto = String(detalle?.codigo || '').trim().toUpperCase() || null;
      const nombreProducto = String(detalle?.descripcion || '').trim() || null;

      const esperadoRaw = Number(detalle?.esperado);
      const recibidoRaw = Number(detalle?.recibido);
      const esperado = Number.isFinite(esperadoRaw) ? esperadoRaw : null;
      const recibido = Number.isFinite(recibidoRaw) ? recibidoRaw : null;

      await pool.query(
        `INSERT INTO conteo_errores (
           codigo_lote,
           lote_producto,
           codigo_producto,
           nombre_producto,
           cantidad_esperada,
           cantidad_recibida
         )
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [cleanCodigoLote, loteProducto, codigoProducto, nombreProducto, esperado, recibido]
      );
    }
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

app.get('/productos', async (req, res) => {
  try {
    const rawQuery = String(req.query?.q || '').trim();
    const hasCustomLimit = req.query && req.query.limit !== undefined;
    const hasCustomOffset = req.query && req.query.offset !== undefined;
    const limit = Math.min(Math.max(Number(req.query?.limit || 50), 1), 200);
    const offset = Math.max(Number(req.query?.offset || 0), 0);
    const isPagedRequest = hasCustomLimit || hasCustomOffset || rawQuery.length > 0;

    const whereParts = ['COALESCE(activo, TRUE) = TRUE'];
    const params = [];

    if (rawQuery) {
      params.push(`%${rawQuery}%`);
      whereParts.push(`(
        codigo_producto ILIKE $${params.length}
        OR descripcion ILIKE $${params.length}
      )`);
    }

    const whereSql = whereParts.length ? `WHERE ${whereParts.join(' AND ')}` : '';
    const baseSelect = `
      SELECT
        id_producto,
        codigo_producto,
        descripcion AS nombre_producto,
        descripcion,
        unidad_primaria,
        paquetes AS paquetes_por_cesta,
        sobre_piso
      FROM productos
      ${whereSql}
      ORDER BY codigo_producto ASC
    `;

    let result;
    if (isPagedRequest) {
      const pagedParams = params.slice();
      pagedParams.push(limit);
      pagedParams.push(offset);

      result = await pool.query(
        `${baseSelect} LIMIT $${pagedParams.length - 1} OFFSET $${pagedParams.length}`,
        pagedParams
      );

      const countResult = await pool.query(
        `SELECT COUNT(*)::INT AS total FROM productos ${whereSql}`,
        params
      );
      const total = Number(countResult.rows?.[0]?.total || 0);
      res.setHeader('X-Total-Count', String(total));
      res.setHeader('X-Limit', String(limit));
      res.setHeader('X-Offset', String(offset));
      res.setHeader('X-Has-More', String(offset + result.rows.length < total));
    } else {
      result = await pool.query(baseSelect);
    }

    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

app.post('/productos', async (req, res) => {
  const { codigo, descripcion, unidad, paquetes, sobre_piso } = req.body;
  if (!(await hasAdminAccess(req))) {
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
  if (!(await hasAdminAccess(req))) {
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

app.post('/productos/purge-catalog', async (req, res) => {
  if (!(await hasAdminAccess(req))) {
    return res.status(403).json({ ok: false, error: 'adminKey inválido' });
  }

  const rawCodes = Array.isArray(req.body?.codigos) ? req.body.codigos : [];
  const keepCodes = Array.from(
    new Set(
      rawCodes
        .map((value) => String(value || '').trim().toUpperCase())
        .filter(Boolean)
    )
  );

  if (!keepCodes.length) {
    return res.status(400).json({ ok: false, error: 'Debes enviar codigos con al menos 1 elemento.' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    await client.query(
      `UPDATE productos
       SET activo = TRUE
       WHERE UPPER(TRIM(codigo_producto)) = ANY($1::text[])`,
      [keepCodes]
    );

    const candidates = await client.query(
      `SELECT
         p.id_producto,
         p.codigo_producto,
         EXISTS(SELECT 1 FROM empaquetados_detalle ed WHERE ed.id_producto = p.id_producto) AS used_empaquetados,
         EXISTS(SELECT 1 FROM control_inventario_guardia cg WHERE cg.id_producto = p.id_producto) AS used_control,
         EXISTS(SELECT 1 FROM almacen09_salidas_detalle sd WHERE sd.id_producto = p.id_producto) AS used_salidas
       FROM productos p
       WHERE UPPER(TRIM(p.codigo_producto)) <> ALL($1::text[])
       ORDER BY p.codigo_producto ASC`,
      [keepCodes]
    );

    const deletableIds = [];
    const blockedRows = [];

    for (const row of candidates.rows) {
      const hasRefs = Boolean(row.used_empaquetados) || Boolean(row.used_control) || Boolean(row.used_salidas);
      if (hasRefs) {
        blockedRows.push(row);
      } else {
        deletableIds.push(Number(row.id_producto));
      }
    }

    let deletedCount = 0;
    if (deletableIds.length) {
      const deleted = await client.query(
        `DELETE FROM productos
         WHERE id_producto = ANY($1::int[])`,
        [deletableIds]
      );
      deletedCount = Number(deleted.rowCount || 0);
    }

    let archivedCount = 0;
    if (blockedRows.length) {
      const blockedIds = blockedRows.map((row) => Number(row.id_producto));
      const archived = await client.query(
        `UPDATE productos
         SET activo = FALSE
         WHERE id_producto = ANY($1::int[])`,
        [blockedIds]
      );
      archivedCount = Number(archived.rowCount || 0);
    }

    const finalCountResult = await client.query(
      `SELECT COUNT(*)::int AS total
       FROM productos
       WHERE COALESCE(activo, TRUE) = TRUE`
    );
    const finalActiveCount = Number(finalCountResult.rows?.[0]?.total || 0);

    await client.query('COMMIT');

    return res.json({
      ok: true,
      keepCodes: keepCodes.length,
      deleted: deletedCount,
      archived: archivedCount,
      activeAfterPurge: finalActiveCount,
      blockedRefs: blockedRows.slice(0, 50).map((row) => ({
        codigo: row.codigo_producto,
        empaquetados: Boolean(row.used_empaquetados),
        control: Boolean(row.used_control),
        salidas: Boolean(row.used_salidas),
      })),
    });
  } catch (error) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    return res.status(500).json({ ok: false, error: error.message });
  } finally {
    client.release();
  }
});

app.post('/api/empaquetados', async (req, res) => {
  const auth = await requireRolesForRequest(req, res, [APP_ROLES.ADMIN, APP_ROLES.EMPAQUETADO]);
  if (!auth) return;

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

app.post('/api/control-inventario', async (req, res) => {
  const auth = await requireRolesForRequest(req, res, [APP_ROLES.ADMIN]);
  if (!auth) return;

  const body = req.body || {};
  const hasDetalle = Array.isArray(body.detalle);
  const cabecera = hasDetalle ? body.cabecera || {} : body;

  const cleanAlmacenista = String(cabecera.almacenista || '').trim();
  const cleanTurno = String(cabecera.turno_actual || '').trim();
  const cleanMomento = String(cabecera.momento_conteo || '').trim();
  const cleanAlmacen = String(cabecera.almacen || '').trim();

  if (!cleanAlmacenista || !cleanTurno || !cleanMomento || !cleanAlmacen) {
    return res.status(400).json({ ok: false, error: 'Faltan campos obligatorios en cabecera' });
  }

  const detalle = hasDetalle
    ? body.detalle
    : [
        {
          id_producto: body.id_producto,
          cantidad_fisica_contada: body.cantidad_fisica_contada,
          fecha_elaboracion: body.fecha_elaboracion,
        },
      ];

  const detalleNormalizado = (detalle || []).map((item) => ({
    id_producto: Number(item?.id_producto),
    cantidad_fisica_contada: Number(item?.cantidad_fisica_contada),
    fecha_elaboracion: String(item?.fecha_elaboracion || '').trim(),
  }));

  if (!detalleNormalizado.length) {
    return res.status(400).json({ ok: false, error: 'detalle es obligatorio' });
  }

  const invalidItem = detalleNormalizado.find(
    (item) =>
      !Number.isInteger(item.id_producto) ||
      item.id_producto <= 0 ||
      !Number.isFinite(item.cantidad_fisica_contada) ||
      item.cantidad_fisica_contada <= 0 ||
      !/^\d{4}-\d{2}-\d{2}$/.test(item.fecha_elaboracion)
  );
  if (invalidItem) {
    return res.status(400).json({ ok: false, error: 'Hay líneas inválidas en detalle' });
  }

  const productIds = [...new Set(detalleNormalizado.map((item) => item.id_producto))];
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const productResult = await client.query(
      `SELECT id_producto
       FROM productos
       WHERE COALESCE(activo, TRUE) = TRUE
         AND id_producto = ANY($1::int[])`,
      [productIds]
    );
    const validIds = new Set(productResult.rows.map((row) => Number(row.id_producto)));
    const missing = productIds.filter((id) => !validIds.has(id));
    if (missing.length) {
      await client.query('ROLLBACK');
      return res.status(400).json({ ok: false, error: `Productos inválidos o inactivos: ${missing.join(', ')}` });
    }

    const insertedRows = [];
    for (const item of detalleNormalizado) {
      const inserted = await client.query(
        `INSERT INTO control_inventario_guardia (
           almacenista,
           turno_actual,
           momento_conteo,
           id_producto,
           cantidad_fisica_contada,
           fecha_elaboracion,
           almacen
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         RETURNING id_control, created_at, id_producto, cantidad_fisica_contada, fecha_elaboracion`,
        [
          cleanAlmacenista,
          cleanTurno,
          cleanMomento,
          item.id_producto,
          Math.floor(item.cantidad_fisica_contada),
          item.fecha_elaboracion,
          cleanAlmacen,
        ]
      );
      insertedRows.push(inserted.rows[0]);
    }

    await client.query('COMMIT');
    return res.status(201).json({ ok: true, total: insertedRows.length, rows: insertedRows });
  } catch (error) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    return res.status(500).json({ ok: false, error: error.message });
  } finally {
    client.release();
  }
});

app.get('/api/registros', async (req, res) => {
  const tipo = String(req.query.tipo || 'Consolidado').trim().toLowerCase();
  const limit = Math.min(Math.max(Number(req.query.limit || 200), 1), 5000);
  const offset = Math.max(Number(req.query.offset || 0), 0);
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
      const fetchLimit = limit + 1;
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
      params.push(fetchLimit);
      params.push(offset);
      const whereClauseActual = wherePartsActual.length ? `WHERE ${wherePartsActual.join(' AND ')}` : '';
      const whereClauseHistorico = wherePartsHistorico.length ? `WHERE ${wherePartsHistorico.join(' AND ')}` : '';

      const result = await pool.query(
        `WITH actual AS (
          SELECT
            ed.id_detalle::bigint AS "__ROW_ID",
            'empaquetados_detalle'::text AS "__ROW_SOURCE",
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
            hr.id_historico AS "__ROW_ID",
            'historico_resultados_consolidado'::text AS "__ROW_SOURCE",
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
          "__ROW_SOURCE",
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
        LIMIT $${params.length - 1}
        OFFSET $${params.length}`,
        params
      );

      const hasMore = result.rows.length > limit;
      const rows = hasMore ? result.rows.slice(0, limit) : result.rows;
      const headers = rows.length ? Object.keys(rows[0]) : [];
      return res.json({
        ok: true,
        sheet: 'Consolidado',
        headers,
        rows,
        total: rows.length,
        hasMore,
        offset,
        limit,
      });
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
  const auth = await requireRolesForRequest(req, res, [APP_ROLES.ADMIN]);
  if (!auth) return;

  const rowsRaw = Array.isArray(req.body?.rows) ? req.body.rows : [];
  const rows = rowsRaw
    .map((row) => ({
      source: String(row?.source || '').trim().toLowerCase(),
      id: String(row?.id ?? '').trim(),
    }))
    .filter((row) => {
      if (!/^\d+$/.test(row.id) || row.id === '0') return false;
      return row.source === 'empaquetados_detalle' || row.source === 'historico_resultados_consolidado';
    });

  const legacyIdsRaw = Array.isArray(req.body?.ids) ? req.body.ids : [];
  const legacyIds = legacyIdsRaw
    .map((id) => Number(id))
    .filter((id) => Number.isInteger(id) && id > 0);

  const detalleIdsSet = new Set(legacyIds);
  const historicoIdsSet = new Set();

  rows.forEach((row) => {
    if (row.source === 'empaquetados_detalle') {
      detalleIdsSet.add(Number(row.id));
      return;
    }
    historicoIdsSet.add(row.id);
  });

  const detalleIds = Array.from(detalleIdsSet).filter((id) => Number.isInteger(id) && id > 0);
  const historicoIds = Array.from(historicoIdsSet);

  if (!detalleIds.length && !historicoIds.length) {
    return res.status(400).json({ ok: false, error: 'rows o ids es obligatorio y debe contener valores válidos' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    let deletedTotal = 0;
    let deletedDetalleCount = 0;
    let deletedHistoricoCount = 0;
    let deletedCabecerasCount = 0;

    if (detalleIds.length) {
      const deleted = await client.query(
        `DELETE FROM empaquetados_detalle
         WHERE id_detalle = ANY($1::int[])
         RETURNING id_cabecera`,
        [detalleIds]
      );
      deletedDetalleCount = deleted.rowCount || 0;
      deletedTotal += deletedDetalleCount;

      const cabeceras = [...new Set(deleted.rows.map((row) => Number(row.id_cabecera)).filter(Boolean))];
      if (cabeceras.length) {
        const deletedCabeceras = await client.query(
          `DELETE FROM empaquetados_cabecera ec
           WHERE ec.id_cabecera = ANY($1::int[])
             AND NOT EXISTS (
               SELECT 1
               FROM empaquetados_detalle ed
               WHERE ed.id_cabecera = ec.id_cabecera
             )`,
          [cabeceras]
        );
        deletedCabecerasCount = deletedCabeceras.rowCount || 0;
      }
    }

    if (historicoIds.length) {
      const deletedHistorico = await client.query(
        `DELETE FROM historico_resultados_consolidado
         WHERE id_historico = ANY($1::bigint[])`,
        [historicoIds]
      );
      deletedHistoricoCount = deletedHistorico.rowCount || 0;
      deletedTotal += deletedHistoricoCount;
    }

    await client.query('COMMIT');
    return res.json({
      ok: true,
      deleted: deletedTotal,
      deleted_detalle: deletedDetalleCount,
      deleted_historico: deletedHistoricoCount,
      deleted_cabeceras: deletedCabecerasCount,
    });
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
  const auth = await requireRolesForRequest(req, res, [APP_ROLES.ADMIN, APP_ROLES.ALMACEN]);
  if (!auth) return;

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

    const erroresConteoDetalle = [];
    const productosValidados = [];
    for (const producto of productosResult.rows) {
      const recibido = cantidadesPorLinea.has(producto.line_key)
        ? cantidadesPorLinea.get(producto.line_key)
        : cantidadesPorProductoId.has(producto.id)
          ? cantidadesPorProductoId.get(producto.id)
          : cantidadesPorCodigo.get(producto.codigo);

      const esperado = Number(producto.cantidad);
      const recibidoNumero = Number(recibido);
      if (recibido === undefined || Number.isNaN(recibidoNumero) || recibidoNumero !== esperado) {
        erroresConteoDetalle.push({
          codigo: producto.codigo,
          descripcion: producto.descripcion,
          lote_producto: producto.lote_producto,
          esperado,
          recibido: recibido === undefined || Number.isNaN(recibidoNumero) ? null : recibidoNumero,
        });
        continue;
      }

      productosValidados.push({
        id: producto.id,
        codigo: producto.codigo,
        cantidad: producto.cantidad,
        recibido: recibidoNumero,
      });
    }

    if (erroresConteoDetalle.length) {
      await client.query('ROLLBACK');
      await registrarErrorConteo(codigoRegistro, erroresConteoDetalle);
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
         SELECT
           id,
           codigo_lote,
           lote_producto,
           codigo_producto,
           nombre_producto,
           cantidad_esperada,
           cantidad_recibida,
           created_at
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
         COALESCE(NULLIF(TRIM(e.lote_producto), ''), lr.lote_referencia, e.codigo_lote) AS lote_mostrado,
         e.codigo_producto,
         e.nombre_producto,
         e.cantidad_esperada,
         e.cantidad_recibida,
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

app.post('/api/almacen09/salidas-facturas', async (req, res) => {
  const auth = await requireRolesForRequest(req, res, [APP_ROLES.ADMIN, APP_ROLES.ALMACEN]);
  if (!auth) return;

  const numeroFacturaRaw = String(req.body?.numero_factura || '').trim();
  const fechaEmisionRaw = String(req.body?.fecha_emision || '').trim();
  const detalleRaw = Array.isArray(req.body?.detalle) ? req.body.detalle : [];

  const numeroFactura = numeroFacturaRaw.toUpperCase();
  const fechaEmision = fechaEmisionRaw || new Date().toISOString();

  if (!numeroFactura) {
    return res.status(400).json({ ok: false, error: 'numero_factura es obligatorio' });
  }
  if (!detalleRaw.length) {
    return res.status(400).json({ ok: false, error: 'detalle es obligatorio' });
  }

  const detalle = detalleRaw
    .map((linea) => ({
      codigo: String(linea?.codigo || '').trim().toUpperCase(),
      producto: String(linea?.producto || '').trim(),
      lote: String(linea?.lote || '').trim().toUpperCase(),
      maquina: String(linea?.maquina || '').trim(),
      cantidad: Number(linea?.cantidad),
    }))
    .filter((linea) => linea.codigo && linea.lote && Number.isFinite(linea.cantidad) && linea.cantidad > 0);

  if (!detalle.length) {
    return res.status(400).json({ ok: false, error: 'No hay líneas válidas en detalle' });
  }

  const distinctCodigos = [...new Set(detalle.map((linea) => linea.codigo))];

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const duplicated = await client.query(
      'SELECT 1 FROM almacen09_salidas_facturas WHERE UPPER(TRIM(numero_factura)) = $1 LIMIT 1',
      [numeroFactura]
    );
    if (duplicated.rowCount) {
      await client.query('ROLLBACK');
      return res.status(409).json({ ok: false, error: 'El serial de factura ya existe' });
    }

    const productsResult = await client.query(
      `SELECT id_producto, UPPER(TRIM(codigo_producto)) AS codigo_producto, descripcion
       FROM productos
       WHERE UPPER(TRIM(codigo_producto)) = ANY($1::text[])`,
      [distinctCodigos]
    );

    const productMap = new Map();
    productsResult.rows.forEach((row) => {
      productMap.set(String(row.codigo_producto || '').trim().toUpperCase(), {
        id_producto: Number(row.id_producto),
        descripcion: String(row.descripcion || '').trim(),
      });
    });

    const missingCodigos = distinctCodigos.filter((codigo) => !productMap.has(codigo));
    if (missingCodigos.length) {
      await client.query('ROLLBACK');
      return res.status(400).json({ ok: false, error: `Códigos no encontrados: ${missingCodigos.join(', ')}` });
    }

    const facturaInsert = await client.query(
      `INSERT INTO almacen09_salidas_facturas (numero_factura, fecha_emision, estado)
       VALUES ($1, $2, 'emitida')
       RETURNING id_factura, numero_factura, fecha_emision`,
      [numeroFactura, fechaEmision]
    );

    const idFactura = Number(facturaInsert.rows[0].id_factura);
    for (const linea of detalle) {
      const product = productMap.get(linea.codigo);
      await client.query(
        `INSERT INTO almacen09_salidas_detalle (
           id_factura,
           id_producto,
           codigo_producto,
           producto,
           numero_lote,
           maquina,
           cantidad
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7)`,
        [
          idFactura,
          Number(product.id_producto),
          linea.codigo,
          linea.producto || product.descripcion,
          linea.lote,
          linea.maquina || null,
          Math.floor(linea.cantidad),
        ]
      );
    }

    await client.query('COMMIT');
    return res.status(201).json({
      ok: true,
      factura: {
        id_factura: idFactura,
        numero_factura: facturaInsert.rows[0].numero_factura,
        fecha_emision: facturaInsert.rows[0].fecha_emision,
        lineas: detalle.length,
      },
    });
  } catch (error) {
    try { await client.query('ROLLBACK'); } catch (_) {}
    return res.status(500).json({ ok: false, error: error.message });
  } finally {
    client.release();
  }
});

app.get('/api/almacen09/salidas-facturas', async (req, res) => {
  const limit = Math.min(Math.max(Number(req.query?.limit || 100), 1), 500);
  try {
    const result = await pool.query(
      `SELECT
         sf.id_factura,
         sf.numero_factura,
         TO_CHAR(sf.fecha_emision, 'YYYY-MM-DD HH24:MI:SS') AS fecha_emision,
         sd.codigo_producto,
         sd.producto,
         sd.numero_lote,
         sd.maquina,
         sd.cantidad
       FROM almacen09_salidas_facturas sf
       JOIN almacen09_salidas_detalle sd ON sd.id_factura = sf.id_factura
       ORDER BY sf.fecha_emision DESC, sf.id_factura DESC, sd.id_detalle ASC
       LIMIT $1`,
      [limit]
    );
    return res.json({ ok: true, rows: result.rows, total: result.rowCount || 0 });
  } catch (error) {
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.get('/api/almacen09/stock-actual', async (req, res) => {
  const desdeRaw = String(req.query?.desde || STOCK_RESET_DATE).trim();
  const desde = /^\d{4}-\d{2}-\d{2}$/.test(desdeRaw) ? desdeRaw : STOCK_RESET_DATE;
  const q = String(req.query?.q || '').trim();
  const codigo = String(req.query?.codigo || '').trim().toUpperCase();
  const loteFiltro = String(req.query?.loteFiltro || '').trim().toLowerCase();
  const rawLimit = Number(req.query?.limit);
  const hasLimit = Number.isFinite(rawLimit) && rawLimit > 0;
  const limit = hasLimit ? Math.min(Math.max(Math.floor(rawLimit), 1), 4000) : 0;

  try {
    const params = [desde];
    let dynamicWhere = '';

    if (codigo) {
      params.push(codigo);
      dynamicWhere += ` AND sa.codigo_producto = $${params.length}`;
    } else if (q) {
      params.push(`%${q}%`);
      dynamicWhere += ` AND (sa.codigo_producto ILIKE $${params.length} OR sa.producto ILIKE $${params.length})`;
    }

    if (loteFiltro === 'dia2a5') {
      dynamicWhere += ' AND sa.age_days BETWEEN 2 AND 5';
    } else if (loteFiltro === 'primeros2') {
      dynamicWhere += ' AND sa.age_days BETWEEN 0 AND 1';
    }

    let limitSql = '';
    if (hasLimit) {
      params.push(limit);
      limitSql = ` LIMIT $${params.length}`;
    }

    const result = await pool.query(
      `WITH stock_validado AS (
         SELECT
           UPPER(TRIM(p.codigo_producto)) AS codigo_producto,
           p.descripcion AS producto,
           UPPER(TRIM(ed.numero_lote)) AS numero_lote,
           DATE(ec.fecha_hora) AS fecha_empaquetado,
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
         GROUP BY UPPER(TRIM(p.codigo_producto)), p.descripcion, UPPER(TRIM(ed.numero_lote)), DATE(ec.fecha_hora)
       ),
       salidas AS (
         SELECT
           UPPER(TRIM(sd.codigo_producto)) AS codigo_producto,
           UPPER(TRIM(sd.numero_lote)) AS numero_lote,
           SUM(sd.cantidad)::int AS cantidad_salida
         FROM almacen09_salidas_detalle sd
         JOIN almacen09_salidas_facturas sf ON sf.id_factura = sd.id_factura
         WHERE DATE(sf.fecha_emision) >= $1
         GROUP BY UPPER(TRIM(sd.codigo_producto)), UPPER(TRIM(sd.numero_lote))
       ),
       base AS (
         SELECT
           sv.codigo_producto,
           sv.producto,
           sv.numero_lote,
           sv.fecha_empaquetado,
           GREATEST(0, sv.cantidad - COALESCE(sa.cantidad_salida, 0))::int AS cantidad
         FROM stock_validado sv
         LEFT JOIN salidas sa
           ON sa.codigo_producto = sv.codigo_producto
          AND sa.numero_lote = sv.numero_lote
         WHERE GREATEST(0, sv.cantidad - COALESCE(sa.cantidad_salida, 0)) > 0
       ),
       stock_actual AS (
         SELECT
           base.codigo_producto,
           base.producto,
           base.numero_lote,
           base.fecha_empaquetado,
           base.cantidad,
           (DATE(NOW() AT TIME ZONE 'America/Caracas') - base.fecha_empaquetado)::int AS age_days
         FROM base
       )
       SELECT
         sa.codigo_producto,
         sa.producto,
         sa.numero_lote,
         TO_CHAR(sa.fecha_empaquetado, 'YYYY-MM-DD') AS fecha_empaquetado,
         sa.cantidad
       FROM stock_actual sa
       WHERE 1=1 ${dynamicWhere}
       ORDER BY sa.codigo_producto, sa.numero_lote, sa.fecha_empaquetado${limitSql}`,
      params
    );

    return res.json({ ok: true, desde, rows: result.rows, total: result.rows.length });
  } catch (error) {
    return res.status(500).json({ ok: false, error: 'Error al calcular stock actual desde Almacén09' });
  }
});

Promise.all([
  ensureAuthTables(),
  ensureAlmacen09Tables(),
  ensureProductosSoftDelete(),
  ensureHistoricoResultadosTable(),
  ensureControlInventarioTable(),
  ensureSalidas09Tables(),
  ensurePerformanceIndexes(),
  dropLegacyUnusedTables(),
])
  .then(async () => {
    await ensureInitialAdminUsers();
    app.listen(port, () => {
      console.log(`Servidor escuchando en puerto ${port}`);
    });
  })
  .catch((error) => {
    console.error('No se pudieron preparar las tablas base:', error);
    process.exit(1);
  });
