import crypto from 'crypto';
import { Pool } from 'pg';

const DEFAULT_PASSWORD = process.env.AUTH_DEFAULT_PASSWORD || 'Admin12345';

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(String(password || ''), salt, 64).toString('hex');
  return `scrypt:${salt}:${hash}`;
}

async function main() {
  await pool.query('ALTER TABLE auth_users ALTER COLUMN username TYPE VARCHAR(20)');
  await pool.query('ALTER TABLE auth_users DROP CONSTRAINT IF EXISTS auth_users_role_check');

  await pool.query(`
    UPDATE auth_users
       SET role = 'produccion'
     WHERE lower(trim(role)) IN ('empaquetado', 'produccion', 'producción')
  `);
  await pool.query(`
    UPDATE auth_users
       SET role = 'almacen'
     WHERE lower(trim(role)) IN ('almacen', 'almacén')
  `);
  await pool.query(`
    UPDATE auth_users
       SET role = 'administrador'
     WHERE lower(trim(role)) IN ('administrador', 'admin')
  `);
  await pool.query(`
    UPDATE auth_users
       SET role = 'facturacion'
     WHERE lower(trim(role)) IN ('facturacion', 'facturación')
  `);
  await pool.query(`
    UPDATE auth_users
       SET role = 'ventas'
     WHERE lower(trim(role)) IN ('ventas', 'venta')
  `);
  await pool.query(`
    UPDATE auth_users
       SET role = 'vendedor'
     WHERE lower(trim(role)) IN ('vendedor', 'seller')
  `);
  await pool.query(`
    UPDATE auth_users
       SET role = 'almacen'
     WHERE role IS NULL
        OR trim(role) = ''
        OR lower(trim(role)) NOT IN ('administrador', 'produccion', 'almacen', 'facturacion', 'ventas', 'vendedor')
  `);

  await pool.query(`
    ALTER TABLE auth_users
    ADD CONSTRAINT auth_users_role_check
    CHECK (role IN ('administrador', 'produccion', 'almacen', 'facturacion', 'ventas', 'vendedor'))
  `);

  const users = [
    ['FACTURACION', 'facturacion'],
    ['VENTAS', 'ventas'],
    ['VENDEDOR', 'vendedor'],
  ];
  for (const [username, role] of users) {
    await pool.query(
      `INSERT INTO auth_users (username, role, password_hash, activo)
       VALUES ($1, $2, $3, TRUE)
       ON CONFLICT (username) DO UPDATE
         SET role = EXCLUDED.role,
             activo = TRUE,
             updated_at = NOW()`,
      [username, role, hashPassword(DEFAULT_PASSWORD)]
    );
  }

  const result = await pool.query(`
    SELECT username, role, activo
      FROM auth_users
     WHERE username IN ('ALMACEN', 'FACTURACION', 'VENTAS', 'VENDEDOR')
        OR lower(role) IN ('almacen', 'facturacion', 'ventas', 'vendedor')
     ORDER BY username
  `);

  console.log(JSON.stringify({ ok: true, users: result.rows }, null, 2));
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end().catch(() => {});
  });
