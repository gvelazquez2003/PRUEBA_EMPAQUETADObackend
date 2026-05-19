import crypto from 'crypto';
import dotenv from 'dotenv';
import { Pool } from 'pg';

dotenv.config();

const DEFAULT_PASSWORD = process.env.AUTH_DEFAULT_PASSWORD || 'Admin12345';
const TEST_USERS = [
  ['PRUEBAS2', 'produccion'],
  ['PRUEBAS3', 'almacen'],
  ['PRUEBAS4', 'facturacion'],
];

if (!process.env.DATABASE_URL) {
  console.error('DATABASE_URL es obligatorio.');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const hash = crypto.scryptSync(String(password || ''), salt, 64).toString('hex');
  return `scrypt:${salt}:${hash}`;
}

try {
  for (const [username, role] of TEST_USERS) {
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

  const result = await pool.query(
    `SELECT username, role, activo
       FROM auth_users
      WHERE username LIKE 'PRUEBAS%'
      ORDER BY username`
  );

  console.log(JSON.stringify({
    restaurados: TEST_USERS.map(([username, role]) => ({
      username,
      role,
      password: DEFAULT_PASSWORD,
    })),
    actuales: result.rows,
  }, null, 2));
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
} finally {
  await pool.end();
}
