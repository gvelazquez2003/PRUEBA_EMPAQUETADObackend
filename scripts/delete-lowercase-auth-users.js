import dotenv from 'dotenv';
import { Pool } from 'pg';

dotenv.config();

if (!process.env.DATABASE_URL) {
  console.error('DATABASE_URL es obligatorio.');
  process.exit(1);
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

try {
  const before = await pool.query(
    `SELECT username, role
       FROM auth_users
      WHERE username <> UPPER(username)
      ORDER BY username`
  );

  const deleted = await pool.query(
    `DELETE FROM auth_users
      WHERE username <> UPPER(username)
      RETURNING username, role`
  );

  const remaining = await pool.query(
    `SELECT username, role
       FROM auth_users
      ORDER BY username`
  );

  console.log(JSON.stringify({
    usuarios_con_minuscula_antes: before.rows,
    eliminados: deleted.rows,
    usuarios_restantes: remaining.rows,
  }, null, 2));
} catch (error) {
  console.error(error.message);
  process.exitCode = 1;
} finally {
  await pool.end();
}
