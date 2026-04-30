CREATE SCHEMA IF NOT EXISTS "Nuevas Tablas";

CREATE TABLE IF NOT EXISTS "Nuevas Tablas".datos_users (
    id_user INTEGER PRIMARY KEY,
    nombre TEXT NOT NULL,
    apellido TEXT NOT NULL,
    ci TEXT UNIQUE,
    telf TEXT
);

ALTER TABLE "Nuevas Tablas".datos_users
DROP CONSTRAINT IF EXISTS datos_users_id_user_fkey;

ALTER TABLE "Nuevas Tablas".datos_users
ADD CONSTRAINT datos_users_id_user_fkey
FOREIGN KEY (id_user)
REFERENCES "Nuevas Tablas".users(id_user)
ON UPDATE CASCADE
ON DELETE CASCADE;

INSERT INTO "Nuevas Tablas".datos_users (id_user, nombre, apellido, ci, telf)
SELECT id_user, 'Gustavo', 'Velazquez', 'V30246221', '04241640330'
FROM "Nuevas Tablas".users
WHERE users = 'GVelazquez'
ON CONFLICT (id_user) DO UPDATE
SET nombre = EXCLUDED.nombre,
    apellido = EXCLUDED.apellido,
    ci = EXCLUDED.ci,
    telf = EXCLUDED.telf;
