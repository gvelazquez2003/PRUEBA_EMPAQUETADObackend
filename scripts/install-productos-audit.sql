-- Instala auditoria de cambios en productos.
-- Desde que se ejecuta, cada INSERT/UPDATE/DELETE queda registrado en productos_audit.

CREATE TABLE IF NOT EXISTS productos_audit (
  id_audit BIGSERIAL PRIMARY KEY,
  operation TEXT NOT NULL,
  changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  db_user TEXT NOT NULL DEFAULT CURRENT_USER,
  app_name TEXT NOT NULL DEFAULT CURRENT_SETTING('application_name', TRUE),
  client_addr TEXT,
  old_row JSONB,
  new_row JSONB
);

CREATE OR REPLACE FUNCTION productos_audit_trigger_fn()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO productos_audit (
    operation,
    db_user,
    app_name,
    client_addr,
    old_row,
    new_row
  )
  VALUES (
    TG_OP,
    CURRENT_USER,
    CURRENT_SETTING('application_name', TRUE),
    INET_CLIENT_ADDR()::TEXT,
    CASE WHEN TG_OP IN ('UPDATE', 'DELETE') THEN TO_JSONB(OLD) ELSE NULL END,
    CASE WHEN TG_OP IN ('INSERT', 'UPDATE') THEN TO_JSONB(NEW) ELSE NULL END
  );

  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_trigger
    WHERE tgname = 'trg_productos_audit'
  ) THEN
    CREATE TRIGGER trg_productos_audit
    AFTER INSERT OR UPDATE OR DELETE ON productos
    FOR EACH ROW
    EXECUTE FUNCTION productos_audit_trigger_fn();
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_productos_audit_changed_at
ON productos_audit (changed_at DESC);

SELECT 'productos_audit_instalado' AS estado;
