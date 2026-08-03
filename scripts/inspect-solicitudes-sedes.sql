-- Inspeccion segura para phpPgAdmin. No modifica datos.
SELECT table_schema, table_name, column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN (
    'solicitudes_sedes',
    'solicitudes_sedes_detalle',
    'entregas_sedes',
    'entregas_sedes_detalle',
    'productos',
    'sedes'
  )
ORDER BY table_name, ordinal_position;

SELECT tc.table_name, tc.constraint_name, tc.constraint_type, kcu.column_name
FROM information_schema.table_constraints tc
LEFT JOIN information_schema.key_column_usage kcu
  ON kcu.constraint_name = tc.constraint_name
 AND kcu.table_schema = tc.table_schema
WHERE tc.table_schema = 'public'
  AND tc.table_name IN (
    'solicitudes_sedes',
    'solicitudes_sedes_detalle',
    'entregas_sedes',
    'entregas_sedes_detalle'
  )
ORDER BY tc.table_name, tc.constraint_type, tc.constraint_name;
