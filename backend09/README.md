# backend09

Componentes de Almacén 09 integrados al backend principal.

## Estado
- Endpoints implementados en `src/index.js` bajo prefijo `/api/almacen09`.
- Tablas de soporte se auto-crean al iniciar backend.
- La validación de Almacén09 guarda un resumen JSON por lote en `almacen_lotes_procesados`.

## Endpoints
- `GET /api/almacen09/lotes`
- `POST /api/almacen09/validar-conteo`
- `POST /api/almacen09/borrar-lotes`
- `POST /api/almacen09/borrar-registros`
- `GET /api/almacen09/errores-conteo?key=...`

## Reinicio total en Neon
1. Ejecuta `reset_total_backend09.sql`.
2. Verifica que ambos conteos queden en 0.
3. Ejecuta `seed_masivo_neon.sql` para poblar prueba de carga.
4. Si quieres limpiar luego de probar, ejecuta `cleanup_masivo_neon.sql`.
