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
