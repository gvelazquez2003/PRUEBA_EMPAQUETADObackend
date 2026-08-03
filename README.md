# Backend Node + PostgreSQL SPanel (Render)

## 1) Variables de entorno

Copia `.env.example` a `.env` y completa:

- `DATABASE_URL`: cadena PostgreSQL de la base asignada en SPanel.
- `DB_SSL`: `false` si PostgreSQL de SPanel no tiene TLS configurado; `true` si SPanel habilita SSL.
- `DB_CONNECT_TIMEOUT_MS`: timeout de conexión al servidor PostgreSQL (ejemplo `10000`).
- `DB_POOL_MAX`: conexiones simultáneas máximas del backend a PostgreSQL (recomendado `5` en hosting compartido).
- `PORT`: puerto local (ejemplo `3001`).
- `NODE_ENV`: `development` o `production`.
- `ADMIN_KEY`: clave para crear/eliminar productos.
- `CORS_ORIGIN`: dominios permitidos separados por coma o `*`.
- `ROUTING_PROVIDER`: usa `openrouteservice` para el módulo de Rutas sin Google Routes API.
- `OPENROUTESERVICE_API_KEY`: clave de OpenRouteService para geocoding, matriz y direcciones.
- `ROUTING_REFERENCE_LAT` / `ROUTING_REFERENCE_LNG`: punto central operativo para descartar coordenadas absurdamente lejanas (por defecto Bello Campo).
- `ROUTING_MAX_GEOCODE_DISTANCE_KM`: distancia maxima permitida desde el punto central para aceptar coordenadas automaticas (recomendado `250`).
- `ORS_ROUTING_SNAP_RADII`: radios de reintento para que ORS conecte coordenadas cercanas a una via (recomendado `350,1000,2500,5000`).
- `GOOGLE_GEOCODING_FALLBACK_ENABLED`: `true` activa Google Geocoding solo como respaldo cuando OpenRouteService no ubica una dirección.
- `GOOGLE_GEOCODING_API_KEY`: clave restringida a Geocoding API. No debe exponerse en frontend.
- `GOOGLE_GEOCODING_DAILY_LIMIT`: límite diario interno de llamadas fallback (recomendado `250`).
- `GOOGLE_GEOCODING_MONTHLY_LIMIT`: límite mensual interno de llamadas fallback (recomendado `9000`).
- `GOOGLE_GEOCODING_CACHE_DAYS`: días que se reutilizan coordenadas resueltas por Google (recomendado `30`).
- `GOOGLE_GEOCODING_MAX_VARIANTS`: cantidad máxima de versiones limpias que se prueban por dirección cuando ORS falla (recomendado `5`).

## 2) Ejecutar local

```bash
npm install
npm run dev
```

## 3) Inicializar DB en PostgreSQL

Ejecuta el archivo `schema.sql` en tu base PostgreSQL.

Si ya tienes una base en producción y solo quieres optimizar sin reset total, ejecuta `optimize_neon.sql`.

## 3.1) Importar clientes masivos

Para cargar `Clientes.csv` en la tabla `"Nuevas Tablas".clientes`:

```bash
npm run import:clientes -- "C:\Users\gvela\OneDrive\Escritorio\Clientes.csv"
```

Si tu tabla está en otra ruta o esquema, puedes pasar la ruta del CSV, el esquema y el nombre de la tabla como argumentos adicionales.

## 4) Endpoints principales

- `GET /health`
- `GET /destinos`
- `GET /sedes`
- `GET /responsables`
- `GET /productos`
- `POST /productos`
- `DELETE /productos/:codigo`
- `POST /api/empaquetados`
- `GET /api/registros?tipo=Produccion|General|Almacen09|Consolidado&limit=200`
- `POST /api/registros/delete`
- `POST /api/control-inventario`
- `GET /api/almacen09/lotes`
- `POST /api/almacen09/validar-conteo`
- `POST /api/almacen09/borrar-lotes`
- `POST /api/almacen09/borrar-registros`
- `GET /api/almacen09/errores-conteo?key=...`
- `GET /api/almacen09/stock-actual`
- `POST /api/almacen09/salidas-facturas`
- `GET /api/almacen09/salidas-facturas?limit=100`
- `POST /auth/register`
- `POST /auth/login`
- `GET /auth/session`
- `POST /auth/logout`
- `GET /auth/users`
- `DELETE /auth/users/:username`

## 5) Estructura PostgreSQL optimizada

Tablas activas por dominio:

- Catálogos maestros:
	- `destinos`: destinos de despacho para empaquetado.
	- `sedes`: sedes disponibles.
	- `responsables`: personal responsable.
	- `productos`: catálogo oficial (con soft delete vía columna `activo`).
- Operación de empaquetado:
	- `empaquetados_cabecera`: encabezado por registro (fecha, destino, responsable, sede).
	- `empaquetados_detalle`: líneas por producto/lote/cantidad.
- Almacén09 (entradas y validación):
	- `almacen_lotes_procesados`: estado por lote (validado/descartado) y resumen JSON de validación.
	- `conteo_errores`: trazabilidad de diferencias por lote y por producto.
- Almacén09 (salidas por facturación):
	- `salidas_facturas`: cabecera de facturas.
	- `almacen09_salidas_detalle`: detalle por producto/lote/cantidad.
- Control de inventario por guardia:
	- `control_inventario_guardia`: conteos físicos por producto y fecha de elaboración.
- Histórico consolidado:
	- `historico_resultados_consolidado`: consolidado importado desde CSV para reportes.
- Seguridad/autenticación:
	- `auth_users`: usuarios del sistema con rol (`administrador`, `produccion`, `almacen`).
	- `auth_sessions`: sesiones activas con expiración y revocación.

Notas:
- El rol antiguo `empaquetado` se migra automáticamente a `produccion` al iniciar el backend.
- `mermas_cabecera`, `mermas_detalle`, `lotes` y `lote_productos` se consideran legacy/no funcionales y se eliminan automáticamente al iniciar el backend.
- El endpoint de stock (`/api/almacen09/stock-actual`) ya descuenta lo facturado en Salidas09.
- Se agregaron índices de rendimiento para consultas de productos, empaquetado, almacén, control de inventario, auth e histórico.

## 6) Despliegue en Render

- Root Directory: dejar vacío (este repo ya es solo backend).
- Build Command: `npm install`
- Start Command: `npm start`
- Variables: `DATABASE_URL`, `DB_SSL`, `DB_CONNECT_TIMEOUT_MS`, `DB_POOL_MAX`, `NODE_ENV=production`, `ADMIN_KEY`, `CORS_ORIGIN`

Para SPanel sin SSL, usa una URL sin `sslmode` y define `DB_SSL=false`:

```env
DATABASE_URL=postgresql://USUARIO:CLAVE@HOST:5432/NOMBRE_DB
DB_SSL=false
DB_CONNECT_TIMEOUT_MS=10000
DB_POOL_MAX=5
NODE_ENV=production
ADMIN_KEY=CAMBIAR_CLAVE_ADMIN
CORS_ORIGIN=https://produccionpdt.com
ROUTING_PROVIDER=openrouteservice
OPENROUTESERVICE_API_KEY=CAMBIAR_API_KEY
ROUTING_REFERENCE_LAT=10.492
ROUTING_REFERENCE_LNG=-66.856
ROUTING_MAX_GEOCODE_DISTANCE_KM=250
ORS_ROUTING_SNAP_RADII=350,1000,2500,5000
GOOGLE_GEOCODING_FALLBACK_ENABLED=true
GOOGLE_GEOCODING_API_KEY=CAMBIAR_API_KEY_GEOCODING
GOOGLE_GEOCODING_DAILY_LIMIT=250
GOOGLE_GEOCODING_MONTHLY_LIMIT=9000
GOOGLE_GEOCODING_CACHE_DAYS=30
GOOGLE_GEOCODING_MAX_VARIANTS=5
```

El usuario PostgreSQL debe estar asignado a `admin01_neondbfinal` y SPanel debe permitir conexiones desde los rangos IP de salida del servicio Render.
En Render no agregues `DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` ni `DB_PORT`: el backend usa `DATABASE_URL`. Tampoco es necesario fijar `PORT`; Render lo proporciona al servicio.

## 7) Solicitudes y Entregas a Sedes

Modulo interno para registrar solicitudes de productos por sede, registrar entregas y sincronizar una copia operativa hacia Google Sheets desde el backend.

### Variables de entorno

```env
SOLICITUDES_SHEETS_WEBHOOK_URL=
SOLICITUDES_SHEETS_WEBHOOK_SECRET=
SOLICITUDES_SHEETS_SYNC_ENABLED=false
SOLICITUDES_ALLOWED_UNITS=UND,KG,PAQ,LT,CAJ,BLT,ENV,SAC,RLL,BOLSA,CESTA,BULTO
```

`SOLICITUDES_SHEETS_SYNC_ENABLED` queda en `false` por defecto. El registro en PostgreSQL no depende de Sheets.

### Matriz de permisos

- `administrador`: acceso completo, cambio manual de estado, reintentos de Sheets y actualizacion de unidad primaria.
- `produccion`: crear y consultar solicitudes; consultar entregas.
- `almacen`: consultar solicitudes; crear y consultar entregas; actualizar unidad primaria; reintentar Sheets.

No se agregaron roles nuevos. Se reutilizan los roles existentes de `auth_users`.

### Endpoints

- `GET /api/solicitudes-sedes/catalogos/sedes`
- `GET /api/solicitudes-sedes/catalogos/productos?limit=5000&q=texto`
- `PATCH /api/solicitudes-sedes/catalogos/productos/:id/unidad`
- `GET /api/solicitudes-sedes`
- `GET /api/solicitudes-sedes/:id`
- `POST /api/solicitudes-sedes`
- `PATCH /api/solicitudes-sedes/:id/estado`
- `GET /api/entregas-sedes`
- `GET /api/entregas-sedes/:id`
- `POST /api/entregas-sedes`
- `POST /api/solicitudes-sedes/sheets/retry`

Todas las respuestas mantienen el formato general del backend:

```json
{ "ok": true }
```

o:

```json
{ "ok": false, "error": "mensaje" }
```

### Payload de solicitud

```json
{
  "fecha": "2026-08-03",
  "hora": "07:30",
  "sede_id": 1,
  "responsable_nombre": "Nombre Apellido",
  "responsable_email": "correo@dominio.com",
  "observaciones": "Opcional",
  "productos": [
    {
      "producto_id": 51,
      "cantidad_solicitada": 10,
      "unidad_medida": "UND"
    }
  ]
}
```

### Payload de entrega

```json
{
  "solicitud_id": 10,
  "fecha": "2026-08-03",
  "hora": "09:00",
  "sede_id": 1,
  "responsable_nombre": "Nombre Apellido",
  "productos": [
    {
      "producto_id": 51,
      "cantidad_entregada": 6,
      "unidad_medida": "UND"
    }
  ]
}
```

`solicitud_id` puede ser `null` para entregas historicas sin relacion confiable.

### Reglas principales

- Las cantidades deben ser mayores que cero.
- No se permite repetir productos dentro del mismo formulario.
- `productos.unidad_primaria` puede ser `NULL`; en ese caso la interfaz exige seleccionar una unidad valida.
- El catalogo de productos del modulo solo lista codigos activos cuyo `codigo_producto` normalizado empieza por `PT` o `ST`.
- La unidad seleccionada se guarda como copia historica en el detalle.
- `LTS` se normaliza como `LT`.
- No se deduce fresco/empaquetado ni unidad por nombre o codigo del producto.
- Las entregas relacionadas no se aceptan si la solicitud esta `CANCELADA` o `COMPLETADA`.
- El estado queda `PENDIENTE`, `PARCIAL`, `COMPLETADA` o `CANCELADA`.
- La cantidad pendiente visible nunca se muestra negativa aunque exista sobreentrega controlada.

### Transacciones

Solicitud:

```text
BEGIN -> cabecera -> detalles -> evento outbox -> COMMIT -> intento Sheets
```

Entrega:

```text
BEGIN -> SELECT solicitud FOR UPDATE -> cabecera -> detalles -> recalcular estado -> evento outbox -> COMMIT -> intento Sheets
```

El bloqueo `FOR UPDATE` evita carreras cuando dos usuarios registran entregas de la misma solicitud al mismo tiempo.

### Outbox de Google Sheets

El script `scripts/migration-solicitudes-sedes-outbox.sql` crea solo la tabla tecnica `solicitudes_sedes_sheets_outbox`. No modifica ni recrea las tablas de negocio.

Flujo:

```text
guardar en PostgreSQL -> crear outbox en la misma transaccion -> intentar Sheets despues del COMMIT -> marcar sincronizado/error -> permitir reintento
```

Los reintentos usan `FOR UPDATE SKIP LOCKED` para evitar que dos procesos tomen el mismo evento.

### Apps Script

Usa `scripts/google_apps_script_solicitudes_sedes.gs` como plantilla. Propiedades recomendadas:

- `SOLICITUDES_SHEETS_WEBHOOK_SECRET`
- `SOLICITUDES_SPREADSHEET_ID` opcional si no se usa el spreadsheet activo.
- `SOLICITUDES_SHEET_SOLICITUDES` opcional.
- `SOLICITUDES_SHEET_ENTREGAS` opcional.

El script usa `LockService`, valida secreto compartido y actualiza por `row_reference` para no duplicar filas.

### SPanel futuro

1. Subir cambios al repositorio.
2. En SPanel, hacer `git pull` del backend y frontend.
3. Instalar dependencias si cambiaron.
4. Revisar `scripts/inspect-solicitudes-sedes.sql` en phpPgAdmin.
5. Aplicar manualmente `scripts/migration-solicitudes-sedes-outbox.sql` si se activara Sheets.
6. Configurar variables `.env` sin credenciales en Git.
7. Reiniciar la app Node.js desde SPanel.
