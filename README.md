# Backend Node + Neon (Render)

## 1) Variables de entorno

Copia `.env.example` a `.env` y completa:

- `DATABASE_URL`: string de conexión de Neon.
- `PORT`: puerto local (ejemplo `3001`).
- `NODE_ENV`: `development` o `production`.
- `ADMIN_KEY`: clave para crear/eliminar productos.
- `CORS_ORIGIN`: dominios permitidos separados por coma o `*`.

## 2) Ejecutar local

```bash
npm install
npm run dev
```

## 3) Inicializar DB en Neon

Ejecuta el archivo `schema.sql` en tu proyecto de Neon.

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

## 5) Estructura Neon optimizada

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
	- `almacen09_salidas_facturas`: cabecera de facturas.
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
- Variables: `DATABASE_URL`, `NODE_ENV=production`, `ADMIN_KEY`, `CORS_ORIGIN`
