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

## 4) Endpoints principales

- `GET /health`
- `GET /destinos`
- `GET /sedes`
- `GET /responsables`
- `GET /productos`
- `POST /productos`
- `DELETE /productos/:codigo`
- `POST /api/empaquetados`
- `POST /api/mermas`
- `GET /api/registros?tipo=Empaquetado|Merma&limit=20`
- `POST /api/control-inventario`
- `GET /api/almacen09/stock-actual`
- `POST /api/almacen09/salidas-facturas`
- `GET /api/almacen09/salidas-facturas?limit=100`

## 5) Mapa front -> tablas SQL (Neon)

- Empaquetado (form principal):
	- `empaquetados_cabecera`
	- `empaquetados_detalle`
	- catálogo auxiliar: `productos`, `destinos`, `responsables`, `sedes`
- Almacén09 Entradas (validación de conteo):
	- `almacen_lotes_procesados`
	- `conteo_errores`
	- referencia de origen: `empaquetados_cabecera`, `empaquetados_detalle`
- Almacén09 Salidas (facturación):
	- `almacen09_salidas_facturas`
	- `almacen09_salidas_detalle`
- Control de Inventario (cambio de guardia):
	- `control_inventario_guardia`
	- referencia de producto: `productos`
- Histórico consolidado:
	- `historico_resultados_consolidado`

Notas:
- `lotes` y `lote_productos` son tablas legacy y se eliminan automáticamente al iniciar el backend para evitar confusión.
- El endpoint de stock (`/api/almacen09/stock-actual`) ya descuenta lo facturado en Salidas09.

## 6) Despliegue en Render

- Root Directory: dejar vacío (este repo ya es solo backend).
- Build Command: `npm install`
- Start Command: `npm start`
- Variables: `DATABASE_URL`, `NODE_ENV=production`, `ADMIN_KEY`, `CORS_ORIGIN`
