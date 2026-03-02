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

## 5) Despliegue en Render

- Root Directory: dejar vacío (este repo ya es solo backend).
- Build Command: `npm install`
- Start Command: `npm start`
- Variables: `DATABASE_URL`, `NODE_ENV=production`, `ADMIN_KEY`, `CORS_ORIGIN`
