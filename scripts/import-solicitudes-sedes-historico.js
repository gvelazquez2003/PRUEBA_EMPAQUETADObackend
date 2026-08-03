import fs from 'fs/promises';
import path from 'path';

const ALLOWED_UNITS = new Set(['UND', 'KG', 'PAQ', 'LT', 'CAJ', 'BLT', 'ENV', 'SAC', 'RLL', 'BOLSA', 'CESTA', 'BULTO']);

function parseArgs(argv) {
  const args = { dryRun: true };
  for (let i = 2; i < argv.length; i += 1) {
    const item = argv[i];
    if (item === '--apply') args.dryRun = false;
    else if (item === '--dry-run') args.dryRun = true;
    else if (item === '--file') args.file = argv[i += 1];
    else if (item.startsWith('--file=')) args.file = item.slice('--file='.length);
  }
  return args;
}

function parseCsvLine(line) {
  const values = [];
  let current = '';
  let quoted = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    const next = line[i + 1];
    if (ch === '"' && quoted && next === '"') {
      current += '"';
      i += 1;
    } else if (ch === '"') {
      quoted = !quoted;
    } else if (ch === ',' && !quoted) {
      values.push(current);
      current = '';
    } else {
      current += ch;
    }
  }
  values.push(current);
  return values.map((value) => value.trim());
}

function normalizeHeader(value) {
  return String(value || '')
    .trim()
    .toLowerCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');
}

function normalizeUnit(value) {
  const clean = String(value || '').trim().toUpperCase().replace(/\s+/g, '');
  const normalized = clean === 'LTS' ? 'LT' : clean;
  return normalized && ALLOWED_UNITS.has(normalized) ? normalized : '';
}

function parseBusinessDate(value) {
  const raw = String(value || '').trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) return { ok: true, iso: raw };
  const latin = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (!latin) return { ok: false };
  const day = Number(latin[1]);
  const month = Number(latin[2]);
  const year = Number(latin[3]);
  const iso = `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
  const date = new Date(`${iso}T00:00:00Z`);
  return Number.isFinite(date.getTime()) && date.toISOString().slice(0, 10) === iso
    ? { ok: true, iso }
    : { ok: false };
}

async function main() {
  const args = parseArgs(process.argv);
  if (!args.file) {
    console.error('Uso: node scripts/import-solicitudes-sedes-historico.js --file export.csv [--dry-run|--apply]');
    process.exitCode = 1;
    return;
  }
  const filePath = path.resolve(args.file);
  const content = await fs.readFile(filePath, 'utf8');
  const lines = content.split(/\r?\n/).filter((line) => line.trim());
  const headers = parseCsvLine(lines.shift() || '').map(normalizeHeader);
  const required = ['tipo', 'fecha', 'sede', 'codigo_producto', 'cantidad'];
  const missing = required.filter((header) => !headers.includes(header));
  const rejected = [];
  const duplicateKeys = new Map();
  let accepted = 0;
  if (missing.length) {
    console.error(`Encabezados faltantes: ${missing.join(', ')}`);
    process.exitCode = 1;
    return;
  }
  lines.forEach((line, index) => {
    const row = Object.fromEntries(parseCsvLine(line).map((value, i) => [headers[i], value]));
    const errors = [];
    const type = String(row.tipo || '').trim().toLowerCase();
    const parsedDate = parseBusinessDate(row.fecha);
    const unit = normalizeUnit(row.unidad_medida || row.unidad || '');
    if (!parsedDate.ok) errors.push('fecha invalida');
    if (!['solicitud', 'entrega'].includes(type)) errors.push('tipo invalido');
    if (!(Number(row.cantidad) > 0)) errors.push('cantidad invalida');
    if (!String(row.codigo_producto || '').trim()) errors.push('codigo_producto vacio');
    if ((row.unidad_medida || row.unidad) && !unit) errors.push('unidad invalida');
    if (type === 'entrega' && row.solicitud_id && !(Number(row.solicitud_id) > 0)) errors.push('solicitud_id invalido');

    const key = [
      type,
      parsedDate.iso || row.fecha || '',
      String(row.sede || '').trim().toUpperCase(),
      String(row.codigo_producto || '').trim().toUpperCase(),
      String(row.cantidad || '').trim(),
      unit,
      String(row.solicitud_id || '').trim(),
    ].join('|');
    if (duplicateKeys.has(key)) {
      errors.push(`duplicado exacto de fila ${duplicateKeys.get(key)}`);
    } else {
      duplicateKeys.set(key, index + 2);
    }

    if (errors.length) rejected.push({ row: index + 2, errors });
    else accepted += 1;
  });
  console.log(JSON.stringify({
    ok: rejected.length === 0,
    mode: args.dryRun ? 'dry-run' : 'apply-disabled',
    total_rows: lines.length,
    accepted,
    rejected,
    message: args.dryRun
      ? 'Validacion completada. No se inserto nada.'
      : 'Modo apply aun requiere conexion/revision manual antes de insertar.',
  }, null, 2));
}

main().catch((error) => {
  console.error(error.message || error);
  process.exitCode = 1;
});
