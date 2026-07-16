const app = global.__PDT_VRP_APP;
const pool = global.__PDT_VRP_POOL;

if (!app || !pool) {
    throw new Error("VRP integrado requiere app y pool del backend principal.");
}
function cleanEnvValue(value) {
    let cleaned = String(value || "").trim();
    if ((cleaned.startsWith('"') && cleaned.endsWith('"')) || (cleaned.startsWith("'") && cleaned.endsWith("'"))) {
        cleaned = cleaned.slice(1, -1).trim();
    }
    return cleaned;
}

function parsePositiveIntEnv(value, fallback) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? Math.max(0, Math.trunc(parsed)) : fallback;
}

const GOOGLE_MAPS_API_KEY = cleanEnvValue(process.env.GOOGLE_MAPS_SERVER_API_KEY || process.env.GOOGLE_MAPS_API_KEY || "");
const GOOGLE_MAPS_BROWSER_API_KEY = cleanEnvValue(process.env.GOOGLE_MAPS_BROWSER_API_KEY || "");
const GOOGLE_GEOCODING_API_KEY = cleanEnvValue(process.env.GOOGLE_GEOCODING_API_KEY || process.env.GOOGLE_GEOCODING_FALLBACK_API_KEY || "");
const OPENROUTESERVICE_API_KEY = cleanEnvValue(process.env.OPENROUTESERVICE_API_KEY || process.env.ORS_API_KEY || "");
const ROUTING_PROVIDER = cleanEnvValue(
    process.env.ROUTING_PROVIDER || (OPENROUTESERVICE_API_KEY ? "openrouteservice" : "google")
).toLowerCase();
const GOOGLE_GEOCODING_FALLBACK_ENABLED = ["1", "true", "yes", "si", "sí"].includes(
    normalizeHeader(process.env.GOOGLE_GEOCODING_FALLBACK_ENABLED || "")
);
const GOOGLE_GEOCODING_DAILY_LIMIT = parsePositiveIntEnv(process.env.GOOGLE_GEOCODING_DAILY_LIMIT || 250, 250);
const GOOGLE_GEOCODING_MONTHLY_LIMIT = parsePositiveIntEnv(process.env.GOOGLE_GEOCODING_MONTHLY_LIMIT || 9000, 9000);
const GOOGLE_GEOCODING_CACHE_DAYS = Math.max(1, parsePositiveIntEnv(process.env.GOOGLE_GEOCODING_CACHE_DAYS || 30, 30));
const DISTRIBUTION_ORIGIN_NAME = process.env.DISTRIBUTION_ORIGIN_NAME || "PDT Bello Campo";
const DISTRIBUTION_ORIGIN = process.env.DISTRIBUTION_ORIGIN || "Edificio Onnis, Avenida Francisco de Miranda, & Avenida Coromoto, Caracas 1060, Miranda, Venezuela";
const DATABASE_URL = cleanEnvValue(process.env.DATABASE_URL || "");
const FRONTEND_ORIGIN = process.env.FRONTEND_ORIGIN || "*";
const SOURCE_TABLE = cleanEnvValue(process.env.SOURCE_TABLE || "hojas_ruta_exportadas");
const AUTO_DEPLOY_ON_DB_CHANGE = String(process.env.AUTO_DEPLOY_ON_DB_CHANGE || "false").toLowerCase() === "true";
const RENDER_DEPLOY_HOOK_URL = process.env.RENDER_DEPLOY_HOOK_URL || "";
const DB_WATCH_INTERVAL_MS = Number(process.env.DB_WATCH_INTERVAL_MS || 120000);
const AUTO_DEPLOY_COOLDOWN_MS = Number(process.env.AUTO_DEPLOY_COOLDOWN_MS || 600000);
const DB_CHANGE_WATCH_QUERY = process.env.DB_CHANGE_WATCH_QUERY || "";
const configuredExactOptimizationMaxStops = Number(process.env.EXACT_OPTIMIZATION_MAX_STOPS || 14);
const EXACT_OPTIMIZATION_MAX_STOPS = Number.isFinite(configuredExactOptimizationMaxStops)
    ? Math.max(1, Math.min(18, Math.trunc(configuredExactOptimizationMaxStops)))
    : 14;
const TRAFFIC_OPTIMAL_MATRIX_MAX_STOPS = 9;

function getRoutingProvider() {
    if (["openrouteservice", "ors", "openroute"].includes(ROUTING_PROVIDER)) return "openrouteservice";
    return "google";
}

function getRoutingProviderLabel() {
    return getRoutingProvider() === "openrouteservice" ? "OpenRouteService" : "Google Maps";
}

function normalizeHeader(value) {
    return String(value || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .trim()
        .toLowerCase()
        .replace(/\s+/g, " ");
}

function normalizeText(value) {
    return String(value || "").trim();
}

function normalizeUsername(value) {
    return String(value || "")
        .normalize("NFD")
        .replace(/[\u0300-\u036f]/g, "")
        .toUpperCase()
        .replace(/[^A-Z0-9]/g, "")
        .slice(0, 20);
}

function normalizeRole(value) {
    const role = normalizeHeader(value).replace(/\s+/g, "_");
    if (role === "administrador" || role === "admin") return "administrador";
    if (role === "conductor" || role === "chofer" || role === "driver") return "conductor";
    if (role === "oplogistico") return "conductor";
    return role;
}

function getRequestToken(req) {
    const authHeader = String(req.headers.authorization || "").trim();
    const match = /^Bearer\s+(.+)$/i.exec(authHeader);
    if (match && match[1]) return String(match[1]).trim();
    return normalizeText(req.query?.token || req.body?.token || "");
}

async function getRequestAuthContext(req) {
    const token = getRequestToken(req);
    if (!token) return null;
    try {
        const result = await pool.query(
            `SELECT
                u.username,
                u.role,
                COALESCE(u.full_name, '') AS full_name,
                COALESCE(u.vehicle_plate, '') AS vehicle_plate
             FROM auth_sessions s
             JOIN auth_users u ON u.id_user = s.id_user
             WHERE s.token = $1
               AND s.revoked_at IS NULL
               AND (s.expires_at IS NULL OR s.expires_at > NOW())
               AND u.activo = TRUE
             LIMIT 1`,
            [token]
        );
        if (!result.rowCount) return null;
        const row = result.rows[0];
        return {
            username: normalizeUsername(row.username),
            role: normalizeRole(row.role),
            fullName: normalizeText(row.full_name),
            vehiclePlate: normalizeUsername(row.vehicle_plate)
        };
    } catch (error) {
        console.error("No se pudo validar sesion VRP:", error.message || error);
        return null;
    }
}

async function requireRequestAuthContext(req, res) {
    const auth = await getRequestAuthContext(req);
    if (!auth) {
        res.status(401).json({ ok: false, error: "Sesion requerida para consultar rutas." });
        return null;
    }
    return auth;
}

function isConductorRestricted(auth) {
    const username = normalizeUsername(auth?.username);
    return normalizeRole(auth?.role) === "conductor" && username && username !== "OPLOGISTICO";
}

function appendConductorSheetFilter(whereParts, values, auth) {
    if (!isConductorRestricted(auth)) return;
    values.push(`%_${normalizeUsername(auth.username)}`);
    whereParts.push(`REGEXP_REPLACE(UPPER(TRIM(COALESCE(nombre_archivo, ''))), '[^A-Z0-9_]', '', 'g') LIKE $${values.length}`);
}

function quoteIdent(identifier) {
    return `"${String(identifier).replace(/"/g, "\"\"")}"`;
}

function parseTableRef(tableRef) {
    const parts = String(tableRef || "").split(".");
    if (parts.length === 2) return { schema: parts[0], table: parts[1] };
    return { schema: "public", table: String(tableRef || "") };
}

function pickColumn(columns, candidates) {
    const normalized = columns.map((col) => ({ original: col, normalized: normalizeHeader(col) }));
    for (const candidate of candidates) {
        const found = normalized.find((item) => item.normalized === normalizeHeader(candidate));
        if (found) return found.original;
    }
    return null;
}

function pickColumnByContains(columns, fragments) {
    const normalized = columns.map((col) => ({ original: col, normalized: normalizeHeader(col) }));
    const found = normalized.find((item) =>
        fragments.every((fragment) => item.normalized.includes(normalizeHeader(fragment)))
    );
    return found ? found.original : null;
}

function sqlExpr(columnName, fallback = "") {
    if (!columnName) return `'${fallback}'`;
    return `COALESCE(TRIM(${quoteIdent(columnName)}::text), '')`;
}

function makeClientKey(clientId, route, address) {
    return [normalizeText(clientId), normalizeText(route), normalizeText(address)].join("::");
}

function isRouteSheetSource(columns) {
    const normalized = columns.map(normalizeHeader);
    return normalized.includes("id_hoja") && normalized.includes("facturas");
}

function makeRouteKey(sheetId) {
    return `hoja:${normalizeText(sheetId)}`;
}

function parseRouteKey(route) {
    const value = normalizeText(route);
    if (value.toLowerCase().startsWith("hoja:")) return value.slice(5);
    return "";
}

function formatDateValue(value) {
    if (!value) return "";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return normalizeText(value);
    return date.toISOString().slice(0, 10);
}

function makeRouteDisplayName(sheet) {
    const routeName = normalizeText(sheet.ruta_nombre) || "SIN RUTA";
    const date = formatDateValue(sheet.fecha_entrega);
    return date ? `${routeName} - ${date}` : routeName;
}

function normalizeDeliveryAddress(address) {
    const value = normalizeText(address);
    if (!value) return "";
    const comparable = normalizeHeader(value);
    if (comparable.includes("venezuela")) return value;
    return `${value}, Venezuela`;
}

async function ensureDatabaseReady() {
    await pool.query(`
        CREATE TABLE IF NOT EXISTS client_overrides (
            client_key TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            address TEXT NOT NULL DEFAULT '',
            route_name TEXT NOT NULL DEFAULT '',
            transport TEXT NOT NULL DEFAULT '',
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS delivery_status (
            client_key TEXT PRIMARY KEY,
            delivered BOOLEAN NOT NULL DEFAULT FALSE,
            delivered_baskets INT NOT NULL DEFAULT 0,
            delivered_at TIMESTAMPTZ,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        ALTER TABLE delivery_status
        ADD COLUMN IF NOT EXISTS delivered_baskets INT NOT NULL DEFAULT 0
    `);
    await pool.query(`
        ALTER TABLE delivery_status
        ADD COLUMN IF NOT EXISTS partial BOOLEAN NOT NULL DEFAULT FALSE
    `);
    await pool.query(`
        ALTER TABLE delivery_status
        ADD COLUMN IF NOT EXISTS partial_detail JSONB
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS address_validations (
            client_key TEXT PRIMARY KEY,
            address TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            reason TEXT NOT NULL DEFAULT '',
            formatted_address TEXT NOT NULL DEFAULT '',
            location_type TEXT NOT NULL DEFAULT '',
            partial_match BOOLEAN NOT NULL DEFAULT FALSE,
            checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_address_validations_address
        ON address_validations (address)
    `);
    await pool.query(`
        ALTER TABLE address_validations
        ADD COLUMN IF NOT EXISTS provider TEXT NOT NULL DEFAULT ''
    `);
    await pool.query(`
        ALTER TABLE address_validations
        ADD COLUMN IF NOT EXISTS latitude DOUBLE PRECISION
    `);
    await pool.query(`
        ALTER TABLE address_validations
        ADD COLUMN IF NOT EXISTS longitude DOUBLE PRECISION
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS routing_geocoding_usage (
            id BIGSERIAL PRIMARY KEY,
            provider TEXT NOT NULL DEFAULT '',
            address TEXT NOT NULL DEFAULT '',
            success BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE INDEX IF NOT EXISTS idx_routing_geocoding_usage_provider_created
        ON routing_geocoding_usage (provider, created_at)
    `);
}

async function getSourceColumns() {
    const { schema, table } = parseTableRef(SOURCE_TABLE);
    const result = await pool.query(
        `SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2`,
        [schema, table]
    );
    return result.rows.map((row) => row.column_name);
}

async function fetchRouteSheets(routeFilter = "", auth = null) {
    const { schema, table } = parseTableRef(SOURCE_TABLE);
    const tableRef = `${quoteIdent(schema)}.${quoteIdent(table)}`;
    const routeSheetId = parseRouteKey(routeFilter);
    const values = [];
    const whereParts = [];
    if (routeSheetId) {
        values.push(routeSheetId);
        whereParts.push(`id_hoja::text = $${values.length}`);
    }
    appendConductorSheetFilter(whereParts, values, auth);
    const where = whereParts.length ? `WHERE ${whereParts.join(" AND ")}` : "";

    const result = await pool.query(
        `SELECT
            id_hoja::text,
            ruta_nombre,
            fecha_entrega,
            conductor,
            numero_camion,
            total_despachos,
            total_cestas,
            usuario,
            nombre_archivo,
            COALESCE(facturas, '[]'::jsonb) AS facturas
         FROM ${tableRef}
         ${where}
         ORDER BY fecha_entrega DESC, id_hoja DESC`,
        values
    );
    return result.rows;
}

function flattenRouteSheetClients(sheets) {
    const clients = [];
    sheets.forEach((sheet) => {
        const routeKey = makeRouteKey(sheet.id_hoja);
        const routeName = normalizeText(sheet.ruta_nombre) || "SIN RUTA";
        const routeDisplayName = makeRouteDisplayName(sheet);
        const facturas = Array.isArray(sheet.facturas) ? sheet.facturas : [];

        facturas.forEach((invoice, index) => {
            const clientId = normalizeText(invoice.numero_control || invoice.id_factura || index + 1);
            const address = normalizeDeliveryAddress(invoice.direccion_texto);
            const name = normalizeText(invoice.cliente_nombre);
            const key = makeClientKey(
                `${sheet.id_hoja}:${clientId}:${normalizeText(invoice.numero_factura || invoice.id_factura || index + 1)}`,
                routeKey,
                address
            );

            clients.push({
                key,
                sheet: "hojas_ruta_exportadas",
                sheetId: String(sheet.id_hoja),
                rowNumber: index + 1,
                clientId,
                invoiceId: normalizeText(invoice.id_factura),
                invoiceNumber: normalizeText(invoice.numero_factura),
                controlNumber: normalizeText(invoice.numero_control),
                name,
                nombre_o_razon_social: name,
                address,
                originalAddress: normalizeText(invoice.direccion_texto),
                route: routeKey,
                routeName,
                routeDisplayName,
                zone: normalizeText(invoice.zona_nombre),
                transport: normalizeText(invoice.transporte_nombre || sheet.numero_camion),
                driver: normalizeText(sheet.conductor),
                truck: normalizeText(sheet.numero_camion),
                deliveryDate: formatDateValue(sheet.fecha_entrega),
                totalDispatches: Number(sheet.total_despachos || facturas.length || 0),
                totalBaskets: Number(sheet.total_cestas || 0),
                baskets: Number(invoice.total_cestas || invoice.cestas || invoice.cantidad_cestas || 0),
                delivered: invoice.entregado === true,
                detail: Array.isArray(invoice.detalle) ? invoice.detalle : []
            });
        });
    });
    return clients;
}

async function fetchSourceClients(routeFilter, auth = null) {
    const columns = await getSourceColumns();
    if (!columns.length) throw new Error(`No existe la tabla ${SOURCE_TABLE} en PostgreSQL.`);

    if (isRouteSheetSource(columns)) {
        const sheets = await fetchRouteSheets(routeFilter, auth);
        return flattenRouteSheetClients(sheets);
    }

    const idCol = pickColumn(columns, ["CLIENTES", "CIENTES", "CLIENTE ID", "ID CLIENTE"]);
    const nameCol = pickColumn(columns, [
        "NOMBRE O RAZON SOCIAL",
        "NOMBRE_O_RAZON_SOCIAL",
        "NOMBRE O RAZÓN SOCIAL",
        "NOMBRE_O_RAZÓN_SOCIAL",
        "NOMBRE"
    ]) || pickColumnByContains(columns, ["nombre", "razon"]) || pickColumnByContains(columns, ["nombre"]);
    const addressCol = pickColumn(columns, ["DIRECCION", "DIRECCIÓN"]);
    const routeCol = pickColumn(columns, ["RUTA", "RUTA ASIGNADA"]);
    const transportCol = pickColumn(columns, ["TRANSPORTE"]);

    const { schema, table } = parseTableRef(SOURCE_TABLE);
    const values = [];
    const where = routeFilter ? `WHERE ${sqlExpr(routeCol)} = $1` : "";
    if (routeFilter) values.push(routeFilter);

    const query = `
        SELECT
            ROW_NUMBER() OVER ()::int AS row_number,
            ${sqlExpr(idCol)} AS client_id,
            ${sqlExpr(nameCol)} AS name,
            ${sqlExpr(addressCol)} AS address,
            ${sqlExpr(routeCol)} AS route_name,
            ${sqlExpr(transportCol)} AS transport
        FROM ${quoteIdent(schema)}.${quoteIdent(table)}
        ${where}
    `;

    const result = await pool.query(query, values);
    return result.rows.map((row) => ({
        key: makeClientKey(row.client_id, row.route_name, row.address),
        sheet: "NEON",
        rowNumber: row.row_number,
        clientId: row.client_id,
        name: row.name,
        nombre_o_razon_social: row.name,
        address: row.address,
        route: row.route_name,
        transport: row.transport
    }));
}

async function getOverridesMap() {
    const result = await pool.query("SELECT client_key, name, address, route_name, transport FROM client_overrides");
    const map = new Map();
    result.rows.forEach((row) => map.set(row.client_key, row));
    return map;
}

async function getDeliveryStatusMap() {
    const result = await pool.query("SELECT client_key, delivered, delivered_baskets, delivered_at, partial, partial_detail FROM delivery_status");
    const map = new Map();
    result.rows.forEach((row) => map.set(row.client_key, row));
    return map;
}

async function getAddressValidationsMap() {
    const result = await pool.query(`
        SELECT
            client_key,
            address,
            status,
            reason,
            formatted_address,
            location_type,
            partial_match,
            provider,
            latitude,
            longitude,
            checked_at
        FROM address_validations
    `);
    const map = new Map();
    result.rows.forEach((row) => map.set(row.client_key, row));
    return map;
}

function withAddressValidation(client, validation) {
    if (!validation || normalizeText(validation.address) !== normalizeText(client.address)) return client;
    const status = normalizeText(validation.status);
    const latitude = Number(validation.latitude);
    const longitude = Number(validation.longitude);
    const hasLocation = Number.isFinite(latitude) && Number.isFinite(longitude);
    return {
        ...client,
        googleAddressStatus: status,
        googleAddressIssue: Boolean(status && status !== "valid"),
        googleAddressReason: normalizeText(validation.reason),
        googleAddressFormatted: normalizeText(validation.formatted_address),
        googleAddressLocationType: normalizeText(validation.location_type),
        googleAddressPartialMatch: Boolean(validation.partial_match),
        googleAddressCheckedAt: validation.checked_at || null,
        routingProvider: normalizeText(validation.provider),
        routingLocation: hasLocation ? { lat: latitude, lng: longitude } : null
    };
}

async function getClients(route, auth = null) {
    const base = await fetchSourceClients(route, auth);
    const overrides = await getOverridesMap();
    const deliveryStatuses = await getDeliveryStatusMap();
    const addressValidations = await getAddressValidationsMap();
    const merged = base.map((client) => {
        const override = overrides.get(client.key);
        const deliveryStatus = deliveryStatuses.get(client.key);
        const delivery = {
            delivered: deliveryStatus ? Boolean(deliveryStatus.delivered) : Boolean(client.delivered),
            deliveredBaskets: deliveryStatus ? Number(deliveryStatus.delivered_baskets || 0) : null,
            deliveredAt: deliveryStatus?.delivered_at || null,
            partial: deliveryStatus ? Boolean(deliveryStatus.partial) : false,
            partialDetail: deliveryStatus?.partial_detail || null
        };
        if (!override) return withAddressValidation({ ...client, ...delivery }, addressValidations.get(client.key));
        const name = normalizeText(override.name || client.name);
        return withAddressValidation({
            ...client,
            ...delivery,
            name,
            nombre_o_razon_social: name,
            address: normalizeText(override.address || client.address),
            route: normalizeText(override.route_name || client.route),
            routeName: normalizeText(override.route_name || client.routeName),
            transport: normalizeText(override.transport || client.transport)
        }, addressValidations.get(client.key));
    });
    if (!route) return merged;
    return merged.filter((client) => normalizeText(client.route) === normalizeText(route));
}

function isClientWithErrors(client) {
    const route = normalizeHeader(client.route);
    const missingFields = !client.clientId || !client.name || !client.address || !client.route;
    return missingFields || route.includes("revisar manualmente") || client.googleAddressIssue === true;
}

function hasValidationCoordinates(validation) {
    const lat = Number(validation?.latitude);
    const lng = Number(validation?.longitude);
    return Number.isFinite(lat) && Number.isFinite(lng);
}

function isGoogleGeocodingFallbackProvider(provider) {
    return normalizeHeader(provider).replace(/-/g, "_") === "google_geocoding_fallback";
}

function isValidationFresh(validation, maxAgeDays) {
    const checkedAt = new Date(validation?.checked_at || 0).getTime();
    if (!Number.isFinite(checkedAt) || checkedAt <= 0) return false;
    const maxAgeMs = Math.max(1, Number(maxAgeDays || 1)) * 24 * 60 * 60 * 1000;
    return Date.now() - checkedAt <= maxAgeMs;
}

function validationMatchesRoutingProvider(validation) {
    if (!validation) return false;
    if (normalizeText(validation.address) === "") return false;
    if (getRoutingProvider() !== "openrouteservice") return true;
    const provider = normalizeText(validation.provider);
    if (provider === "openrouteservice") return hasValidationCoordinates(validation);
    if (isGoogleGeocodingFallbackProvider(provider)) {
        const fresh = isValidationFresh(validation, GOOGLE_GEOCODING_CACHE_DAYS);
        return fresh && (hasValidationCoordinates(validation) || normalizeText(validation.status) === "not_found");
    }
    return false;
}

async function routeStats(auth = null) {
    const columns = await getSourceColumns();
    if (isRouteSheetSource(columns)) {
        const sheets = await fetchRouteSheets("", auth);
        const deliveryStatuses = await getDeliveryStatusMap();
        return sheets.map((sheet) => {
            const facturas = Array.isArray(sheet.facturas) ? sheet.facturas : [];
            const clients = flattenRouteSheetClients([sheet]);
            const totals = clients.reduce((acc, client) => {
                const deliveryStatus = deliveryStatuses.get(client.key);
                const delivered = deliveryStatus ? Boolean(deliveryStatus.delivered) : Boolean(client.delivered);
                const partial = deliveryStatus ? Boolean(deliveryStatus.partial) && !delivered : false;
                acc.delivered += delivered ? 1 : 0;
                acc.partial += partial ? 1 : 0;
                return acc;
            }, { delivered: 0, partial: 0 });
            const totalClients = facturas.length;
            const completedClients = totals.delivered + totals.partial;
            const pendingClients = Math.max(0, totalClients - completedClients);
            const progressPercent = totalClients ? Math.round((completedClients / totalClients) * 100) : 0;
            return {
                route: makeRouteKey(sheet.id_hoja),
                routeName: normalizeText(sheet.ruta_nombre) || "SIN RUTA",
                displayName: makeRouteDisplayName(sheet),
                sheetId: String(sheet.id_hoja),
                deliveryDate: formatDateValue(sheet.fecha_entrega),
                driver: normalizeText(sheet.conductor),
                truck: normalizeText(sheet.numero_camion),
                totalClients,
                deliveredClients: totals.delivered,
                partialClients: totals.partial,
                completedClients,
                pendingClients,
                progressPercent,
                completed: totalClients > 0 && pendingClients === 0,
                totalDispatches: Number(sheet.total_despachos || facturas.length || 0),
                totalBaskets: Number(sheet.total_cestas || 0)
            };
        });
    }

    const clients = await getClients("", auth);
    const grouped = new Map();
    clients.forEach((client) => {
        const route = client.route || "SIN RUTA";
        const current = grouped.get(route) || {
            route,
            totalClients: 0,
            deliveredClients: 0,
            partialClients: 0
        };
        current.totalClients += 1;
        current.deliveredClients += client.delivered ? 1 : 0;
        current.partialClients += client.partial && !client.delivered ? 1 : 0;
        grouped.set(route, current);
    });
    return Array.from(grouped.values())
        .map((item) => {
            const completedClients = item.deliveredClients + item.partialClients;
            const pendingClients = Math.max(0, item.totalClients - completedClients);
            return {
                ...item,
                routeName: item.route,
                displayName: item.route,
                completedClients,
                pendingClients,
                progressPercent: item.totalClients ? Math.round((completedClients / item.totalClients) * 100) : 0,
                completed: item.totalClients > 0 && pendingClients === 0
            };
        })
        .sort((a, b) => a.route.localeCompare(b.route));
}

async function saveClientOverride(key, data) {
    await pool.query(
        `INSERT INTO client_overrides (client_key, name, address, route_name, transport, updated_at)
         VALUES ($1, $2, $3, $4, $5, NOW())
         ON CONFLICT (client_key) DO UPDATE
         SET name = EXCLUDED.name,
             address = EXCLUDED.address,
             route_name = EXCLUDED.route_name,
             transport = EXCLUDED.transport,
             updated_at = NOW()`,
        [key, data.name, data.address, data.route, data.transport]
    );
    await pool.query("DELETE FROM address_validations WHERE client_key = $1", [key]);
}

async function saveDeliveryStatus(key, delivered, deliveredBaskets, partial, partialDetail) {
    const baskets = Math.max(0, Math.trunc(Number(deliveredBaskets || 0)));
    const isPartial = Boolean(partial) && !Boolean(delivered);
    const detailJson = isPartial && Array.isArray(partialDetail) ? JSON.stringify(partialDetail) : null;
    await pool.query(
        `INSERT INTO delivery_status (client_key, delivered, delivered_baskets, delivered_at, partial, partial_detail, updated_at)
         VALUES ($1, $2::boolean, $3::int, CASE WHEN $2::boolean THEN NOW() ELSE NULL END, $4::boolean, $5::jsonb, NOW())
         ON CONFLICT (client_key) DO UPDATE
         SET delivered = EXCLUDED.delivered,
             delivered_baskets = EXCLUDED.delivered_baskets,
             delivered_at = CASE WHEN EXCLUDED.delivered THEN COALESCE(delivery_status.delivered_at, NOW()) ELSE NULL END,
             partial = EXCLUDED.partial,
             partial_detail = EXCLUDED.partial_detail,
             updated_at = NOW()`,
        [key, Boolean(delivered), baskets, isPartial, detailJson]
    );
}

function classifyGeocodingResult(payload) {
    if (payload?.status === "ZERO_RESULTS") {
        return {
            provider: "google",
            status: "not_found",
            reason: "Google Maps no encontro esta direccion.",
            formattedAddress: "",
            locationType: "",
            partialMatch: false,
            latitude: null,
            longitude: null
        };
    }
    if (payload?.status !== "OK") {
        throw new Error(`Google Geocoding API: ${payload?.status || "respuesta invalida"}. Activa Geocoding API para la clave de servidor.`);
    }
    if (!Array.isArray(payload.results) || !payload.results.length) {
        return {
            provider: "google",
            status: "not_found",
            reason: "Google Maps no encontro esta direccion.",
            formattedAddress: "",
            locationType: "",
            partialMatch: false,
            latitude: null,
            longitude: null
        };
    }

    const result = payload.results[0] || {};
    const locationType = normalizeText(result.geometry?.location_type);
    const latitude = Number(result.geometry?.location?.lat);
    const longitude = Number(result.geometry?.location?.lng);
    const hasCoordinates = Number.isFinite(latitude) && Number.isFinite(longitude);
    if (!hasCoordinates) {
        return {
            provider: "google",
            status: "not_found",
            reason: "Google Maps encontro la direccion, pero no devolvio coordenadas validas.",
            formattedAddress: normalizeText(result.formatted_address),
            locationType,
            partialMatch: false,
            latitude: null,
            longitude: null
        };
    }
    if (result.partial_match === true) {
        return {
            provider: "google",
            status: "partial_match",
            reason: "Google Maps solo encontro una coincidencia parcial. Revisa calle, edificio y ciudad.",
            formattedAddress: normalizeText(result.formatted_address),
            locationType,
            partialMatch: true,
            latitude,
            longitude
        };
    }
    if (["APPROXIMATE", "GEOMETRIC_CENTER"].includes(locationType)) {
        return {
            provider: "google",
            status: "low_precision",
            reason: "Google Maps ubico una zona aproximada, no un pin suficientemente preciso.",
            formattedAddress: normalizeText(result.formatted_address),
            locationType,
            partialMatch: false,
            latitude,
            longitude
        };
    }
    return {
        provider: "google",
        status: "valid",
        reason: "",
        formattedAddress: normalizeText(result.formatted_address),
        locationType,
        partialMatch: false,
        latitude,
        longitude
    };
}

async function requestGoogleAddressValidation(address, apiKey = GOOGLE_MAPS_API_KEY) {
    if (!apiKey) {
        throw new Error("Falta una API key de Google Geocoding para validar direcciones.");
    }
    const params = new URLSearchParams({
        address: normalizeText(address),
        key: apiKey,
        language: "es",
        region: "ve"
    });
    const response = await fetch(`https://maps.googleapis.com/maps/api/geocode/json?${params.toString()}`);
    if (!response.ok) {
        throw new Error(`Google Geocoding API HTTP ${response.status}. Activa Geocoding API para la clave de servidor.`);
    }
    return classifyGeocodingResult(await response.json());
}

function classifyOpenRouteGeocodingResult(payload) {
    const features = Array.isArray(payload?.features) ? payload.features : [];
    if (!features.length) {
        return {
            provider: "openrouteservice",
            status: "not_found",
            reason: "OpenRouteService no encontro esta direccion.",
            formattedAddress: "",
            locationType: "",
            partialMatch: false,
            latitude: null,
            longitude: null
        };
    }
    const feature = features[0] || {};
    const properties = feature.properties || {};
    const coordinates = Array.isArray(feature.geometry?.coordinates) ? feature.geometry.coordinates : [];
    const longitude = Number(coordinates[0]);
    const latitude = Number(coordinates[1]);
    const confidence = Number(properties.confidence);
    const accuracy = normalizeText(properties.accuracy || properties.match_type || properties.layer);
    const formattedAddress = normalizeText(properties.label || properties.name || "");
    if (!Number.isFinite(latitude) || !Number.isFinite(longitude)) {
        return {
            provider: "openrouteservice",
            status: "not_found",
            reason: "OpenRouteService encontro la direccion, pero no devolvio coordenadas validas.",
            formattedAddress,
            locationType: accuracy,
            partialMatch: false,
            latitude: null,
            longitude: null
        };
    }
    const lowConfidence = Number.isFinite(confidence) && confidence < 0.55;
    return {
        provider: "openrouteservice",
        status: lowConfidence ? "partial_match" : "valid",
        reason: lowConfidence
            ? "OpenRouteService encontro una coincidencia de baja confianza. Revisa calle, edificio y ciudad."
            : "",
        formattedAddress,
        locationType: accuracy,
        partialMatch: lowConfidence,
        latitude,
        longitude
    };
}

function normalizeOpenRouteQuery(value) {
    return normalizeText(value)
        .replace(/\s+/g, " ")
        .replace(/\s+,/g, ",")
        .replace(/,+/g, ",")
        .trim();
}

function expandVenezuelanAddressAbbreviations(value) {
    return normalizeOpenRouteQuery(String(value || "")
        .replace(/\bAV\b\.?/gi, "Avenida")
        .replace(/\bURB\b\.?/gi, "Urbanizacion")
        .replace(/\bC\.?\s*C\.?\b/gi, "Centro Comercial")
        .replace(/\bCC\b\.?/gi, "Centro Comercial")
        .replace(/\bEDIF\b\.?/gi, "Edificio")
        .replace(/\bPB\b\.?/gi, "Planta Baja")
        .replace(/\bPARQ\b\.?/gi, "Parroquia")
        .replace(/\bPPAL\b\.?/gi, "Principal"));
}

function keepParentheticalMarkersAsText(value) {
    return normalizeOpenRouteQuery(String(value || "").replace(/\(([^)]+)\)/g, "$1 "));
}

function removeLeadingParentheticalMarker(value) {
    return normalizeOpenRouteQuery(String(value || "").replace(/^\s*\([^)]*\)\s*/, ""));
}

function stripTrailingVenezuela(value) {
    return normalizeOpenRouteQuery(String(value || "").replace(/,\s*venezuela\s*$/i, ""));
}

function buildOpenRouteGeocodeQueries(address) {
    const raw = normalizeOpenRouteQuery(address);
    const withoutCountry = stripTrailingVenezuela(raw);
    const variants = [
        raw,
        keepParentheticalMarkersAsText(raw),
        removeLeadingParentheticalMarker(raw),
        expandVenezuelanAddressAbbreviations(raw),
        expandVenezuelanAddressAbbreviations(keepParentheticalMarkersAsText(raw)),
        expandVenezuelanAddressAbbreviations(removeLeadingParentheticalMarker(raw)),
        `${withoutCountry}, Miranda, Venezuela`,
        `${withoutCountry}, Caracas, Venezuela`,
        `${expandVenezuelanAddressAbbreviations(withoutCountry)}, Miranda, Venezuela`,
        `${expandVenezuelanAddressAbbreviations(withoutCountry)}, Caracas, Venezuela`
    ];
    return Array.from(new Set(variants.map(normalizeOpenRouteQuery).filter(Boolean)));
}

async function requestOpenRouteAddressValidation(address) {
    if (!OPENROUTESERVICE_API_KEY) {
        throw new Error("Falta OPENROUTESERVICE_API_KEY para validar direcciones.");
    }
    let lastValidation = null;
    const queries = buildOpenRouteGeocodeQueries(address);
    for (const text of queries) {
        const params = new URLSearchParams({
            api_key: OPENROUTESERVICE_API_KEY,
            text,
            size: "1",
            lang: "es",
            "boundary.country": "VE",
            "focus.point.lat": "10.4806",
            "focus.point.lon": "-66.9036"
        });
        const response = await fetch(`https://api.openrouteservice.org/geocode/search?${params.toString()}`);
        if (!response.ok) {
            throw new Error(`OpenRouteService Geocoding API HTTP ${response.status}. Revisa la API key y la cuota disponible.`);
        }
        lastValidation = classifyOpenRouteGeocodingResult(await response.json());
        if (lastValidation.status !== "not_found") return lastValidation;
    }
    return lastValidation || {
        provider: "openrouteservice",
        status: "not_found",
        reason: "OpenRouteService no encontro esta direccion.",
        formattedAddress: "",
        locationType: "",
        partialMatch: false,
        latitude: null,
        longitude: null
    };
}

async function getGoogleGeocodingFallbackUsage() {
    const result = await pool.query(`
        SELECT
            COUNT(*) FILTER (WHERE created_at >= date_trunc('day', NOW()))::int AS daily,
            COUNT(*) FILTER (WHERE created_at >= date_trunc('month', NOW()))::int AS monthly
        FROM routing_geocoding_usage
        WHERE provider = 'google_geocoding_fallback'
    `);
    const row = result.rows[0] || {};
    return {
        daily: Number(row.daily || 0),
        monthly: Number(row.monthly || 0)
    };
}

async function recordGoogleGeocodingFallbackUsage(address, success) {
    try {
        await pool.query(
            `INSERT INTO routing_geocoding_usage (provider, address, success)
             VALUES ('google_geocoding_fallback', $1, $2::boolean)`,
            [normalizeText(address), Boolean(success)]
        );
    } catch (error) {
        console.error("No se pudo registrar uso de Google Geocoding fallback:", error.message || error);
    }
}

async function getGoogleGeocodingFallbackBlockReason() {
    if (!GOOGLE_GEOCODING_FALLBACK_ENABLED) return "Fallback Google Geocoding desactivado.";
    if (!GOOGLE_GEOCODING_API_KEY) return "Falta GOOGLE_GEOCODING_API_KEY para usar Google como respaldo.";
    if (GOOGLE_GEOCODING_DAILY_LIMIT <= 0 || GOOGLE_GEOCODING_MONTHLY_LIMIT <= 0) {
        return "Los limites de Google Geocoding fallback estan en 0.";
    }
    const usage = await getGoogleGeocodingFallbackUsage();
    if (usage.daily >= GOOGLE_GEOCODING_DAILY_LIMIT) {
        return `Limite diario de Google Geocoding alcanzado (${usage.daily}/${GOOGLE_GEOCODING_DAILY_LIMIT}).`;
    }
    if (usage.monthly >= GOOGLE_GEOCODING_MONTHLY_LIMIT) {
        return `Limite mensual de Google Geocoding alcanzado (${usage.monthly}/${GOOGLE_GEOCODING_MONTHLY_LIMIT}).`;
    }
    return "";
}

async function requestGoogleGeocodingFallbackValidation(address, openRouteValidation) {
    const baseReason = normalizeText(openRouteValidation?.reason) || "OpenRouteService no encontro esta direccion.";
    const blockReason = await getGoogleGeocodingFallbackBlockReason();
    if (blockReason) {
        return {
            ...openRouteValidation,
            reason: `${baseReason} ${blockReason}`
        };
    }

    try {
        const validation = await requestGoogleAddressValidation(address, GOOGLE_GEOCODING_API_KEY);
        await recordGoogleGeocodingFallbackUsage(address, validation.status !== "not_found");
        return {
            ...validation,
            provider: "google_geocoding_fallback",
            reason: validation.reason || "Ubicacion resuelta con Google Geocoding como respaldo."
        };
    } catch (error) {
        await recordGoogleGeocodingFallbackUsage(address, false);
        return {
            ...openRouteValidation,
            reason: `${baseReason} Google Geocoding fallback fallo: ${normalizeText(error.message || error)}`
        };
    }
}

async function requestAddressValidation(address) {
    if (getRoutingProvider() === "openrouteservice") {
        const validation = await requestOpenRouteAddressValidation(address);
        if (validation.status !== "not_found" && hasValidationCoordinates(validation)) return validation;
        return requestGoogleGeocodingFallbackValidation(address, validation);
    }
    const validation = await requestGoogleAddressValidation(address);
    return {
        ...validation,
        provider: "google",
        latitude: validation.latitude,
        longitude: validation.longitude
    };
}

async function saveAddressValidation(key, address, validation) {
    await pool.query(
        `INSERT INTO address_validations (
            client_key,
            address,
            status,
            reason,
            formatted_address,
            location_type,
            partial_match,
            provider,
            latitude,
            longitude,
            checked_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7::boolean, $8, $9::double precision, $10::double precision, NOW())
         ON CONFLICT (client_key) DO UPDATE
         SET address = EXCLUDED.address,
             status = EXCLUDED.status,
             reason = EXCLUDED.reason,
             formatted_address = EXCLUDED.formatted_address,
             location_type = EXCLUDED.location_type,
             partial_match = EXCLUDED.partial_match,
             provider = EXCLUDED.provider,
             latitude = EXCLUDED.latitude,
             longitude = EXCLUDED.longitude,
             checked_at = NOW()`,
        [
            key,
            normalizeText(address),
            validation.status,
            validation.reason,
            validation.formattedAddress,
            validation.locationType,
            Boolean(validation.partialMatch),
            normalizeText(validation.provider || getRoutingProvider()),
            validation.latitude,
            validation.longitude
        ]
    );
}

async function validateClientAddresses(clients) {
    const existing = await getAddressValidationsMap();
    const byAddress = new Map();
    existing.forEach((validation) => {
        const address = normalizeText(validation.address);
        if (address && validationMatchesRoutingProvider(validation) && !byAddress.has(address)) {
            byAddress.set(address, validation);
        }
    });
    const uniquePendingAddresses = new Map();

    clients.forEach((client) => {
        const address = normalizeText(client.address);
        if (!address) return;
        const cached = existing.get(client.key);
        if (cached && normalizeText(cached.address) === address && validationMatchesRoutingProvider(cached)) return;
        if (!byAddress.has(address)) uniquePendingAddresses.set(address, null);
    });

    for (const address of uniquePendingAddresses.keys()) {
        const validation = await requestAddressValidation(address);
        byAddress.set(address, {
            address,
            status: validation.status,
            reason: validation.reason,
            formatted_address: validation.formattedAddress,
            location_type: validation.locationType,
            partial_match: validation.partialMatch,
            provider: validation.provider || getRoutingProvider(),
            latitude: validation.latitude,
            longitude: validation.longitude
        });
    }

    for (const client of clients) {
        const address = normalizeText(client.address);
        if (!address) continue;
        const cached = existing.get(client.key);
        if (cached && normalizeText(cached.address) === address && validationMatchesRoutingProvider(cached)) continue;
        const reused = byAddress.get(address);
        if (!reused) continue;
        await saveAddressValidation(client.key, address, {
            status: reused.status,
            reason: reused.reason,
            formattedAddress: reused.formatted_address,
            locationType: reused.location_type,
            partialMatch: reused.partial_match,
            provider: reused.provider || getRoutingProvider(),
            latitude: reused.latitude,
            longitude: reused.longitude
        });
    }
}

function hasGoogleMapsConfig() {
    return Boolean(GOOGLE_MAPS_API_KEY && GOOGLE_MAPS_BROWSER_API_KEY);
}

function hasRoutingConfig() {
    return getRoutingProvider() === "openrouteservice"
        ? Boolean(OPENROUTESERVICE_API_KEY)
        : Boolean(GOOGLE_MAPS_API_KEY);
}

function getRoutingConfigMissing() {
    if (getRoutingProvider() === "openrouteservice") {
        return [
            !OPENROUTESERVICE_API_KEY ? "OPENROUTESERVICE_API_KEY" : "",
            GOOGLE_GEOCODING_FALLBACK_ENABLED && !GOOGLE_GEOCODING_API_KEY ? "GOOGLE_GEOCODING_API_KEY" : ""
        ].filter(Boolean);
    }
    return [
        !GOOGLE_MAPS_API_KEY ? "GOOGLE_MAPS_SERVER_API_KEY" : ""
    ].filter(Boolean);
}

function getGoogleGeocodingFallbackConfig() {
    return {
        enabled: GOOGLE_GEOCODING_FALLBACK_ENABLED,
        configured: Boolean(GOOGLE_GEOCODING_API_KEY),
        dailyLimit: GOOGLE_GEOCODING_DAILY_LIMIT,
        monthlyLimit: GOOGLE_GEOCODING_MONTHLY_LIMIT,
        cacheDays: GOOGLE_GEOCODING_CACHE_DAYS
    };
}

const GOOGLE_MAPS_DELIVERIES_PER_SEGMENT = 10;

function makeGoogleMapsDirectionsUrl(sequence) {
    const stops = sequence.map((client) => client.address).filter(Boolean);
    const destination = stops[stops.length - 1] || "";
    const waypoints = stops.slice(0, -1);
    const params = new URLSearchParams({
        api: "1",
        destination,
        travelmode: "driving"
    });
    if (waypoints.length) {
        params.set("waypoints", waypoints.join("|"));
    }
    return `https://www.google.com/maps/dir/?${params.toString()}`;
}

function makeGoogleMapsNavigationUrl(address) {
    const params = new URLSearchParams({
        api: "1",
        destination: normalizeText(address),
        travelmode: "driving",
        dir_action: "navigate"
    });
    return `https://www.google.com/maps/dir/?${params.toString()}`;
}

function makeGoogleMapsSegments(sequence) {
    const segments = [];
    for (let index = 0; index < sequence.length; index += GOOGLE_MAPS_DELIVERIES_PER_SEGMENT) {
        const deliveries = sequence.slice(index, index + GOOGLE_MAPS_DELIVERIES_PER_SEGMENT);
        segments.push({
            index: segments.length + 1,
            fromStopNumber: index + 1,
            toStopNumber: index + deliveries.length,
            totalClients: deliveries.length,
            googleMapsUrl: makeGoogleMapsDirectionsUrl(deliveries)
        });
    }
    return segments;
}

function formatMeters(meters) {
    if (!Number.isFinite(meters)) return "";
    if (meters < 1000) return `${Math.round(meters)} m`;
    return `${(meters / 1000).toFixed(1)} km`;
}

function parseDurationSeconds(duration) {
    const match = String(duration || "").match(/^(\d+(?:\.\d+)?)s$/);
    return match ? Number(match[1]) : 0;
}

function formatDuration(duration) {
    const seconds = parseDurationSeconds(duration);
    if (!seconds) return "";
    const minutes = Math.round(seconds / 60);
    if (minutes < 60) return `${minutes} min`;
    const hours = Math.floor(minutes / 60);
    const rest = minutes % 60;
    return rest ? `${hours} h ${rest} min` : `${hours} h`;
}

function getTrafficDepartureTimeIso() {
    const fiveMinutesFromNow = Date.now() + (5 * 60 * 1000);
    return new Date(fiveMinutesFromNow).toISOString();
}

function getMatrixTrafficRoutingPreference(clientCount) {
    return clientCount <= TRAFFIC_OPTIMAL_MATRIX_MAX_STOPS
        ? "TRAFFIC_AWARE_OPTIMAL"
        : "TRAFFIC_AWARE";
}

function formatDurationSeconds(secondsValue) {
    const seconds = Number(secondsValue || 0);
    if (!Number.isFinite(seconds) || seconds <= 0) return "";
    const minutes = Math.round(seconds / 60);
    if (minutes < 60) return `${minutes} min`;
    const hours = Math.floor(minutes / 60);
    const rest = minutes % 60;
    return rest ? `${hours} h ${rest} min` : `${hours} h`;
}

function getClientRoutingLocation(client) {
    const lat = Number(client?.routingLocation?.lat);
    const lng = Number(client?.routingLocation?.lng);
    if (Number.isFinite(lat) && Number.isFinite(lng)) return { lat, lng };
    return null;
}

function toOpenRouteCoordinate(location) {
    if (!location) return null;
    const lat = Number(location.lat);
    const lng = Number(location.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return null;
    return [lng, lat];
}

function parseOpenRouteDurationSeconds(value) {
    const seconds = Number(value || 0);
    return Number.isFinite(seconds) ? seconds : 0;
}

async function openRouteServiceRequest(path, body) {
    if (!OPENROUTESERVICE_API_KEY) {
        throw new Error("Falta OPENROUTESERVICE_API_KEY en variables de entorno.");
    }
    const response = await fetch(`https://api.openrouteservice.org/v2/${path}`, {
        method: "POST",
        headers: {
            "Authorization": OPENROUTESERVICE_API_KEY,
            "Content-Type": "application/json",
            "Accept": "application/json, application/geo+json"
        },
        body: JSON.stringify(body)
    });

    if (!response.ok) {
        let detail = "";
        try {
            const text = await response.text();
            if (text) {
                try {
                    const payload = JSON.parse(text);
                    detail = payload?.error?.message ? `: ${payload.error.message}` : `: ${text.slice(0, 240)}`;
                } catch (_) {
                    detail = `: ${text.slice(0, 240)}`;
                }
            }
        } catch (_) {
            detail = "";
        }
        throw new Error(`OpenRouteService HTTP ${response.status}${detail}`);
    }

    return response.json();
}

async function resolveOpenRouteAddressCoordinate(address) {
    const validation = await requestAddressValidation(address);
    if (validation.status === "not_found" || !Number.isFinite(Number(validation.latitude)) || !Number.isFinite(Number(validation.longitude))) {
        throw new Error(`OpenRouteService no pudo ubicar esta direccion: ${normalizeText(address)}`);
    }
    return { lat: Number(validation.latitude), lng: Number(validation.longitude) };
}

async function computeOpenRouteMatrix(originAddress, clients, originLocation) {
    const locations = [
        toOpenRouteCoordinate(originLocation),
        ...clients.map((client) => toOpenRouteCoordinate(getClientRoutingLocation(client)))
    ];
    if (locations.some((location) => !location)) {
        throw new Error("Faltan coordenadas para uno o mas clientes. Revisa las direcciones antes de calcular la ruta.");
    }

    const payload = await openRouteServiceRequest("matrix/driving-car", {
        locations,
        metrics: ["duration", "distance"],
        units: "m"
    });
    const size = locations.length;
    const durations = Array.from({ length: size }, (_, row) =>
        Array.from({ length: size }, (_, col) => (row === col ? 0 : Infinity))
    );
    const distances = Array.from({ length: size }, () => Array(size).fill(0));

    if (!Array.isArray(payload?.durations)) {
        throw new Error("OpenRouteService no devolvio una matriz de tiempos valida.");
    }

    payload.durations.forEach((row, rowIndex) => {
        if (!Array.isArray(row)) return;
        row.forEach((value, colIndex) => {
            const seconds = Number(value);
            durations[rowIndex][colIndex] = Number.isFinite(seconds) ? seconds : Infinity;
        });
    });

    if (Array.isArray(payload.distances)) {
        payload.distances.forEach((row, rowIndex) => {
            if (!Array.isArray(row)) return;
            row.forEach((value, colIndex) => {
                const meters = Number(value);
                distances[rowIndex][colIndex] = Number.isFinite(meters) ? meters : 0;
            });
        });
    }

    return {
        durations,
        distances,
        routingPreference: "openrouteservice_matrix_driving_car",
        queriedAt: new Date().toISOString()
    };
}

async function computeOpenRouteDetails(originAddress, sequence, originLocation) {
    const coordinates = [
        toOpenRouteCoordinate(originLocation),
        ...sequence.map((client) => toOpenRouteCoordinate(getClientRoutingLocation(client)))
    ];
    if (coordinates.some((location) => !location)) {
        throw new Error("Faltan coordenadas para calcular el detalle de la ruta.");
    }
    const payload = await openRouteServiceRequest("directions/driving-car/json", {
        coordinates,
        language: "es",
        units: "m",
        instructions: false
    });
    const route = payload?.routes?.[0];
    if (!route) throw new Error("OpenRouteService no devolvio rutas para esa consulta.");
    const summary = route.summary || {};
    return {
        distanceMeters: Number(summary.distance || 0),
        durationSeconds: parseOpenRouteDurationSeconds(summary.duration),
        polyline: normalizeText(route.geometry),
        legs: Array.isArray(route.segments)
            ? route.segments.map((segment) => ({
                distanceMeters: Number(segment?.distance || 0),
                durationSeconds: parseOpenRouteDurationSeconds(segment?.duration)
            }))
            : []
    };
}

async function computeOpenRouteOptimizedRoute(originAddress, clients) {
    if (!OPENROUTESERVICE_API_KEY) {
        throw new Error("Falta OPENROUTESERVICE_API_KEY en variables de entorno.");
    }
    const cleanClients = clients.filter((client) => client.address).slice(0, 24);
    if (!cleanClients.length) {
        throw new Error("La ruta necesita al menos 1 cliente con direccion para calcular con OpenRouteService.");
    }
    const missingLocations = cleanClients.filter((client) => !getClientRoutingLocation(client));
    if (missingLocations.length) {
        throw new Error(`${missingLocations.length} cliente(s) no tienen coordenadas validas. Revisa esos datos antes de despachar.`);
    }

    const originLocation = await resolveOpenRouteAddressCoordinate(originAddress);
    const matrix = cleanClients.length > 1
        ? await computeOpenRouteMatrix(originAddress, cleanClients, originLocation)
        : null;
    const optimization = matrix
        ? optimizeClientOrderByDuration(cleanClients, matrix.durations)
        : { sequence: cleanClients, method: "openrouteservice_single_stop" };
    const routeDetails = await computeOpenRouteDetails(originAddress, optimization.sequence, originLocation);

    return buildOpenRouteOptimizedRouteResponse(
        originAddress,
        optimization.sequence,
        routeDetails,
        matrix,
        optimization.method
    );
}

async function computeOptimizedRoute(originAddress, clients) {
    if (getRoutingProvider() === "openrouteservice") {
        return computeOpenRouteOptimizedRoute(originAddress, clients);
    }
    return computeGoogleOptimizedRoute(originAddress, clients);
}

async function computeGoogleOptimizedRoute(originAddress, clients) {
    if (!GOOGLE_MAPS_API_KEY) {
        throw new Error("Falta GOOGLE_MAPS_SERVER_API_KEY o GOOGLE_MAPS_API_KEY en variables de entorno.");
    }

    const cleanClients = clients.filter((client) => client.address).slice(0, 24);
    if (!cleanClients.length) {
        throw new Error("La ruta necesita al menos 1 cliente con direccion para calcular con Routes API.");
    }

    const matrix = cleanClients.length > 1
        ? await computeTrafficMatrix(originAddress, cleanClients)
        : null;
    const optimization = matrix
        ? optimizeClientOrderByDuration(cleanClients, matrix.durations)
        : { sequence: cleanClients, method: "routes_api_single_stop" };
    const routeDetails = await computeRouteDetails(originAddress, optimization.sequence);

    return buildOptimizedRouteResponse(
        originAddress,
        optimization.sequence,
        routeDetails,
        matrix,
        optimization.method
    );
}

async function googleRoutesRequest(url, fieldMask, body) {
    const response = await fetch(url, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": GOOGLE_MAPS_API_KEY,
            "X-Goog-FieldMask": fieldMask
        },
        body: JSON.stringify(body)
    });

    if (!response.ok) {
        let detail = "";
        try {
            const text = await response.text();
            if (text) {
                try {
                    const payload = JSON.parse(text);
                    detail = payload?.error?.message ? `: ${payload.error.message}` : `: ${text.slice(0, 240)}`;
                } catch (_) {
                    detail = `: ${text.slice(0, 240)}`;
                }
            }
        } catch (_) {
            detail = "";
        }
        throw new Error(`Google Routes API HTTP ${response.status}${detail}`);
    }

    return response.json();
}

async function computeTrafficMatrix(originAddress, clients) {
    const locations = [
        { address: originAddress },
        ...clients.map((client) => ({ address: client.address }))
    ];
    const body = {
        origins: locations.map((location) => ({ waypoint: location })),
        destinations: locations.map((location) => ({ waypoint: location })),
        travelMode: "DRIVE",
        routingPreference: getMatrixTrafficRoutingPreference(clients.length),
        departureTime: getTrafficDepartureTimeIso(),
        languageCode: "es-419",
        units: "METRIC"
    };
    const entries = await googleRoutesRequest(
        "https://routes.googleapis.com/distanceMatrix/v2:computeRouteMatrix",
        "originIndex,destinationIndex,duration,distanceMeters,status,condition",
        body
    );
    const size = locations.length;
    const durations = Array.from({ length: size }, () => Array(size).fill(Infinity));
    const distances = Array.from({ length: size }, () => Array(size).fill(0));

    for (let index = 0; index < size; index += 1) {
        durations[index][index] = 0;
    }

    if (!Array.isArray(entries)) {
        throw new Error("Google Routes API no devolvio una matriz de rutas valida.");
    }

    entries.forEach((entry) => {
        const originIndex = Number(entry.originIndex);
        const destinationIndex = Number(entry.destinationIndex);
        if (!Number.isInteger(originIndex) || !Number.isInteger(destinationIndex)) return;
        const statusCode = entry.status?.code;
        if (statusCode && statusCode !== 0) return;

        const seconds = parseDurationSeconds(entry.duration);
        if (originIndex !== destinationIndex && seconds > 0) {
            durations[originIndex][destinationIndex] = seconds;
        }
        distances[originIndex][destinationIndex] = Number(entry.distanceMeters || 0);
    });

    return {
        durations,
        distances,
        routingPreference: body.routingPreference,
        queriedAt: new Date().toISOString()
    };
}

function pathDurationSeconds(order, durations) {
    let total = 0;
    let previous = 0;
    for (const clientIndex of order) {
        const matrixIndex = clientIndex + 1;
        const value = durations[previous]?.[matrixIndex];
        total += Number.isFinite(value) ? value : 86400;
        previous = matrixIndex;
    }
    return total;
}

function nearestNeighborOrder(clients, durations) {
    const remaining = clients.map((_, index) => index);
    const order = [];
    let previousMatrixIndex = 0;

    while (remaining.length) {
        let bestRemainingIndex = 0;
        let bestDuration = Infinity;
        remaining.forEach((clientIndex, remainingIndex) => {
            const matrixIndex = clientIndex + 1;
            const duration = durations[previousMatrixIndex]?.[matrixIndex] ?? Infinity;
            if (duration < bestDuration) {
                bestDuration = duration;
                bestRemainingIndex = remainingIndex;
            }
        });
        const [nextClientIndex] = remaining.splice(bestRemainingIndex, 1);
        order.push(nextClientIndex);
        previousMatrixIndex = nextClientIndex + 1;
    }

    return order;
}

function twoOptOpenPath(order, durations) {
    let best = [...order];
    let bestScore = pathDurationSeconds(best, durations);
    let improved = true;
    let guard = 0;

    while (improved && guard < 100) {
        improved = false;
        guard += 1;
        for (let start = 0; start < best.length - 1; start += 1) {
            for (let end = start + 1; end < best.length; end += 1) {
                const candidate = [
                    ...best.slice(0, start),
                    ...best.slice(start, end + 1).reverse(),
                    ...best.slice(end + 1)
                ];
                const candidateScore = pathDurationSeconds(candidate, durations);
                if (candidateScore + 1 < bestScore) {
                    best = candidate;
                    bestScore = candidateScore;
                    improved = true;
                }
            }
        }
    }

    return best;
}

function exactShortestOpenPathOrder(clients, durations) {
    const clientCount = clients.length;
    const stateCount = 1 << clientCount;
    const costs = Array.from({ length: stateCount }, () => Array(clientCount).fill(Infinity));
    const parents = Array.from({ length: stateCount }, () => Array(clientCount).fill(-1));

    for (let clientIndex = 0; clientIndex < clientCount; clientIndex += 1) {
        costs[1 << clientIndex][clientIndex] = pathDurationSeconds([clientIndex], durations);
    }

    for (let mask = 1; mask < stateCount; mask += 1) {
        for (let last = 0; last < clientCount; last += 1) {
            if (!(mask & (1 << last)) || !Number.isFinite(costs[mask][last])) continue;

            for (let next = 0; next < clientCount; next += 1) {
                if (mask & (1 << next)) continue;

                const nextMask = mask | (1 << next);
                const legSeconds = durations[last + 1]?.[next + 1];
                const candidateCost = costs[mask][last] + (Number.isFinite(legSeconds) ? legSeconds : 86400);
                if (candidateCost < costs[nextMask][next]) {
                    costs[nextMask][next] = candidateCost;
                    parents[nextMask][next] = last;
                }
            }
        }
    }

    const fullMask = stateCount - 1;
    let last = 0;
    for (let clientIndex = 1; clientIndex < clientCount; clientIndex += 1) {
        if (costs[fullMask][clientIndex] < costs[fullMask][last]) {
            last = clientIndex;
        }
    }

    const reversedOrder = [];
    let mask = fullMask;
    while (last >= 0) {
        reversedOrder.push(last);
        const previous = parents[mask][last];
        mask ^= (1 << last);
        last = previous;
    }
    return reversedOrder.reverse();
}

function optimizeClientOrderByDuration(clients, durations) {
    if (clients.length <= EXACT_OPTIMIZATION_MAX_STOPS) {
        const exact = exactShortestOpenPathOrder(clients, durations);
        return {
            sequence: exact.map((clientIndex) => clients[clientIndex]),
            method: "routes_api_traffic_matrix_exact_shortest_duration"
        };
    }

    const nearest = nearestNeighborOrder(clients, durations);
    const improved = twoOptOpenPath(nearest, durations);
    return {
        sequence: improved.map((clientIndex) => clients[clientIndex]),
        method: "routes_api_traffic_matrix_nearest_neighbor_2opt_fallback"
    };
}

async function computeRouteDetails(originAddress, sequence) {
    const destination = sequence[sequence.length - 1];
    const intermediates = sequence.slice(0, -1);
    const body = {
        origin: { address: originAddress },
        destination: { address: destination.address },
        intermediates: intermediates.map((client) => ({ address: client.address })),
        travelMode: "DRIVE",
        routingPreference: "TRAFFIC_AWARE_OPTIMAL",
        departureTime: getTrafficDepartureTimeIso(),
        optimizeWaypointOrder: false,
        languageCode: "es-419",
        units: "METRIC"
    };
    const payload = await googleRoutesRequest(
        "https://routes.googleapis.com/directions/v2:computeRoutes",
        [
            "routes.distanceMeters",
            "routes.duration",
            "routes.staticDuration",
            "routes.polyline.encodedPolyline",
            "routes.legs.distanceMeters",
            "routes.legs.duration",
            "routes.legs.staticDuration",
            "routes.legs.endLocation"
        ].join(","),
        body
    );
    const route = payload?.routes?.[0];
    if (!route) throw new Error("Google Routes API no devolvio rutas para esa consulta.");
    return route;
}

function buildOptimizedRouteResponse(originAddress, sequence, route, matrix, optimizationMethod) {
    const legs = Array.isArray(route.legs) ? route.legs : [];
    const googleMapsSegments = makeGoogleMapsSegments(sequence);
    return {
        provider: "google",
        origin: originAddress,
        totalClients: sequence.length,
        totalDistanceKm: Number(((route.distanceMeters || 0) / 1000).toFixed(2)),
        totalDurationText: formatDuration(route.duration),
        totalDurationSeconds: parseDurationSeconds(route.duration),
        trafficAware: true,
        trafficRoutingPreference: "TRAFFIC_AWARE_OPTIMAL",
        matrixRoutingPreference: matrix?.routingPreference || "",
        optimizationMethod,
        matrixQueriedAt: matrix?.queriedAt || "",
        queriedAt: new Date().toISOString(),
        polyline: route.polyline?.encodedPolyline || "",
        googleMapsUrl: googleMapsSegments[0]?.googleMapsUrl || "",
        googleMapsSegments,
        sequence: sequence.map((client, index) => {
            const leg = legs[index] || {};
            const latLng = leg.endLocation?.latLng;
            return {
                ...client,
                stopNumber: index + 1,
                legDistanceMeters: Number(leg.distanceMeters || 0),
                legDistanceText: formatMeters(Number(leg.distanceMeters || 0)),
                legDurationText: formatDuration(leg.duration),
                legDurationSeconds: parseDurationSeconds(leg.duration),
                googleMapsNavigationUrl: makeGoogleMapsNavigationUrl(client.address),
                location: latLng ? {
                    lat: Number(latLng.latitude),
                    lng: Number(latLng.longitude)
                } : null
            };
        })
    };
}

function buildOpenRouteOptimizedRouteResponse(originAddress, sequence, route, matrix, optimizationMethod) {
    const legs = Array.isArray(route.legs) ? route.legs : [];
    const googleMapsSegments = makeGoogleMapsSegments(sequence);
    return {
        provider: "openrouteservice",
        origin: originAddress,
        totalClients: sequence.length,
        totalDistanceKm: Number(((route.distanceMeters || 0) / 1000).toFixed(2)),
        totalDurationText: formatDurationSeconds(route.durationSeconds),
        totalDurationSeconds: parseOpenRouteDurationSeconds(route.durationSeconds),
        trafficAware: false,
        trafficRoutingPreference: "OPENROUTESERVICE_DRIVING_CAR",
        matrixRoutingPreference: matrix?.routingPreference || "",
        optimizationMethod,
        matrixQueriedAt: matrix?.queriedAt || "",
        queriedAt: new Date().toISOString(),
        polyline: route.polyline || "",
        googleMapsUrl: googleMapsSegments[0]?.googleMapsUrl || "",
        googleMapsSegments,
        sequence: sequence.map((client, index) => {
            const leg = legs[index] || {};
            const location = getClientRoutingLocation(client);
            return {
                ...client,
                stopNumber: index + 1,
                legDistanceMeters: Number(leg.distanceMeters || 0),
                legDistanceText: formatMeters(Number(leg.distanceMeters || 0)),
                legDurationText: formatDurationSeconds(leg.durationSeconds),
                legDurationSeconds: parseOpenRouteDurationSeconds(leg.durationSeconds),
                googleMapsNavigationUrl: makeGoogleMapsNavigationUrl(client.address),
                location
            };
        })
    };
}

async function optimizeRoute(clients, originAddress) {
    return computeOptimizedRoute(originAddress, clients);
}

function defaultDbChangeQuery() {
    const { schema, table } = parseTableRef(SOURCE_TABLE);
    const tableRef = `${quoteIdent(schema)}.${quoteIdent(table)}`;
    return `
        SELECT md5(COUNT(*)::text || ':' || COALESCE(SUM(length(t::text))::text, '0')) AS signature
        FROM ${tableRef} AS t
    `;
}

async function getDbSignature() {
    const query = DB_CHANGE_WATCH_QUERY || defaultDbChangeQuery();
    const result = await pool.query(query);
    const signature = result?.rows?.[0]?.signature;
    return String(signature || "");
}

async function triggerRenderDeploy(reason) {
    if (!RENDER_DEPLOY_HOOK_URL) return;
    const response = await fetch(RENDER_DEPLOY_HOOK_URL, { method: "POST" });
    if (!response.ok) {
        throw new Error(`Deploy hook fallo con HTTP ${response.status}`);
    }
    console.log(`Deploy disparado por cambio DB: ${reason}`);
}

function startDbChangeWatcher() {
    if (!AUTO_DEPLOY_ON_DB_CHANGE) return;
    if (!RENDER_DEPLOY_HOOK_URL) {
        console.warn("AUTO_DEPLOY_ON_DB_CHANGE=true pero falta RENDER_DEPLOY_HOOK_URL.");
        return;
    }

    let lastSignature = "";
    let lastDeployAt = 0;

    const checkChanges = async () => {
        try {
            const signature = await getDbSignature();
            if (!lastSignature) {
                lastSignature = signature;
                return;
            }
            if (signature === lastSignature) return;

            const now = Date.now();
            if (now - lastDeployAt < AUTO_DEPLOY_COOLDOWN_MS) {
                console.log("Cambio detectado, pero en cooldown de deploy.");
                lastSignature = signature;
                return;
            }

            lastSignature = signature;
            lastDeployAt = now;
            await triggerRenderDeploy("source_table_signature_changed");
        } catch (error) {
            console.error("Watcher DB error:", error.message || error);
        }
    };

    setInterval(checkChanges, Math.max(30000, DB_WATCH_INTERVAL_MS));
    checkChanges().catch((error) => console.error("Watcher DB init error:", error.message || error));
    console.log("Watcher de cambios en DB activo.");
}

app.get("/api/health", async (_, res) => {
    try {
        await ensureDatabaseReady();
        await pool.query("SELECT 1");
        res.json({
            ok: true,
            service: "prueba-empaquetado-vrp-integrado",
            db: "connected",
            source: SOURCE_TABLE,
            routingProvider: getRoutingProvider(),
            routingReady: hasRoutingConfig(),
            googleMapsReady: hasGoogleMapsConfig(),
            googleGeocodingFallback: getGoogleGeocodingFallbackConfig()
        });
    } catch (error) {
        res.status(500).json({ ok: false, error: String(error.message || error) });
    }
});

app.get("/api/maps-config", (_, res) => {
    const provider = getRoutingProvider();
    res.json({
        ok: true,
        enabled: hasRoutingConfig(),
        provider,
        providerLabel: getRoutingProviderLabel(),
        browserApiKey: provider === "google" ? GOOGLE_MAPS_BROWSER_API_KEY : "",
        origin: DISTRIBUTION_ORIGIN,
        originName: DISTRIBUTION_ORIGIN_NAME,
        requiredApis: provider === "openrouteservice"
            ? [
                "OpenRouteService Directions",
                "OpenRouteService Matrix",
                "OpenRouteService Geocode",
                ...(GOOGLE_GEOCODING_FALLBACK_ENABLED ? ["Google Geocoding API fallback"] : [])
            ]
            : ["Routes API", "Geocoding API"],
        googleGeocodingFallback: getGoogleGeocodingFallbackConfig(),
        missing: getRoutingConfigMissing()
    });
});

app.get("/api/session", async (req, res) => {
    try {
        const auth = await getRequestAuthContext(req);
        res.json({
            ok: true,
            session: auth ? {
                username: auth.username,
                role: auth.role,
                fullName: auth.fullName,
                vehiclePlate: auth.vehiclePlate
            } : null
        });
    } catch (error) {
        res.status(500).json({ ok: false, error: String(error.message || error) });
    }
});

app.get("/api/routes", async (req, res) => {
    try {
        await ensureDatabaseReady();
        const auth = await requireRequestAuthContext(req, res);
        if (!auth) return;
        res.json({ routes: await routeStats(auth) });
    } catch (error) {
        res.status(500).json({ ok: false, error: String(error.message || error) });
    }
});

app.get("/api/clients", async (req, res) => {
    try {
        await ensureDatabaseReady();
        const auth = await requireRequestAuthContext(req, res);
        if (!auth) return;
        const route = normalizeText(req.query.route);
        const clients = await getClients(route, auth);
        res.json({ total: clients.length, clients });
    } catch (error) {
        res.status(500).json({ ok: false, error: String(error.message || error) });
    }
});

app.get("/api/errors", async (req, res) => {
    try {
        await ensureDatabaseReady();
        const auth = await requireRequestAuthContext(req, res);
        if (!auth) return;
        if (normalizeRole(auth.role) === "conductor") {
            return res.status(403).json({ ok: false, error: "Los conductores solo pueden visualizar rutas." });
        }
        const clients = (await getClients("", auth)).filter(isClientWithErrors);
        res.json({ total: clients.length, clients });
    } catch (error) {
        res.status(500).json({ ok: false, error: String(error.message || error) });
    }
});

app.put("/api/clients/:key", async (req, res) => {
    try {
        await ensureDatabaseReady();
        const auth = await requireRequestAuthContext(req, res);
        if (!auth) return;
        if (normalizeRole(auth.role) === "conductor") {
            return res.status(403).json({ ok: false, error: "Los conductores no pueden ajustar datos." });
        }
        const key = decodeURIComponent(req.params.key);
        const { name, address, route, transport } = req.body || {};
        await saveClientOverride(key, {
            name: normalizeText(name),
            address: normalizeText(address),
            route: normalizeText(route),
            transport: normalizeText(transport)
        });
        res.json({ ok: true, key });
    } catch (error) {
        res.status(500).json({ ok: false, error: String(error.message || error) });
    }
});

app.put("/api/deliveries/:key", async (req, res) => {
    try {
        await ensureDatabaseReady();
        const auth = await requireRequestAuthContext(req, res);
        if (!auth) return;
        const key = decodeURIComponent(req.params.key);
        const delivered = req.body?.delivered === true;
        const partial = req.body?.partial === true;
        const partialDetail = Array.isArray(req.body?.partialDetail) ? req.body.partialDetail : null;
        const hasDeliveredBaskets = Object.prototype.hasOwnProperty.call(req.body || {}, "deliveredBaskets");
        const deliveredBaskets = Number(req.body?.deliveredBaskets);
        if (!key) return res.status(400).json({ ok: false, error: "Debes enviar una entrega valida." });
        if (!hasDeliveredBaskets) {
            return res.status(400).json({ ok: false, error: "Debes enviar la cantidad de cestas entregadas." });
        }
        if (!Number.isInteger(deliveredBaskets) || deliveredBaskets < 0) {
            return res.status(400).json({ ok: false, error: "La cantidad de cestas debe ser un numero entero no negativo." });
        }
        await saveDeliveryStatus(key, delivered, deliveredBaskets, partial, partialDetail);
        res.json({ ok: true, key, delivered, deliveredBaskets, partial });
    } catch (error) {
        res.status(500).json({ ok: false, error: String(error.message || error) });
    }
});

app.post("/api/optimize-route", async (req, res) => {
    try {
        await ensureDatabaseReady();
        const auth = await requireRequestAuthContext(req, res);
        if (!auth) return;
        const route = normalizeText(req.body?.route);
        const origin = normalizeText(req.body?.origin) || DISTRIBUTION_ORIGIN;
        if (!route) return res.status(400).json({ ok: false, error: "Debes enviar route." });
        let clients = (await getClients(route, auth)).filter((client) => client.address);
        if (!clients.length) {
            return res.status(404).json({ ok: false, error: `No hay clientes con direccion para la ruta ${route}.` });
        }
        await validateClientAddresses(clients);
        clients = (await getClients(route, auth)).filter((client) => client.address);
        const notFoundClients = clients.filter((client) => client.googleAddressStatus === "not_found");
        if (notFoundClients.length) {
            return res.status(422).json({
                ok: false,
                error: `${notFoundClients.length} cliente(s) tienen direcciones que ${getRoutingProviderLabel()} no pudo encontrar. Revisa esos datos antes de despachar.`,
                invalidClients: notFoundClients.map((client) => ({
                    key: client.key,
                    clientId: client.clientId,
                    name: client.nombre_o_razon_social || client.name,
                    address: client.address,
                    reason: client.googleAddressReason
                }))
            });
        }
        if (getRoutingProvider() === "openrouteservice") {
            const clientsWithoutCoordinates = clients.filter((client) => !getClientRoutingLocation(client));
            if (clientsWithoutCoordinates.length) {
                return res.status(422).json({
                    ok: false,
                    error: `${clientsWithoutCoordinates.length} cliente(s) no tienen coordenadas validas para OpenRouteService. Revisa esas direcciones antes de despachar.`,
                    invalidClients: clientsWithoutCoordinates.map((client) => ({
                        key: client.key,
                        clientId: client.clientId,
                        name: client.nombre_o_razon_social || client.name,
                        address: client.address,
                        reason: client.googleAddressReason || "Sin coordenadas"
                    }))
                });
            }
        }
        const optimized = await optimizeRoute(clients, origin);
        res.json({ ok: true, route, optimized });
    } catch (error) {
        res.status(500).json({ ok: false, error: String(error.message || error) });
    }
});

module.exports = { ensureDatabaseReady, startDbChangeWatcher };
