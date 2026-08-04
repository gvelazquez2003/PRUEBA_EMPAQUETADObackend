--
-- PostgreSQL database dump
--

\restrict ZGbI1MeIj6qdsOVujTvy0f9JnbN77LX3VXUh294bYBuzPB51wERVtsEcpl40iMm

-- Dumped from database version 14.23
-- Dumped by pg_dump version 14.23

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: productos_audit_trigger_fn(); Type: FUNCTION; Schema: public; Owner: admin01_pasante
--

CREATE FUNCTION public.productos_audit_trigger_fn() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
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
    $$;


ALTER FUNCTION public.productos_audit_trigger_fn() OWNER TO admin01_pasante;

--
-- Name: pronostico_actualizar_updated_at(); Type: FUNCTION; Schema: public; Owner: admin01
--

CREATE FUNCTION public.pronostico_actualizar_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.pronostico_actualizar_updated_at() OWNER TO admin01;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: address_validations; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.address_validations (
    client_key text NOT NULL,
    address text DEFAULT ''::text NOT NULL,
    status text DEFAULT ''::text NOT NULL,
    reason text DEFAULT ''::text NOT NULL,
    formatted_address text DEFAULT ''::text NOT NULL,
    location_type text DEFAULT ''::text NOT NULL,
    partial_match boolean DEFAULT false NOT NULL,
    checked_at timestamp with time zone DEFAULT now() NOT NULL,
    provider text DEFAULT ''::text NOT NULL,
    latitude double precision,
    longitude double precision
);


ALTER TABLE public.address_validations OWNER TO admin01_pasante;

--
-- Name: cambios_registros; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.cambios_registros (
    id_cambio bigint NOT NULL,
    responsable character varying(180) NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    id_cliente bigint,
    nombre_cliente character varying(180),
    producto jsonb,
    direccion_id bigint,
    direccion_texto character varying(240),
    ruta_nombre character varying(120),
    codigo_cambio character varying(20),
    grupo_cambio character varying(40),
    rif_cliente character varying(40),
    contacto character varying(180),
    telefono character varying(20)
);


ALTER TABLE public.cambios_registros OWNER TO admin01_pasante;

--
-- Name: almacen09_cambios_productos_id_cambio_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.almacen09_cambios_productos_id_cambio_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.almacen09_cambios_productos_id_cambio_seq OWNER TO admin01_pasante;

--
-- Name: almacen09_cambios_productos_id_cambio_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.almacen09_cambios_productos_id_cambio_seq OWNED BY public.cambios_registros.id_cambio;


--
-- Name: almacen09_clientes; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.almacen09_clientes (
    id_cliente bigint NOT NULL,
    nombre character varying(160) NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.almacen09_clientes OWNER TO admin01_pasante;

--
-- Name: almacen09_clientes_id_cliente_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.almacen09_clientes_id_cliente_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.almacen09_clientes_id_cliente_seq OWNER TO admin01_pasante;

--
-- Name: almacen09_clientes_id_cliente_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.almacen09_clientes_id_cliente_seq OWNED BY public.almacen09_clientes.id_cliente;


--
-- Name: almacen09_direcciones; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.almacen09_direcciones (
    id_direccion bigint NOT NULL,
    direccion character varying(240) NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.almacen09_direcciones OWNER TO admin01_pasante;

--
-- Name: almacen09_direcciones_id_direccion_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.almacen09_direcciones_id_direccion_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.almacen09_direcciones_id_direccion_seq OWNER TO admin01_pasante;

--
-- Name: almacen09_direcciones_id_direccion_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.almacen09_direcciones_id_direccion_seq OWNED BY public.almacen09_direcciones.id_direccion;


--
-- Name: almacen09_salidas_detalle; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.almacen09_salidas_detalle (
    id_detalle bigint NOT NULL,
    id_factura bigint NOT NULL,
    id_cambio bigint,
    id_producto integer NOT NULL,
    codigo_producto character varying(30) NOT NULL,
    producto text NOT NULL,
    numero_lote character varying(80) NOT NULL,
    cantidad integer NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    CONSTRAINT almacen09_salidas_detalle_cantidad_check CHECK ((cantidad > 0))
);


ALTER TABLE public.almacen09_salidas_detalle OWNER TO admin01_pasante;

--
-- Name: almacen09_salidas_detalle_id_detalle_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.almacen09_salidas_detalle_id_detalle_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.almacen09_salidas_detalle_id_detalle_seq OWNER TO admin01_pasante;

--
-- Name: almacen09_salidas_detalle_id_detalle_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.almacen09_salidas_detalle_id_detalle_seq OWNED BY public.almacen09_salidas_detalle.id_detalle;


--
-- Name: almacen09_sucursales; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.almacen09_sucursales (
    id_sucursal bigint NOT NULL,
    nombre character varying(160) NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.almacen09_sucursales OWNER TO admin01_pasante;

--
-- Name: almacen09_sucursales_id_sucursal_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.almacen09_sucursales_id_sucursal_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.almacen09_sucursales_id_sucursal_seq OWNER TO admin01_pasante;

--
-- Name: almacen09_sucursales_id_sucursal_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.almacen09_sucursales_id_sucursal_seq OWNED BY public.almacen09_sucursales.id_sucursal;


--
-- Name: almacen09_vendedores; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.almacen09_vendedores (
    id_vendedor bigint NOT NULL,
    nombre character varying(160) NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    codigo_ven character varying(10),
    tipo character varying(10),
    descripcion character varying(200) NOT NULL,
    cedula character varying(40),
    direc1 character varying(240),
    direc2 character varying(240),
    telefonos character varying(120),
    fecha_creacion timestamp without time zone,
    estado boolean,
    codigo_zona character varying(20)
);


ALTER TABLE public.almacen09_vendedores OWNER TO admin01_pasante;

--
-- Name: almacen09_vendedores_id_vendedor_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.almacen09_vendedores_id_vendedor_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.almacen09_vendedores_id_vendedor_seq OWNER TO admin01_pasante;

--
-- Name: almacen09_vendedores_id_vendedor_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.almacen09_vendedores_id_vendedor_seq OWNED BY public.almacen09_vendedores.id_vendedor;


--
-- Name: almacen09_zonas; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.almacen09_zonas (
    id_zona bigint NOT NULL,
    nombre character varying(120) NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.almacen09_zonas OWNER TO admin01_pasante;

--
-- Name: almacen09_zonas_id_zona_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.almacen09_zonas_id_zona_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.almacen09_zonas_id_zona_seq OWNER TO admin01_pasante;

--
-- Name: almacen09_zonas_id_zona_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.almacen09_zonas_id_zona_seq OWNED BY public.almacen09_zonas.id_zona;


--
-- Name: almacen_lotes_procesados; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.almacen_lotes_procesados (
    codigo_lote character varying(50) NOT NULL,
    estado character varying(20) DEFAULT 'validado'::character varying NOT NULL,
    processed_at timestamp without time zone DEFAULT now() NOT NULL,
    resumen_validacion jsonb
);


ALTER TABLE public.almacen_lotes_procesados OWNER TO admin01_pasante;

--
-- Name: almacen_validaciones_detalle; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.almacen_validaciones_detalle (
    id integer NOT NULL,
    codigo_lote character varying(50) NOT NULL,
    id_producto integer NOT NULL,
    codigo_producto character varying(50) NOT NULL,
    cantidad_esperada integer NOT NULL,
    cantidad_contada integer NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.almacen_validaciones_detalle OWNER TO admin01_pasante;

--
-- Name: almacen_validaciones_detalle_id_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.almacen_validaciones_detalle_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.almacen_validaciones_detalle_id_seq OWNER TO admin01_pasante;

--
-- Name: almacen_validaciones_detalle_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.almacen_validaciones_detalle_id_seq OWNED BY public.almacen_validaciones_detalle.id;


--
-- Name: auth_sessions; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.auth_sessions (
    token character varying(128) NOT NULL,
    id_user integer NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp without time zone DEFAULT now() NOT NULL,
    expires_at timestamp without time zone,
    revoked_at timestamp without time zone,
    user_agent text,
    ip_address character varying(80)
);


ALTER TABLE public.auth_sessions OWNER TO admin01_pasante;

--
-- Name: auth_users; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.auth_users (
    id_user integer NOT NULL,
    username character varying(20) NOT NULL,
    role character varying(20) NOT NULL,
    password_hash text NOT NULL,
    activo boolean DEFAULT true NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    full_name character varying(120),
    vehicle_plate character varying(20),
    password_visible text,
    sede_id integer,
    CONSTRAINT auth_users_role_check CHECK (((role)::text = ANY ((ARRAY['administrador'::character varying, 'produccion'::character varying, 'controlp_carga'::character varying, 'controlp_editor'::character varying, 'almacen'::character varying, 'facturacion'::character varying, 'ventas'::character varying, 'vendedor'::character varying, 'conductor'::character varying])::text[]))),
    CONSTRAINT auth_users_username_format_check CHECK ((((username)::text ~ '^[A-Z0-9]{2,20}$'::text) AND ((username)::text ~ '[A-Z]'::text)))
);


ALTER TABLE public.auth_users OWNER TO admin01_pasante;

--
-- Name: auth_users_id_user_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.auth_users_id_user_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.auth_users_id_user_seq OWNER TO admin01_pasante;

--
-- Name: auth_users_id_user_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.auth_users_id_user_seq OWNED BY public.auth_users.id_user;


--
-- Name: backup_almacen09_despacho_20260617_20260623; Type: TABLE; Schema: public; Owner: admin01
--

CREATE TABLE public.backup_almacen09_despacho_20260617_20260623 (
    codigo_lote character varying(50) NOT NULL,
    existia_antes boolean NOT NULL,
    estado_anterior character varying(20),
    processed_at_anterior timestamp without time zone,
    resumen_validacion_anterior jsonb,
    backed_up_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.backup_almacen09_despacho_20260617_20260623 OWNER TO admin01;

--
-- Name: backup_clientes_dup_rif_direccion_20260731; Type: TABLE; Schema: public; Owner: admin01
--

CREATE TABLE public.backup_clientes_dup_rif_direccion_20260731 (
    backed_up_at timestamp with time zone,
    id_cliente text,
    descripcion text,
    tipo_cliente text,
    direccion text,
    ruta text,
    transporte text,
    zona text,
    vendedor text
);


ALTER TABLE public.backup_clientes_dup_rif_direccion_20260731 OWNER TO admin01;

--
-- Name: backup_clientes_valencia_maracay_20260730; Type: TABLE; Schema: public; Owner: admin01
--

CREATE TABLE public.backup_clientes_valencia_maracay_20260730 (
    backed_up_at timestamp with time zone,
    id_cliente text,
    descripcion text,
    tipo_cliente text,
    direccion text,
    ruta text,
    transporte text,
    zona text,
    vendedor text
);


ALTER TABLE public.backup_clientes_valencia_maracay_20260730 OWNER TO admin01;

--
-- Name: backup_productos_pop_id_repair_20260716; Type: TABLE; Schema: public; Owner: admin01
--

CREATE TABLE public.backup_productos_pop_id_repair_20260716 (
    backed_up_at timestamp with time zone,
    id_producto integer,
    codigo_producto character varying(20),
    descripcion character varying(200),
    unidad_primaria character varying(50),
    paquetes integer,
    cestas integer,
    sobre_piso integer,
    activo boolean,
    codigo_barras text
);


ALTER TABLE public.backup_productos_pop_id_repair_20260716 OWNER TO admin01;

--
-- Name: cambios_razones; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.cambios_razones (
    id_razon bigint NOT NULL,
    razon_texto character varying(180) NOT NULL
);


ALTER TABLE public.cambios_razones OWNER TO admin01_pasante;

--
-- Name: cambios_razones_razon_id_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.cambios_razones_razon_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.cambios_razones_razon_id_seq OWNER TO admin01_pasante;

--
-- Name: cambios_razones_razon_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.cambios_razones_razon_id_seq OWNED BY public.cambios_razones.id_razon;


--
-- Name: client_overrides; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.client_overrides (
    client_key text NOT NULL,
    name text DEFAULT ''::text NOT NULL,
    address text DEFAULT ''::text NOT NULL,
    route_name text DEFAULT ''::text NOT NULL,
    transport text DEFAULT ''::text NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.client_overrides OWNER TO admin01_pasante;

--
-- Name: clientes; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.clientes (
    id_cliente text NOT NULL,
    descripcion text,
    tipo_cliente text,
    direccion text,
    ruta text,
    transporte text,
    zona text,
    vendedor text
);


ALTER TABLE public.clientes OWNER TO admin01_pasante;

--
-- Name: conciliacion_pagos; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.conciliacion_pagos (
    id_pago bigint NOT NULL,
    id_factura bigint NOT NULL,
    fecha_pago date NOT NULL,
    registrado_por character varying(80),
    observacion text,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.conciliacion_pagos OWNER TO admin01_pasante;

--
-- Name: conciliacion_pagos_id_pago_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.conciliacion_pagos_id_pago_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.conciliacion_pagos_id_pago_seq OWNER TO admin01_pasante;

--
-- Name: conciliacion_pagos_id_pago_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.conciliacion_pagos_id_pago_seq OWNED BY public.conciliacion_pagos.id_pago;


--
-- Name: conteo_errores; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.conteo_errores (
    id integer NOT NULL,
    codigo_lote character varying(50),
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    lote_producto character varying(120),
    codigo_producto character varying(30),
    nombre_producto text,
    cantidad_esperada integer,
    cantidad_recibida integer,
    usuario character varying(32)
);


ALTER TABLE public.conteo_errores OWNER TO admin01_pasante;

--
-- Name: conteo_errores_id_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.conteo_errores_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.conteo_errores_id_seq OWNER TO admin01_pasante;

--
-- Name: conteo_errores_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.conteo_errores_id_seq OWNED BY public.conteo_errores.id;


--
-- Name: control_inventario_guardia; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.control_inventario_guardia (
    id_control bigint NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    almacenista character varying(120) NOT NULL,
    turno_actual character varying(120) NOT NULL,
    momento_conteo character varying(180) NOT NULL,
    id_producto integer NOT NULL,
    cantidad_fisica_contada integer NOT NULL,
    fecha_elaboracion date NOT NULL,
    almacen character varying(20) NOT NULL,
    responsable character varying(120) DEFAULT ''::character varying NOT NULL,
    fecha_conteo date,
    numero_lote character varying(80) DEFAULT ''::character varying NOT NULL,
    cestas integer DEFAULT 0 NOT NULL
);


ALTER TABLE public.control_inventario_guardia OWNER TO admin01_pasante;

--
-- Name: control_inventario_guardia_id_control_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.control_inventario_guardia_id_control_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.control_inventario_guardia_id_control_seq OWNER TO admin01_pasante;

--
-- Name: control_inventario_guardia_id_control_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.control_inventario_guardia_id_control_seq OWNED BY public.control_inventario_guardia.id_control;


--
-- Name: delivery_status; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.delivery_status (
    client_key text NOT NULL,
    delivered boolean DEFAULT false NOT NULL,
    delivered_at timestamp with time zone,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    delivered_baskets integer DEFAULT 0 NOT NULL,
    partial boolean DEFAULT false NOT NULL,
    partial_detail jsonb,
    supplied_baskets integer DEFAULT 0 NOT NULL,
    recovered_baskets integer DEFAULT 0 NOT NULL
);


ALTER TABLE public.delivery_status OWNER TO admin01_pasante;

--
-- Name: destinos; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.destinos (
    id_destino integer NOT NULL,
    nombre character varying(100) NOT NULL
);


ALTER TABLE public.destinos OWNER TO admin01_pasante;

--
-- Name: destinos_id_destino_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.destinos_id_destino_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.destinos_id_destino_seq OWNER TO admin01_pasante;

--
-- Name: destinos_id_destino_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.destinos_id_destino_seq OWNED BY public.destinos.id_destino;


--
-- Name: empaquetados_cabecera; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.empaquetados_cabecera (
    id_cabecera integer NOT NULL,
    fecha_hora timestamp without time zone NOT NULL,
    id_destino integer,
    numero_registro character varying(50),
    id_responsable integer,
    id_sede integer
);


ALTER TABLE public.empaquetados_cabecera OWNER TO admin01_pasante;

--
-- Name: empaquetados_cabecera_id_cabecera_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.empaquetados_cabecera_id_cabecera_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.empaquetados_cabecera_id_cabecera_seq OWNER TO admin01_pasante;

--
-- Name: empaquetados_cabecera_id_cabecera_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.empaquetados_cabecera_id_cabecera_seq OWNED BY public.empaquetados_cabecera.id_cabecera;


--
-- Name: empaquetados_detalle; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.empaquetados_detalle (
    id_detalle integer NOT NULL,
    id_cabecera integer,
    id_producto integer,
    cantidad integer NOT NULL,
    numero_lote character varying(50) NOT NULL,
    codigo_producto character varying(30),
    producto text
);


ALTER TABLE public.empaquetados_detalle OWNER TO admin01_pasante;

--
-- Name: empaquetados_detalle_id_detalle_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.empaquetados_detalle_id_detalle_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.empaquetados_detalle_id_detalle_seq OWNER TO admin01_pasante;

--
-- Name: empaquetados_detalle_id_detalle_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.empaquetados_detalle_id_detalle_seq OWNED BY public.empaquetados_detalle.id_detalle;


--
-- Name: entregas_sedes; Type: TABLE; Schema: public; Owner: admin01
--

CREATE TABLE public.entregas_sedes (
    id_entrega bigint NOT NULL,
    solicitud_id bigint,
    fecha date NOT NULL,
    hora time without time zone NOT NULL,
    sede_id integer NOT NULL,
    responsable_nombre character varying(150) NOT NULL,
    responsable_email character varying(200),
    observaciones text,
    huella_temporal timestamp with time zone DEFAULT now() NOT NULL,
    origen character varying(30) DEFAULT 'PRODUCCIONPDT'::character varying NOT NULL,
    referencia_externa character varying(150),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.entregas_sedes OWNER TO admin01;

--
-- Name: entregas_sedes_detalle; Type: TABLE; Schema: public; Owner: admin01
--

CREATE TABLE public.entregas_sedes_detalle (
    id_detalle bigint NOT NULL,
    entrega_id bigint NOT NULL,
    producto_id integer NOT NULL,
    familia character varying(100),
    cantidad_entregada numeric(12,3) NOT NULL,
    unidad_medida character varying(50),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chk_cantidad_entregada CHECK ((cantidad_entregada > (0)::numeric))
);


ALTER TABLE public.entregas_sedes_detalle OWNER TO admin01;

--
-- Name: entregas_sedes_detalle_id_detalle_seq; Type: SEQUENCE; Schema: public; Owner: admin01
--

ALTER TABLE public.entregas_sedes_detalle ALTER COLUMN id_detalle ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.entregas_sedes_detalle_id_detalle_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: entregas_sedes_id_entrega_seq; Type: SEQUENCE; Schema: public; Owner: admin01
--

ALTER TABLE public.entregas_sedes ALTER COLUMN id_entrega ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.entregas_sedes_id_entrega_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: facturas_bot_uploads; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.facturas_bot_uploads (
    id_upload bigint NOT NULL,
    original_name character varying(240) NOT NULL,
    stored_name character varying(260) NOT NULL,
    file_path text NOT NULL,
    mime_type character varying(120) DEFAULT 'application/pdf'::character varying NOT NULL,
    file_size bigint DEFAULT 0 NOT NULL,
    sha256 character varying(64) NOT NULL,
    estado character varying(30) DEFAULT 'pendiente'::character varying NOT NULL,
    mensaje text,
    uploaded_by character varying(80),
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    id_factura bigint,
    extracted_payload jsonb,
    processed_by character varying(80),
    processed_at timestamp without time zone
);


ALTER TABLE public.facturas_bot_uploads OWNER TO admin01_pasante;

--
-- Name: facturas_bot_uploads_id_upload_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.facturas_bot_uploads_id_upload_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.facturas_bot_uploads_id_upload_seq OWNER TO admin01_pasante;

--
-- Name: facturas_bot_uploads_id_upload_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.facturas_bot_uploads_id_upload_seq OWNED BY public.facturas_bot_uploads.id_upload;


--
-- Name: historico_resultados_consolidado; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.historico_resultados_consolidado (
    id_historico bigint NOT NULL,
    fecha date,
    fecha_empaquetado timestamp without time zone,
    fecha_almacen09 timestamp without time zone,
    codigo_producto character varying(30),
    producto text NOT NULL,
    cantidad integer,
    entregado_a character varying(120),
    numero_registro character varying(50),
    responsable character varying(120),
    sede character varying(160),
    numero_lote character varying(80),
    source_hash character varying(64) NOT NULL,
    origen_historico character varying(20) DEFAULT 'csv'::character varying NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.historico_resultados_consolidado OWNER TO admin01_pasante;

--
-- Name: historico_resultados_consolidado_id_historico_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.historico_resultados_consolidado_id_historico_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.historico_resultados_consolidado_id_historico_seq OWNER TO admin01_pasante;

--
-- Name: historico_resultados_consolidado_id_historico_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.historico_resultados_consolidado_id_historico_seq OWNED BY public.historico_resultados_consolidado.id_historico;


--
-- Name: hojas_ruta_exportadas; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.hojas_ruta_exportadas (
    id_hoja bigint NOT NULL,
    ruta_nombre character varying(120) NOT NULL,
    fecha_entrega date NOT NULL,
    fecha_busqueda_desde date,
    fecha_busqueda_hasta date,
    conductor character varying(180),
    numero_camion character varying(80),
    total_despachos integer DEFAULT 0 NOT NULL,
    total_cestas integer DEFAULT 0 NOT NULL,
    usuario character varying(80),
    facturas jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    hoja_html text,
    nombre_archivo character varying(180)
);


ALTER TABLE public.hojas_ruta_exportadas OWNER TO admin01_pasante;

--
-- Name: hojas_ruta_exportadas_id_hoja_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.hojas_ruta_exportadas_id_hoja_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.hojas_ruta_exportadas_id_hoja_seq OWNER TO admin01_pasante;

--
-- Name: hojas_ruta_exportadas_id_hoja_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.hojas_ruta_exportadas_id_hoja_seq OWNED BY public.hojas_ruta_exportadas.id_hoja;


--
-- Name: idempotency_requests; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.idempotency_requests (
    scope character varying(120) NOT NULL,
    request_key character varying(120) NOT NULL,
    response_status integer,
    response_body jsonb,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    completed_at timestamp without time zone
);


ALTER TABLE public.idempotency_requests OWNER TO admin01_pasante;

--
-- Name: mermas_cabecera; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.mermas_cabecera (
    id_merma bigint NOT NULL,
    fecha_hora timestamp without time zone NOT NULL,
    id_responsable integer NOT NULL,
    id_sede integer NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.mermas_cabecera OWNER TO admin01_pasante;

--
-- Name: mermas_cabecera_id_merma_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.mermas_cabecera_id_merma_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.mermas_cabecera_id_merma_seq OWNER TO admin01_pasante;

--
-- Name: mermas_cabecera_id_merma_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.mermas_cabecera_id_merma_seq OWNED BY public.mermas_cabecera.id_merma;


--
-- Name: mermas_detalle; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.mermas_detalle (
    id_detalle bigint NOT NULL,
    id_merma bigint NOT NULL,
    id_producto integer NOT NULL,
    codigo_producto character varying(30) NOT NULL,
    producto text NOT NULL,
    cantidad integer NOT NULL,
    id_motivo integer NOT NULL,
    motivo character varying(160) NOT NULL,
    numero_lote character varying(80) NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    CONSTRAINT mermas_detalle_cantidad_check CHECK ((cantidad > 0))
);


ALTER TABLE public.mermas_detalle OWNER TO admin01_pasante;

--
-- Name: mermas_detalle_id_detalle_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.mermas_detalle_id_detalle_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.mermas_detalle_id_detalle_seq OWNER TO admin01_pasante;

--
-- Name: mermas_detalle_id_detalle_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.mermas_detalle_id_detalle_seq OWNED BY public.mermas_detalle.id_detalle;


--
-- Name: motivos_merma; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.motivos_merma (
    id_motivo integer NOT NULL,
    nombre character varying(160) NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.motivos_merma OWNER TO admin01_pasante;

--
-- Name: motivos_merma_id_motivo_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.motivos_merma_id_motivo_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.motivos_merma_id_motivo_seq OWNER TO admin01_pasante;

--
-- Name: motivos_merma_id_motivo_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.motivos_merma_id_motivo_seq OWNED BY public.motivos_merma.id_motivo;


--
-- Name: predicciones_demanda; Type: TABLE; Schema: public; Owner: admin01
--

CREATE TABLE public.predicciones_demanda (
    id integer NOT NULL,
    fecha_objetivo date NOT NULL,
    sede_id integer NOT NULL,
    producto_id integer NOT NULL,
    cantidad_proyectada numeric(10,2) NOT NULL,
    fecha_calculo timestamp without time zone DEFAULT now(),
    version_modelo character varying(50) DEFAULT 'v1.0'::character varying
);


ALTER TABLE public.predicciones_demanda OWNER TO admin01;

--
-- Name: predicciones_demanda_id_seq; Type: SEQUENCE; Schema: public; Owner: admin01
--

ALTER TABLE public.predicciones_demanda ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.predicciones_demanda_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: productos; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.productos (
    id_producto integer NOT NULL,
    codigo_producto character varying(20) NOT NULL,
    descripcion character varying(200) NOT NULL,
    unidad_primaria character varying(50),
    paquetes integer DEFAULT 0,
    cestas integer DEFAULT 0,
    sobre_piso integer DEFAULT 0,
    activo boolean DEFAULT true NOT NULL,
    codigo_barras text
);


ALTER TABLE public.productos OWNER TO admin01_pasante;

--
-- Name: productos_audit; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.productos_audit (
    id_audit bigint NOT NULL,
    operation text NOT NULL,
    changed_at timestamp with time zone DEFAULT now() NOT NULL,
    db_user text DEFAULT CURRENT_USER NOT NULL,
    app_name text DEFAULT current_setting('application_name'::text, true) NOT NULL,
    client_addr text,
    old_row jsonb,
    new_row jsonb
);


ALTER TABLE public.productos_audit OWNER TO admin01_pasante;

--
-- Name: productos_audit_id_audit_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.productos_audit_id_audit_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.productos_audit_id_audit_seq OWNER TO admin01_pasante;

--
-- Name: productos_audit_id_audit_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.productos_audit_id_audit_seq OWNED BY public.productos_audit.id_audit;


--
-- Name: productos_id_producto_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.productos_id_producto_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.productos_id_producto_seq OWNER TO admin01_pasante;

--
-- Name: productos_id_producto_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.productos_id_producto_seq OWNED BY public.productos.id_producto;


--
-- Name: responsables; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.responsables (
    id_responsable integer NOT NULL,
    nombre_completo character varying(100) NOT NULL
);


ALTER TABLE public.responsables OWNER TO admin01_pasante;

--
-- Name: responsables_id_responsable_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.responsables_id_responsable_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.responsables_id_responsable_seq OWNER TO admin01_pasante;

--
-- Name: responsables_id_responsable_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.responsables_id_responsable_seq OWNED BY public.responsables.id_responsable;


--
-- Name: routing_geocoding_usage; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.routing_geocoding_usage (
    id bigint NOT NULL,
    provider text DEFAULT ''::text NOT NULL,
    address text DEFAULT ''::text NOT NULL,
    success boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.routing_geocoding_usage OWNER TO admin01_pasante;

--
-- Name: routing_geocoding_usage_id_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.routing_geocoding_usage_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.routing_geocoding_usage_id_seq OWNER TO admin01_pasante;

--
-- Name: routing_geocoding_usage_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.routing_geocoding_usage_id_seq OWNED BY public.routing_geocoding_usage.id;


--
-- Name: rutas_completadas_ocultas; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.rutas_completadas_ocultas (
    id_hoja bigint NOT NULL,
    hidden_by character varying(80) DEFAULT ''::character varying NOT NULL,
    hidden_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.rutas_completadas_ocultas OWNER TO admin01_pasante;

--
-- Name: salidas_cliente_sucursales; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.salidas_cliente_sucursales (
    id_relacion bigint NOT NULL,
    cliente_id bigint,
    cliente_nombre character varying(160) NOT NULL,
    cliente_key character varying(160) NOT NULL,
    sucursal_nombre character varying(160) NOT NULL,
    sucursal_key character varying(160) NOT NULL,
    zona_nombre character varying(120),
    direccion_id bigint,
    direccion_texto character varying(240),
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.salidas_cliente_sucursales OWNER TO admin01_pasante;

--
-- Name: salidas_cliente_sucursales_id_relacion_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.salidas_cliente_sucursales_id_relacion_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.salidas_cliente_sucursales_id_relacion_seq OWNER TO admin01_pasante;

--
-- Name: salidas_cliente_sucursales_id_relacion_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.salidas_cliente_sucursales_id_relacion_seq OWNED BY public.salidas_cliente_sucursales.id_relacion;


--
-- Name: salidas_facturas; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.salidas_facturas (
    id_factura bigint NOT NULL,
    numero_control bigint,
    numero_factura character varying(80) NOT NULL,
    fecha_emision timestamp without time zone NOT NULL,
    cliente_id bigint,
    cliente_nombre character varying(160),
    vendedor_id bigint,
    vendedor_nombre character varying(160),
    zona_id bigint,
    zona_nombre character varying(120),
    sucursal_id bigint,
    sucursal_nombre character varying(160),
    direccion_id bigint,
    direccion_texto character varying(240),
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    estado character varying(20) DEFAULT 'emitida'::character varying NOT NULL,
    ruta_nombre character varying(120),
    transporte_nombre character varying(120),
    documento character varying(30) DEFAULT 'factura'::character varying NOT NULL,
    fecha_vencimiento date
);


ALTER TABLE public.salidas_facturas OWNER TO admin01_pasante;

--
-- Name: salidas_facturas_id_factura_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.salidas_facturas_id_factura_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.salidas_facturas_id_factura_seq OWNER TO admin01_pasante;

--
-- Name: salidas_facturas_id_factura_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.salidas_facturas_id_factura_seq OWNED BY public.salidas_facturas.id_factura;


--
-- Name: salidas_reentregas_pendientes; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.salidas_reentregas_pendientes (
    id_reentrega bigint NOT NULL,
    client_key text NOT NULL,
    sheet_id text DEFAULT ''::text NOT NULL,
    original_invoice_id text DEFAULT ''::text NOT NULL,
    original_numero_factura text DEFAULT ''::text NOT NULL,
    original_numero_control text DEFAULT ''::text NOT NULL,
    cliente_nombre character varying(160),
    vendedor_nombre character varying(160),
    zona_nombre character varying(120),
    ruta_nombre character varying(120),
    transporte_nombre character varying(120),
    direccion_texto character varying(240),
    fecha_emision date DEFAULT CURRENT_DATE NOT NULL,
    detalle jsonb DEFAULT '[]'::jsonb NOT NULL,
    estado character varying(30) DEFAULT 'pendiente'::character varying NOT NULL,
    created_by character varying(80),
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.salidas_reentregas_pendientes OWNER TO admin01_pasante;

--
-- Name: salidas_reentregas_pendientes_id_reentrega_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.salidas_reentregas_pendientes_id_reentrega_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.salidas_reentregas_pendientes_id_reentrega_seq OWNER TO admin01_pasante;

--
-- Name: salidas_reentregas_pendientes_id_reentrega_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.salidas_reentregas_pendientes_id_reentrega_seq OWNED BY public.salidas_reentregas_pendientes.id_reentrega;


--
-- Name: sedes; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.sedes (
    id_sede integer NOT NULL,
    nombre character varying(100) NOT NULL
);


ALTER TABLE public.sedes OWNER TO admin01_pasante;

--
-- Name: sedes_id_sede_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

CREATE SEQUENCE public.sedes_id_sede_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.sedes_id_sede_seq OWNER TO admin01_pasante;

--
-- Name: sedes_id_sede_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01_pasante
--

ALTER SEQUENCE public.sedes_id_sede_seq OWNED BY public.sedes.id_sede;


--
-- Name: solicitudes_sedes; Type: TABLE; Schema: public; Owner: admin01
--

CREATE TABLE public.solicitudes_sedes (
    id_solicitud bigint NOT NULL,
    fecha date NOT NULL,
    hora time without time zone NOT NULL,
    sede_id integer NOT NULL,
    responsable_nombre character varying(150) NOT NULL,
    responsable_email character varying(200),
    observaciones text,
    estado character varying(20) DEFAULT 'PENDIENTE'::character varying NOT NULL,
    huella_temporal timestamp with time zone DEFAULT now() NOT NULL,
    origen character varying(30) DEFAULT 'PRODUCCIONPDT'::character varying NOT NULL,
    referencia_externa character varying(150),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chk_solicitudes_estado CHECK (((estado)::text = ANY ((ARRAY['PENDIENTE'::character varying, 'PARCIAL'::character varying, 'COMPLETADA'::character varying, 'CANCELADA'::character varying])::text[])))
);


ALTER TABLE public.solicitudes_sedes OWNER TO admin01;

--
-- Name: solicitudes_sedes_detalle; Type: TABLE; Schema: public; Owner: admin01
--

CREATE TABLE public.solicitudes_sedes_detalle (
    id_detalle bigint NOT NULL,
    solicitud_id bigint NOT NULL,
    producto_id integer NOT NULL,
    familia character varying(100),
    cantidad_solicitada numeric(12,3) NOT NULL,
    unidad_medida character varying(50),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chk_cantidad_solicitada CHECK ((cantidad_solicitada > (0)::numeric))
);


ALTER TABLE public.solicitudes_sedes_detalle OWNER TO admin01;

--
-- Name: solicitudes_sedes_detalle_id_detalle_seq; Type: SEQUENCE; Schema: public; Owner: admin01
--

ALTER TABLE public.solicitudes_sedes_detalle ALTER COLUMN id_detalle ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.solicitudes_sedes_detalle_id_detalle_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: solicitudes_sedes_id_solicitud_seq; Type: SEQUENCE; Schema: public; Owner: admin01
--

ALTER TABLE public.solicitudes_sedes ALTER COLUMN id_solicitud ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.solicitudes_sedes_id_solicitud_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: solicitudes_sedes_sheets_outbox; Type: TABLE; Schema: public; Owner: admin01
--

CREATE TABLE public.solicitudes_sedes_sheets_outbox (
    id_sync bigint NOT NULL,
    event_type character varying(60) NOT NULL,
    entity_type character varying(60) NOT NULL,
    entity_id bigint,
    tipo character varying(30) NOT NULL,
    referencia_externa character varying(80) NOT NULL,
    payload jsonb NOT NULL,
    estado character varying(30) DEFAULT 'pendiente'::character varying NOT NULL,
    intentos integer DEFAULT 0 NOT NULL,
    ultimo_error text,
    synced_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT chk_solicitudes_sheets_outbox_estado CHECK (((estado)::text = ANY ((ARRAY['pendiente'::character varying, 'procesando'::character varying, 'sincronizado'::character varying, 'error'::character varying])::text[]))),
    CONSTRAINT chk_solicitudes_sheets_outbox_tipo CHECK (((tipo)::text = ANY ((ARRAY['solicitud'::character varying, 'entrega'::character varying])::text[])))
);


ALTER TABLE public.solicitudes_sedes_sheets_outbox OWNER TO admin01;

--
-- Name: solicitudes_sedes_sheets_outbox_id_sync_seq; Type: SEQUENCE; Schema: public; Owner: admin01
--

ALTER TABLE public.solicitudes_sedes_sheets_outbox ALTER COLUMN id_sync ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.solicitudes_sedes_sheets_outbox_id_sync_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: spc_masas; Type: TABLE; Schema: public; Owner: admin01
--

CREATE TABLE public.spc_masas (
    id bigint NOT NULL,
    amasadora text NOT NULL,
    lote text NOT NULL,
    temp_masa_final double precision,
    tiempo_amasado double precision,
    temp_fermentacion double precision,
    ph_masa_fermentada double precision,
    humedad_relativa double precision,
    tiempo_horneado double precision,
    tiempo_enfriamiento double precision,
    responsable_etapa_1 text,
    responsable_etapa_2 text,
    responsable_etapa_3 text,
    responsable_etapa_4 text,
    fecha_registro timestamp without time zone DEFAULT now()
);


ALTER TABLE public.spc_masas OWNER TO admin01;

--
-- Name: spc_masas_id_seq; Type: SEQUENCE; Schema: public; Owner: admin01
--

CREATE SEQUENCE public.spc_masas_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.spc_masas_id_seq OWNER TO admin01;

--
-- Name: spc_masas_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: admin01
--

ALTER SEQUENCE public.spc_masas_id_seq OWNED BY public.spc_masas.id;


--
-- Name: users; Type: TABLE; Schema: public; Owner: admin01_pasante
--

CREATE TABLE public.users (
    id_user integer NOT NULL,
    users text,
    clave text NOT NULL,
    correo text NOT NULL,
    estado boolean DEFAULT true,
    rol text,
    fecha_creacion timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    fecha_update timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    permisos text
);


ALTER TABLE public.users OWNER TO admin01_pasante;

--
-- Name: users_id_user_seq; Type: SEQUENCE; Schema: public; Owner: admin01_pasante
--

ALTER TABLE public.users ALTER COLUMN id_user ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.users_id_user_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: ventas_diarias; Type: TABLE; Schema: public; Owner: admin01
--

CREATE TABLE public.ventas_diarias (
    id integer NOT NULL,
    fecha date NOT NULL,
    sede_id integer NOT NULL,
    producto_id integer NOT NULL,
    cantidad integer NOT NULL,
    precio_unitario numeric(10,2) NOT NULL,
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);


ALTER TABLE public.ventas_diarias OWNER TO admin01;

--
-- Name: ventas_diarias_id_seq; Type: SEQUENCE; Schema: public; Owner: admin01
--

ALTER TABLE public.ventas_diarias ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.ventas_diarias_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: almacen09_clientes id_cliente; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.almacen09_clientes ALTER COLUMN id_cliente SET DEFAULT nextval('public.almacen09_clientes_id_cliente_seq'::regclass);


--
-- Name: almacen09_direcciones id_direccion; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.almacen09_direcciones ALTER COLUMN id_direccion SET DEFAULT nextval('public.almacen09_direcciones_id_direccion_seq'::regclass);


--
-- Name: almacen09_salidas_detalle id_detalle; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.almacen09_salidas_detalle ALTER COLUMN id_detalle SET DEFAULT nextval('public.almacen09_salidas_detalle_id_detalle_seq'::regclass);


--
-- Name: almacen09_sucursales id_sucursal; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.almacen09_sucursales ALTER COLUMN id_sucursal SET DEFAULT nextval('public.almacen09_sucursales_id_sucursal_seq'::regclass);


--
-- Name: almacen09_vendedores id_vendedor; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.almacen09_vendedores ALTER COLUMN id_vendedor SET DEFAULT nextval('public.almacen09_vendedores_id_vendedor_seq'::regclass);


--
-- Name: almacen09_zonas id_zona; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.almacen09_zonas ALTER COLUMN id_zona SET DEFAULT nextval('public.almacen09_zonas_id_zona_seq'::regclass);


--
-- Name: almacen_validaciones_detalle id; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.almacen_validaciones_detalle ALTER COLUMN id SET DEFAULT nextval('public.almacen_validaciones_detalle_id_seq'::regclass);


--
-- Name: auth_users id_user; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.auth_users ALTER COLUMN id_user SET DEFAULT nextval('public.auth_users_id_user_seq'::regclass);


--
-- Name: cambios_razones id_razon; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.cambios_razones ALTER COLUMN id_razon SET DEFAULT nextval('public.cambios_razones_razon_id_seq'::regclass);


--
-- Name: cambios_registros id_cambio; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.cambios_registros ALTER COLUMN id_cambio SET DEFAULT nextval('public.almacen09_cambios_productos_id_cambio_seq'::regclass);


--
-- Name: conciliacion_pagos id_pago; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.conciliacion_pagos ALTER COLUMN id_pago SET DEFAULT nextval('public.conciliacion_pagos_id_pago_seq'::regclass);


--
-- Name: conteo_errores id; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.conteo_errores ALTER COLUMN id SET DEFAULT nextval('public.conteo_errores_id_seq'::regclass);


--
-- Name: control_inventario_guardia id_control; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.control_inventario_guardia ALTER COLUMN id_control SET DEFAULT nextval('public.control_inventario_guardia_id_control_seq'::regclass);


--
-- Name: destinos id_destino; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.destinos ALTER COLUMN id_destino SET DEFAULT nextval('public.destinos_id_destino_seq'::regclass);


--
-- Name: empaquetados_cabecera id_cabecera; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.empaquetados_cabecera ALTER COLUMN id_cabecera SET DEFAULT nextval('public.empaquetados_cabecera_id_cabecera_seq'::regclass);


--
-- Name: empaquetados_detalle id_detalle; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.empaquetados_detalle ALTER COLUMN id_detalle SET DEFAULT nextval('public.empaquetados_detalle_id_detalle_seq'::regclass);


--
-- Name: facturas_bot_uploads id_upload; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.facturas_bot_uploads ALTER COLUMN id_upload SET DEFAULT nextval('public.facturas_bot_uploads_id_upload_seq'::regclass);


--
-- Name: historico_resultados_consolidado id_historico; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.historico_resultados_consolidado ALTER COLUMN id_historico SET DEFAULT nextval('public.historico_resultados_consolidado_id_historico_seq'::regclass);


--
-- Name: hojas_ruta_exportadas id_hoja; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.hojas_ruta_exportadas ALTER COLUMN id_hoja SET DEFAULT nextval('public.hojas_ruta_exportadas_id_hoja_seq'::regclass);


--
-- Name: mermas_cabecera id_merma; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.mermas_cabecera ALTER COLUMN id_merma SET DEFAULT nextval('public.mermas_cabecera_id_merma_seq'::regclass);


--
-- Name: mermas_detalle id_detalle; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.mermas_detalle ALTER COLUMN id_detalle SET DEFAULT nextval('public.mermas_detalle_id_detalle_seq'::regclass);


--
-- Name: motivos_merma id_motivo; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.motivos_merma ALTER COLUMN id_motivo SET DEFAULT nextval('public.motivos_merma_id_motivo_seq'::regclass);


--
-- Name: productos id_producto; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.productos ALTER COLUMN id_producto SET DEFAULT nextval('public.productos_id_producto_seq'::regclass);


--
-- Name: productos_audit id_audit; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.productos_audit ALTER COLUMN id_audit SET DEFAULT nextval('public.productos_audit_id_audit_seq'::regclass);


--
-- Name: responsables id_responsable; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.responsables ALTER COLUMN id_responsable SET DEFAULT nextval('public.responsables_id_responsable_seq'::regclass);


--
-- Name: routing_geocoding_usage id; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.routing_geocoding_usage ALTER COLUMN id SET DEFAULT nextval('public.routing_geocoding_usage_id_seq'::regclass);


--
-- Name: salidas_cliente_sucursales id_relacion; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.salidas_cliente_sucursales ALTER COLUMN id_relacion SET DEFAULT nextval('public.salidas_cliente_sucursales_id_relacion_seq'::regclass);


--
-- Name: salidas_facturas id_factura; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.salidas_facturas ALTER COLUMN id_factura SET DEFAULT nextval('public.salidas_facturas_id_factura_seq'::regclass);


--
-- Name: salidas_reentregas_pendientes id_reentrega; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.salidas_reentregas_pendientes ALTER COLUMN id_reentrega SET DEFAULT nextval('public.salidas_reentregas_pendientes_id_reentrega_seq'::regclass);


--
-- Name: sedes id_sede; Type: DEFAULT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.sedes ALTER COLUMN id_sede SET DEFAULT nextval('public.sedes_id_sede_seq'::regclass);


--
-- Name: spc_masas id; Type: DEFAULT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.spc_masas ALTER COLUMN id SET DEFAULT nextval('public.spc_masas_id_seq'::regclass);


--
-- Name: address_validations address_validations_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.address_validations
    ADD CONSTRAINT address_validations_pkey PRIMARY KEY (client_key);


--
-- Name: backup_almacen09_despacho_20260617_20260623 backup_almacen09_despacho_20260617_20260623_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.backup_almacen09_despacho_20260617_20260623
    ADD CONSTRAINT backup_almacen09_despacho_20260617_20260623_pkey PRIMARY KEY (codigo_lote);


--
-- Name: cambios_razones cambios_razones_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.cambios_razones
    ADD CONSTRAINT cambios_razones_pkey PRIMARY KEY (id_razon);


--
-- Name: conciliacion_pagos conciliacion_pagos_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.conciliacion_pagos
    ADD CONSTRAINT conciliacion_pagos_pkey PRIMARY KEY (id_pago);


--
-- Name: delivery_status delivery_status_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.delivery_status
    ADD CONSTRAINT delivery_status_pkey PRIMARY KEY (client_key);


--
-- Name: entregas_sedes_detalle entregas_sedes_detalle_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.entregas_sedes_detalle
    ADD CONSTRAINT entregas_sedes_detalle_pkey PRIMARY KEY (id_detalle);


--
-- Name: entregas_sedes entregas_sedes_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.entregas_sedes
    ADD CONSTRAINT entregas_sedes_pkey PRIMARY KEY (id_entrega);


--
-- Name: facturas_bot_uploads facturas_bot_uploads_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.facturas_bot_uploads
    ADD CONSTRAINT facturas_bot_uploads_pkey PRIMARY KEY (id_upload);


--
-- Name: idempotency_requests idempotency_requests_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.idempotency_requests
    ADD CONSTRAINT idempotency_requests_pkey PRIMARY KEY (scope, request_key);


--
-- Name: predicciones_demanda predicciones_demanda_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.predicciones_demanda
    ADD CONSTRAINT predicciones_demanda_pkey PRIMARY KEY (id);


--
-- Name: productos_audit productos_audit_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.productos_audit
    ADD CONSTRAINT productos_audit_pkey PRIMARY KEY (id_audit);


--
-- Name: productos productos_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.productos
    ADD CONSTRAINT productos_pkey PRIMARY KEY (id_producto);


--
-- Name: routing_geocoding_usage routing_geocoding_usage_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.routing_geocoding_usage
    ADD CONSTRAINT routing_geocoding_usage_pkey PRIMARY KEY (id);


--
-- Name: rutas_completadas_ocultas rutas_completadas_ocultas_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.rutas_completadas_ocultas
    ADD CONSTRAINT rutas_completadas_ocultas_pkey PRIMARY KEY (id_hoja);


--
-- Name: salidas_reentregas_pendientes salidas_reentregas_pendientes_client_key_key; Type: CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.salidas_reentregas_pendientes
    ADD CONSTRAINT salidas_reentregas_pendientes_client_key_key UNIQUE (client_key);


--
-- Name: salidas_reentregas_pendientes salidas_reentregas_pendientes_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.salidas_reentregas_pendientes
    ADD CONSTRAINT salidas_reentregas_pendientes_pkey PRIMARY KEY (id_reentrega);


--
-- Name: sedes sedes_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.sedes
    ADD CONSTRAINT sedes_pkey PRIMARY KEY (id_sede);


--
-- Name: solicitudes_sedes_detalle solicitudes_sedes_detalle_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.solicitudes_sedes_detalle
    ADD CONSTRAINT solicitudes_sedes_detalle_pkey PRIMARY KEY (id_detalle);


--
-- Name: solicitudes_sedes solicitudes_sedes_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.solicitudes_sedes
    ADD CONSTRAINT solicitudes_sedes_pkey PRIMARY KEY (id_solicitud);


--
-- Name: solicitudes_sedes_sheets_outbox solicitudes_sedes_sheets_outbox_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.solicitudes_sedes_sheets_outbox
    ADD CONSTRAINT solicitudes_sedes_sheets_outbox_pkey PRIMARY KEY (id_sync);


--
-- Name: spc_masas spc_masas_lote_amasadora_key; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.spc_masas
    ADD CONSTRAINT spc_masas_lote_amasadora_key UNIQUE (lote, amasadora);


--
-- Name: spc_masas spc_masas_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.spc_masas
    ADD CONSTRAINT spc_masas_pkey PRIMARY KEY (id);


--
-- Name: entregas_sedes_detalle uq_entrega_producto; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.entregas_sedes_detalle
    ADD CONSTRAINT uq_entrega_producto UNIQUE (entrega_id, producto_id);


--
-- Name: predicciones_demanda uq_predicciones_fecha_sede_producto; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.predicciones_demanda
    ADD CONSTRAINT uq_predicciones_fecha_sede_producto UNIQUE (fecha_objetivo, sede_id, producto_id);


--
-- Name: solicitudes_sedes_detalle uq_solicitud_producto; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.solicitudes_sedes_detalle
    ADD CONSTRAINT uq_solicitud_producto UNIQUE (solicitud_id, producto_id);


--
-- Name: solicitudes_sedes_sheets_outbox uq_solicitudes_sedes_sheets_outbox_event; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.solicitudes_sedes_sheets_outbox
    ADD CONSTRAINT uq_solicitudes_sedes_sheets_outbox_event UNIQUE (event_type, referencia_externa);


--
-- Name: ventas_diarias uq_ventas_diarias_fecha_sede_producto; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.ventas_diarias
    ADD CONSTRAINT uq_ventas_diarias_fecha_sede_producto UNIQUE (fecha, sede_id, producto_id);


--
-- Name: ventas_diarias ventas_diarias_pkey; Type: CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.ventas_diarias
    ADD CONSTRAINT ventas_diarias_pkey PRIMARY KEY (id);


--
-- Name: idx_address_validations_address; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_address_validations_address ON public.address_validations USING btree (address);


--
-- Name: idx_almacen09_clientes_nombre_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_almacen09_clientes_nombre_unique ON public.almacen09_clientes USING btree (nombre);


--
-- Name: idx_almacen09_direcciones_direccion_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_almacen09_direcciones_direccion_unique ON public.almacen09_direcciones USING btree (direccion);


--
-- Name: idx_almacen09_sucursales_nombre_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_almacen09_sucursales_nombre_unique ON public.almacen09_sucursales USING btree (nombre);


--
-- Name: idx_almacen09_vendedores_codigo; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_almacen09_vendedores_codigo ON public.almacen09_vendedores USING btree (codigo_ven);


--
-- Name: idx_almacen09_vendedores_descripcion; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_almacen09_vendedores_descripcion ON public.almacen09_vendedores USING btree (lower(TRIM(BOTH FROM descripcion)));


--
-- Name: idx_almacen09_zonas_nombre_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_almacen09_zonas_nombre_unique ON public.almacen09_zonas USING btree (nombre);


--
-- Name: idx_almacen_lotes_procesados_estado_processed; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_almacen_lotes_procesados_estado_processed ON public.almacen_lotes_procesados USING btree (estado, processed_at DESC);


--
-- Name: idx_auth_sessions_revoked; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_auth_sessions_revoked ON public.auth_sessions USING btree (revoked_at);


--
-- Name: idx_auth_sessions_user; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_auth_sessions_user ON public.auth_sessions USING btree (id_user);


--
-- Name: idx_auth_users_role; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_auth_users_role ON public.auth_users USING btree (role);


--
-- Name: idx_auth_users_sede; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_auth_users_sede ON public.auth_users USING btree (sede_id);


--
-- Name: idx_auth_users_username_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_auth_users_username_unique ON public.auth_users USING btree (username);


--
-- Name: idx_cambios_razones_texto; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_cambios_razones_texto ON public.cambios_razones USING btree (razon_texto);


--
-- Name: idx_cambios_razones_texto_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_cambios_razones_texto_unique ON public.cambios_razones USING btree (razon_texto);


--
-- Name: idx_cambios_registros_codigo_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_cambios_registros_codigo_unique ON public.cambios_registros USING btree (codigo_cambio);


--
-- Name: idx_cambios_registros_created_at; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_cambios_registros_created_at ON public.cambios_registros USING btree (created_at DESC);


--
-- Name: idx_cambios_registros_grupo; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_cambios_registros_grupo ON public.cambios_registros USING btree (grupo_cambio);


--
-- Name: idx_cambios_registros_ruta; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_cambios_registros_ruta ON public.cambios_registros USING btree (ruta_nombre);


--
-- Name: idx_conciliacion_pagos_factura; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_conciliacion_pagos_factura ON public.conciliacion_pagos USING btree (id_factura);


--
-- Name: idx_conciliacion_pagos_fecha; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_conciliacion_pagos_fecha ON public.conciliacion_pagos USING btree (fecha_pago DESC);


--
-- Name: idx_conteo_errores_codigo_lote; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_conteo_errores_codigo_lote ON public.conteo_errores USING btree (codigo_lote);


--
-- Name: idx_conteo_errores_created_at; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_conteo_errores_created_at ON public.conteo_errores USING btree (created_at DESC);


--
-- Name: idx_control_inventario_guardia_created_at; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_control_inventario_guardia_created_at ON public.control_inventario_guardia USING btree (created_at DESC);


--
-- Name: idx_control_inventario_guardia_producto_fecha; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_control_inventario_guardia_producto_fecha ON public.control_inventario_guardia USING btree (id_producto, fecha_elaboracion DESC);


--
-- Name: idx_delivery_status_updated_at; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_delivery_status_updated_at ON public.delivery_status USING btree (updated_at DESC);


--
-- Name: idx_empaquetados_cabecera_fecha_hora; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_empaquetados_cabecera_fecha_hora ON public.empaquetados_cabecera USING btree (fecha_hora DESC);


--
-- Name: idx_empaquetados_cabecera_numero_registro; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_empaquetados_cabecera_numero_registro ON public.empaquetados_cabecera USING btree (numero_registro);


--
-- Name: idx_empaquetados_detalle_cabecera; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_empaquetados_detalle_cabecera ON public.empaquetados_detalle USING btree (id_cabecera);


--
-- Name: idx_empaquetados_detalle_codigo_producto; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_empaquetados_detalle_codigo_producto ON public.empaquetados_detalle USING btree (codigo_producto);


--
-- Name: idx_empaquetados_detalle_lote_upper; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_empaquetados_detalle_lote_upper ON public.empaquetados_detalle USING btree (upper(TRIM(BOTH FROM numero_lote)));


--
-- Name: idx_empaquetados_detalle_producto; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_empaquetados_detalle_producto ON public.empaquetados_detalle USING btree (id_producto);


--
-- Name: idx_facturas_bot_uploads_created_at; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_facturas_bot_uploads_created_at ON public.facturas_bot_uploads USING btree (created_at DESC);


--
-- Name: idx_facturas_bot_uploads_estado; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_facturas_bot_uploads_estado ON public.facturas_bot_uploads USING btree (estado);


--
-- Name: idx_facturas_bot_uploads_sha256; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_facturas_bot_uploads_sha256 ON public.facturas_bot_uploads USING btree (sha256);


--
-- Name: idx_historico_resultados_fecha_almacen09; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_historico_resultados_fecha_almacen09 ON public.historico_resultados_consolidado USING btree (fecha_almacen09 DESC);


--
-- Name: idx_historico_resultados_fecha_empaquetado; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_historico_resultados_fecha_empaquetado ON public.historico_resultados_consolidado USING btree (fecha_empaquetado DESC);


--
-- Name: idx_hojas_ruta_exportadas_created_at; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_hojas_ruta_exportadas_created_at ON public.hojas_ruta_exportadas USING btree (created_at DESC);


--
-- Name: idx_hojas_ruta_exportadas_ruta; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_hojas_ruta_exportadas_ruta ON public.hojas_ruta_exportadas USING btree (lower(TRIM(BOTH FROM ruta_nombre)));


--
-- Name: idx_idempotency_requests_created_at; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_idempotency_requests_created_at ON public.idempotency_requests USING btree (created_at DESC);


--
-- Name: idx_mermas_cabecera_fecha_hora; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_mermas_cabecera_fecha_hora ON public.mermas_cabecera USING btree (fecha_hora DESC);


--
-- Name: idx_mermas_cabecera_sede_responsable; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_mermas_cabecera_sede_responsable ON public.mermas_cabecera USING btree (id_sede, id_responsable);


--
-- Name: idx_mermas_detalle_lote_upper; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_mermas_detalle_lote_upper ON public.mermas_detalle USING btree (upper(TRIM(BOTH FROM numero_lote)));


--
-- Name: idx_mermas_detalle_merma; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_mermas_detalle_merma ON public.mermas_detalle USING btree (id_merma);


--
-- Name: idx_mermas_detalle_producto_motivo; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_mermas_detalle_producto_motivo ON public.mermas_detalle USING btree (id_producto, id_motivo);


--
-- Name: idx_motivos_merma_nombre_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_motivos_merma_nombre_unique ON public.motivos_merma USING btree (nombre);


--
-- Name: idx_productos_activo_codigo; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_productos_activo_codigo ON public.productos USING btree (activo, codigo_producto);


--
-- Name: idx_productos_audit_changed_at; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_productos_audit_changed_at ON public.productos_audit USING btree (changed_at DESC);


--
-- Name: idx_productos_codigo_barras_lookup; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_productos_codigo_barras_lookup ON public.productos USING btree (codigo_barras) WHERE ((codigo_barras IS NOT NULL) AND (TRIM(BOTH FROM codigo_barras) <> ''::text));


--
-- Name: idx_productos_codigo_barras_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_productos_codigo_barras_unique ON public.productos USING btree (upper(TRIM(BOTH FROM codigo_barras))) WHERE ((codigo_barras IS NOT NULL) AND (TRIM(BOTH FROM codigo_barras) <> ''::text));


--
-- Name: idx_productos_codigo_producto_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_productos_codigo_producto_unique ON public.productos USING btree (codigo_producto);


--
-- Name: idx_productos_codigo_upper; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_productos_codigo_upper ON public.productos USING btree (upper(TRIM(BOTH FROM codigo_producto)));


--
-- Name: idx_productos_codigo_upper_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_productos_codigo_upper_unique ON public.productos USING btree (upper(TRIM(BOTH FROM codigo_producto)));


--
-- Name: idx_public_clientes_descripcion_direccion; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_public_clientes_descripcion_direccion ON public.clientes USING btree (lower(TRIM(BOTH FROM descripcion)), lower(TRIM(BOTH FROM direccion)));


--
-- Name: idx_public_clientes_descripcion_zona; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_public_clientes_descripcion_zona ON public.clientes USING btree (lower(TRIM(BOTH FROM descripcion)), lower(TRIM(BOTH FROM zona)));


--
-- Name: idx_routing_geocoding_usage_provider_created; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_routing_geocoding_usage_provider_created ON public.routing_geocoding_usage USING btree (provider, created_at);


--
-- Name: idx_rutas_completadas_ocultas_hidden_at; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_rutas_completadas_ocultas_hidden_at ON public.rutas_completadas_ocultas USING btree (hidden_at DESC);


--
-- Name: idx_salidas09_detalle_codigo_lote; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_salidas09_detalle_codigo_lote ON public.almacen09_salidas_detalle USING btree (codigo_producto, numero_lote);


--
-- Name: idx_salidas09_detalle_factura; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_salidas09_detalle_factura ON public.almacen09_salidas_detalle USING btree (id_factura);


--
-- Name: idx_salidas09_detalle_id_cambio; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_salidas09_detalle_id_cambio ON public.almacen09_salidas_detalle USING btree (id_cambio);


--
-- Name: idx_salidas09_facturas_documento; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_salidas09_facturas_documento ON public.salidas_facturas USING btree (documento);


--
-- Name: idx_salidas09_facturas_documento_numero; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_salidas09_facturas_documento_numero ON public.salidas_facturas USING btree (documento, numero_factura);


--
-- Name: idx_salidas09_facturas_documento_numero_lookup; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_salidas09_facturas_documento_numero_lookup ON public.salidas_facturas USING btree (documento, numero_factura);


--
-- Name: idx_salidas09_facturas_fecha; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_salidas09_facturas_fecha ON public.salidas_facturas USING btree (fecha_emision DESC);


--
-- Name: idx_salidas09_facturas_numero_control; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_salidas09_facturas_numero_control ON public.salidas_facturas USING btree (numero_control);


--
-- Name: idx_salidas09_facturas_numero_control_lookup; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_salidas09_facturas_numero_control_lookup ON public.salidas_facturas USING btree (numero_control);


--
-- Name: idx_salidas_cliente_sucursal_key; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX idx_salidas_cliente_sucursal_key ON public.salidas_cliente_sucursales USING btree (cliente_key, sucursal_key);


--
-- Name: idx_salidas_cliente_sucursal_updated; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_salidas_cliente_sucursal_updated ON public.salidas_cliente_sucursales USING btree (updated_at DESC);


--
-- Name: idx_salidas_reentregas_pendientes_estado; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_salidas_reentregas_pendientes_estado ON public.salidas_reentregas_pendientes USING btree (estado, updated_at DESC);


--
-- Name: idx_salidas_reentregas_pendientes_ruta; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE INDEX idx_salidas_reentregas_pendientes_ruta ON public.salidas_reentregas_pendientes USING btree (lower(TRIM(BOTH FROM ruta_nombre)), fecha_emision);


--
-- Name: idx_solicitudes_sedes_sheets_outbox_entity; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX idx_solicitudes_sedes_sheets_outbox_entity ON public.solicitudes_sedes_sheets_outbox USING btree (entity_type, entity_id);


--
-- Name: idx_solicitudes_sedes_sheets_outbox_pending; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX idx_solicitudes_sedes_sheets_outbox_pending ON public.solicitudes_sedes_sheets_outbox USING btree (estado, updated_at, id_sync) WHERE ((estado)::text = ANY ((ARRAY['pendiente'::character varying, 'error'::character varying])::text[]));


--
-- Name: ix_entregas_detalle_entrega; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_entregas_detalle_entrega ON public.entregas_sedes_detalle USING btree (entrega_id);


--
-- Name: ix_entregas_detalle_producto; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_entregas_detalle_producto ON public.entregas_sedes_detalle USING btree (producto_id);


--
-- Name: ix_entregas_fecha_sede; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_entregas_fecha_sede ON public.entregas_sedes USING btree (fecha, sede_id);


--
-- Name: ix_entregas_sede; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_entregas_sede ON public.entregas_sedes USING btree (sede_id);


--
-- Name: ix_entregas_solicitud; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_entregas_solicitud ON public.entregas_sedes USING btree (solicitud_id);


--
-- Name: ix_predicciones_fecha_objetivo; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_predicciones_fecha_objetivo ON public.predicciones_demanda USING btree (fecha_objetivo);


--
-- Name: ix_predicciones_producto_id; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_predicciones_producto_id ON public.predicciones_demanda USING btree (producto_id);


--
-- Name: ix_predicciones_sede_id; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_predicciones_sede_id ON public.predicciones_demanda USING btree (sede_id);


--
-- Name: ix_solicitudes_detalle_producto; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_solicitudes_detalle_producto ON public.solicitudes_sedes_detalle USING btree (producto_id);


--
-- Name: ix_solicitudes_detalle_solicitud; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_solicitudes_detalle_solicitud ON public.solicitudes_sedes_detalle USING btree (solicitud_id);


--
-- Name: ix_solicitudes_estado; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_solicitudes_estado ON public.solicitudes_sedes USING btree (estado);


--
-- Name: ix_solicitudes_fecha_sede; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_solicitudes_fecha_sede ON public.solicitudes_sedes USING btree (fecha, sede_id);


--
-- Name: ix_solicitudes_sede; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_solicitudes_sede ON public.solicitudes_sedes USING btree (sede_id);


--
-- Name: ix_ventas_diarias_fecha; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_ventas_diarias_fecha ON public.ventas_diarias USING btree (fecha);


--
-- Name: ix_ventas_diarias_producto_id; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_ventas_diarias_producto_id ON public.ventas_diarias USING btree (producto_id);


--
-- Name: ix_ventas_diarias_sede_id; Type: INDEX; Schema: public; Owner: admin01
--

CREATE INDEX ix_ventas_diarias_sede_id ON public.ventas_diarias USING btree (sede_id);


--
-- Name: repair_almacen_lotes_codigo_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX repair_almacen_lotes_codigo_unique ON public.almacen_lotes_procesados USING btree (codigo_lote);


--
-- Name: repair_cambios_id_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX repair_cambios_id_unique ON public.cambios_registros USING btree (id_cambio);


--
-- Name: repair_conteo_errores_id_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX repair_conteo_errores_id_unique ON public.conteo_errores USING btree (id);


--
-- Name: repair_control_inventario_id_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX repair_control_inventario_id_unique ON public.control_inventario_guardia USING btree (id_control);


--
-- Name: repair_empaquetados_cabecera_id_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX repair_empaquetados_cabecera_id_unique ON public.empaquetados_cabecera USING btree (id_cabecera);


--
-- Name: repair_empaquetados_detalle_id_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX repair_empaquetados_detalle_id_unique ON public.empaquetados_detalle USING btree (id_detalle);


--
-- Name: repair_hojas_ruta_id_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX repair_hojas_ruta_id_unique ON public.hojas_ruta_exportadas USING btree (id_hoja);


--
-- Name: repair_mermas_cabecera_id_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX repair_mermas_cabecera_id_unique ON public.mermas_cabecera USING btree (id_merma);


--
-- Name: repair_mermas_detalle_id_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX repair_mermas_detalle_id_unique ON public.mermas_detalle USING btree (id_detalle);


--
-- Name: repair_productos_id_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX repair_productos_id_unique ON public.productos USING btree (id_producto);


--
-- Name: repair_salidas_detalle_id_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX repair_salidas_detalle_id_unique ON public.almacen09_salidas_detalle USING btree (id_detalle);


--
-- Name: repair_salidas_facturas_id_unique; Type: INDEX; Schema: public; Owner: admin01_pasante
--

CREATE UNIQUE INDEX repair_salidas_facturas_id_unique ON public.salidas_facturas USING btree (id_factura);


--
-- Name: uq_entregas_referencia_externa; Type: INDEX; Schema: public; Owner: admin01
--

CREATE UNIQUE INDEX uq_entregas_referencia_externa ON public.entregas_sedes USING btree (referencia_externa) WHERE (referencia_externa IS NOT NULL);


--
-- Name: uq_solicitudes_referencia_externa; Type: INDEX; Schema: public; Owner: admin01
--

CREATE UNIQUE INDEX uq_solicitudes_referencia_externa ON public.solicitudes_sedes USING btree (referencia_externa) WHERE (referencia_externa IS NOT NULL);


--
-- Name: entregas_sedes trg_entregas_updated_at; Type: TRIGGER; Schema: public; Owner: admin01
--

CREATE TRIGGER trg_entregas_updated_at BEFORE UPDATE ON public.entregas_sedes FOR EACH ROW EXECUTE FUNCTION public.pronostico_actualizar_updated_at();


--
-- Name: productos trg_productos_audit; Type: TRIGGER; Schema: public; Owner: admin01_pasante
--

CREATE TRIGGER trg_productos_audit AFTER INSERT OR DELETE OR UPDATE ON public.productos FOR EACH ROW EXECUTE FUNCTION public.productos_audit_trigger_fn();


--
-- Name: solicitudes_sedes trg_solicitudes_updated_at; Type: TRIGGER; Schema: public; Owner: admin01
--

CREATE TRIGGER trg_solicitudes_updated_at BEFORE UPDATE ON public.solicitudes_sedes FOR EACH ROW EXECUTE FUNCTION public.pronostico_actualizar_updated_at();


--
-- Name: conciliacion_pagos conciliacion_pagos_id_factura_fkey; Type: FK CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.conciliacion_pagos
    ADD CONSTRAINT conciliacion_pagos_id_factura_fkey FOREIGN KEY (id_factura) REFERENCES public.salidas_facturas(id_factura) ON DELETE CASCADE;


--
-- Name: auth_users fk_auth_users_sede; Type: FK CONSTRAINT; Schema: public; Owner: admin01_pasante
--

ALTER TABLE ONLY public.auth_users
    ADD CONSTRAINT fk_auth_users_sede FOREIGN KEY (sede_id) REFERENCES public.sedes(id_sede) ON UPDATE CASCADE ON DELETE SET NULL;


--
-- Name: entregas_sedes_detalle fk_entrega_detalle_entrega; Type: FK CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.entregas_sedes_detalle
    ADD CONSTRAINT fk_entrega_detalle_entrega FOREIGN KEY (entrega_id) REFERENCES public.entregas_sedes(id_entrega) ON DELETE CASCADE;


--
-- Name: entregas_sedes_detalle fk_entrega_detalle_producto; Type: FK CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.entregas_sedes_detalle
    ADD CONSTRAINT fk_entrega_detalle_producto FOREIGN KEY (producto_id) REFERENCES public.productos(id_producto);


--
-- Name: entregas_sedes fk_entregas_sede; Type: FK CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.entregas_sedes
    ADD CONSTRAINT fk_entregas_sede FOREIGN KEY (sede_id) REFERENCES public.sedes(id_sede);


--
-- Name: entregas_sedes fk_entregas_solicitud; Type: FK CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.entregas_sedes
    ADD CONSTRAINT fk_entregas_solicitud FOREIGN KEY (solicitud_id) REFERENCES public.solicitudes_sedes(id_solicitud) ON DELETE SET NULL;


--
-- Name: predicciones_demanda fk_predicciones_producto; Type: FK CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.predicciones_demanda
    ADD CONSTRAINT fk_predicciones_producto FOREIGN KEY (producto_id) REFERENCES public.productos(id_producto);


--
-- Name: predicciones_demanda fk_predicciones_sede; Type: FK CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.predicciones_demanda
    ADD CONSTRAINT fk_predicciones_sede FOREIGN KEY (sede_id) REFERENCES public.sedes(id_sede);


--
-- Name: solicitudes_sedes_detalle fk_solicitud_detalle_producto; Type: FK CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.solicitudes_sedes_detalle
    ADD CONSTRAINT fk_solicitud_detalle_producto FOREIGN KEY (producto_id) REFERENCES public.productos(id_producto);


--
-- Name: solicitudes_sedes_detalle fk_solicitud_detalle_solicitud; Type: FK CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.solicitudes_sedes_detalle
    ADD CONSTRAINT fk_solicitud_detalle_solicitud FOREIGN KEY (solicitud_id) REFERENCES public.solicitudes_sedes(id_solicitud) ON DELETE CASCADE;


--
-- Name: solicitudes_sedes fk_solicitudes_sede; Type: FK CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.solicitudes_sedes
    ADD CONSTRAINT fk_solicitudes_sede FOREIGN KEY (sede_id) REFERENCES public.sedes(id_sede);


--
-- Name: ventas_diarias fk_ventas_diarias_producto; Type: FK CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.ventas_diarias
    ADD CONSTRAINT fk_ventas_diarias_producto FOREIGN KEY (producto_id) REFERENCES public.productos(id_producto);


--
-- Name: ventas_diarias fk_ventas_diarias_sede; Type: FK CONSTRAINT; Schema: public; Owner: admin01
--

ALTER TABLE ONLY public.ventas_diarias
    ADD CONSTRAINT fk_ventas_diarias_sede FOREIGN KEY (sede_id) REFERENCES public.sedes(id_sede);


--
-- Name: TABLE spc_masas; Type: ACL; Schema: public; Owner: admin01
--

GRANT ALL ON TABLE public.spc_masas TO admin01_pasante;


--
-- Name: SEQUENCE spc_masas_id_seq; Type: ACL; Schema: public; Owner: admin01
--

GRANT SELECT,USAGE ON SEQUENCE public.spc_masas_id_seq TO admin01_pasante;


--
-- PostgreSQL database dump complete
--

\unrestrict ZGbI1MeIj6qdsOVujTvy0f9JnbN77LX3VXUh294bYBuzPB51wERVtsEcpl40iMm

