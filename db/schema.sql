\restrict dbmate

-- Dumped from database version 16.13
-- Dumped by pg_dump version 17.9

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: timescaledb; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS timescaledb WITH SCHEMA public;


--
-- Name: EXTENSION timescaledb; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION timescaledb IS 'Enables scalable inserts and complex queries for time-series data (Community Edition)';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: _compressed_hypertable_2; Type: TABLE; Schema: _timescaledb_internal; Owner: -
--

CREATE TABLE _timescaledb_internal._compressed_hypertable_2 (
);


--
-- Name: aircraft; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.aircraft (
    hex text NOT NULL,
    first_seen timestamp with time zone NOT NULL,
    last_seen timestamp with time zone NOT NULL,
    callsigns text[] DEFAULT '{}'::text[] NOT NULL
);


--
-- Name: callsign_routes; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.callsign_routes (
    callsign text NOT NULL,
    origin_iata text,
    origin_icao text,
    origin_city text,
    origin_country text,
    dest_iata text,
    dest_icao text,
    dest_city text,
    dest_country text,
    fetched_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: digests; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.digests (
    id bigint NOT NULL,
    reference_date date NOT NULL,
    n_days integer NOT NULL,
    content text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: digests_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.digests ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.digests_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: enriched_aircraft; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.enriched_aircraft (
    hex text NOT NULL,
    registration text,
    type text,
    operator text,
    flag text,
    story_score integer,
    story_tags text[] DEFAULT '{}'::text[] NOT NULL,
    annotation text,
    enriched_at timestamp with time zone DEFAULT now() NOT NULL,
    expires_at timestamp with time zone NOT NULL
);


--
-- Name: position_updates; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.position_updates (
    "time" timestamp with time zone NOT NULL,
    hex text NOT NULL,
    lat double precision,
    lon double precision,
    alt_baro integer,
    gs double precision,
    track double precision,
    squawk text,
    rssi double precision
);


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying NOT NULL
);


--
-- Name: sightings; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.sightings (
    id bigint NOT NULL,
    hex text NOT NULL,
    callsign text,
    started_at timestamp with time zone NOT NULL,
    ended_at timestamp with time zone,
    last_seen timestamp with time zone NOT NULL,
    min_altitude integer,
    max_altitude integer,
    min_distance double precision,
    max_distance double precision
);


--
-- Name: sightings_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.sightings ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.sightings_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    chat_id bigint NOT NULL,
    username text,
    registered_at timestamp with time zone DEFAULT now() NOT NULL,
    active boolean DEFAULT true NOT NULL
);


--
-- Name: aircraft aircraft_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.aircraft
    ADD CONSTRAINT aircraft_pkey PRIMARY KEY (hex);


--
-- Name: callsign_routes callsign_routes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.callsign_routes
    ADD CONSTRAINT callsign_routes_pkey PRIMARY KEY (callsign);


--
-- Name: digests digests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.digests
    ADD CONSTRAINT digests_pkey PRIMARY KEY (id);


--
-- Name: digests digests_reference_date_n_days_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.digests
    ADD CONSTRAINT digests_reference_date_n_days_key UNIQUE (reference_date, n_days);


--
-- Name: enriched_aircraft enriched_aircraft_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.enriched_aircraft
    ADD CONSTRAINT enriched_aircraft_pkey PRIMARY KEY (hex);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: sightings sightings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sightings
    ADD CONSTRAINT sightings_pkey PRIMARY KEY (id);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (chat_id);


--
-- Name: position_updates_time_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX position_updates_time_idx ON public.position_updates USING btree ("time" DESC);


--
-- Name: enriched_aircraft enriched_aircraft_hex_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.enriched_aircraft
    ADD CONSTRAINT enriched_aircraft_hex_fkey FOREIGN KEY (hex) REFERENCES public.aircraft(hex);


--
-- Name: sightings sightings_hex_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.sightings
    ADD CONSTRAINT sightings_hex_fkey FOREIGN KEY (hex) REFERENCES public.aircraft(hex);


--
-- PostgreSQL database dump complete
--

\unrestrict dbmate


--
-- Dbmate schema migrations
--

INSERT INTO public.schema_migrations (version) VALUES
    ('20260417192449'),
    ('20260417192450');
