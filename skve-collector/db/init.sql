-- SKVE Collector – Datenbankschema
-- Läuft einmalig beim ersten Start des Timescale-Containers.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ---------------------------------------------------------------------------
-- Preise (DAA, IDA) und Prognose (forecast)
-- Eine Tabelle für alle Preisreihen, unterschieden über die Spalte "series".
-- series-Werte: 'DAA', 'IDA', 'FORECAST_DAA'
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS prices (
    ts        TIMESTAMPTZ   NOT NULL,   -- Beginn der Viertelstunde (UTC)
    series    TEXT          NOT NULL,   -- 'DAA' | 'IDA' | 'FORECAST_DAA'
    eur_mwh   DOUBLE PRECISION,
    PRIMARY KEY (ts, series)
);
SELECT create_hypertable('prices', 'ts', if_not_exists => TRUE);

-- ---------------------------------------------------------------------------
-- Fahrpläne je BHKW-Motor
-- chp_id-Werte: 499 (Jenb.901 S), 500 (Jenb.548), 501 (MWM 400 S), 502 (MAN 252)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS chp_schedule (
    ts        TIMESTAMPTZ   NOT NULL,   -- Messzeitpunkt (UTC)
    chp_id    INTEGER       NOT NULL,
    kw        DOUBLE PRECISION,         -- geplante/anliegende Leistung in kW
    PRIMARY KEY (ts, chp_id)
);
SELECT create_hypertable('chp_schedule', 'ts', if_not_exists => TRUE);

-- ---------------------------------------------------------------------------
-- Stammdaten der Motoren (statisch, für Joins in Grafana)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS chp_master (
    chp_id    INTEGER PRIMARY KEY,
    name      TEXT NOT NULL,
    section   TEXT NOT NULL,            -- 'Hauptstandort' | 'Satellit'
    nenn_kw   INTEGER NOT NULL
);

INSERT INTO chp_master (chp_id, name, section, nenn_kw) VALUES
    (499, '4. Jenb. 901 S',        'Satellit',      901),
    (500, '3. Jenb. 548',          'Hauptstandort', 548),
    (501, '2. MWM 400 S',          'Satellit',      390),
    (502, '1. MAN 252',            'Hauptstandort', 252)
ON CONFLICT (chp_id) DO UPDATE
    SET name = EXCLUDED.name,
        section = EXCLUDED.section,
        nenn_kw = EXCLUDED.nenn_kw;

-- ---------------------------------------------------------------------------
-- Fortschritts-Marker: pro Datenreihe der zuletzt erfolgreich geladene Zeitpunkt.
-- Damit weiß der Collector beim nächsten Lauf, wo er weitermachen muss
-- (Nachhol-Logik / nahtloser Wiederanlauf).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingest_state (
    stream        TEXT PRIMARY KEY,     -- z.B. 'price:DAA', 'chp:499'
    last_ts       TIMESTAMPTZ,          -- letzter geladener Datenpunkt
    last_run      TIMESTAMPTZ,          -- Zeitpunkt des letzten Laufs
    last_status   TEXT                  -- 'ok' | Fehlermeldung
);

-- ---------------------------------------------------------------------------
-- Komfort-View: Erlös je Viertelstunde und Motor (DAA-Preis × Leistung)
-- Rechnet den Spotmarkt-Rohertrag – nur Lesen, keine gespeicherten Werte.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_erloes AS
SELECT
    s.ts,
    s.chp_id,
    m.name,
    m.section,
    s.kw,
    p.eur_mwh,
    s.kw * 0.25 / 1000.0 * p.eur_mwh AS erloes_eur,
    s.kw * 0.25 / 1000.0             AS mwh
FROM chp_schedule s
JOIN chp_master m ON m.chp_id = s.chp_id
LEFT JOIN prices p ON p.ts = s.ts AND p.series = 'DAA';
