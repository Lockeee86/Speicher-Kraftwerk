#!/usr/bin/env python3
"""
SKVE Collector
--------------
Zieht täglich (oder im eingestellten Intervall) Preise, Fahrpläne und Prognose
von der Speicher-Kraftwerk-API und schreibt sie idempotent in TimescaleDB.

Nur lesende Endpunkte. Der POST-/VPP-Schreibpfad wird bewusst NICHT verwendet.

Nahtlosigkeit:
- Pro Datenstrom wird der zuletzt geladene Zeitstempel in `ingest_state` gemerkt.
- Beim Start holt der Collector alles seit diesem Zeitstempel (mit kleinem
  Überlappungs-Puffer). War der Dienst tagelang aus, werden die Tage nachgeholt.
- Geschrieben wird per UPSERT (ON CONFLICT), Doppelläufe erzeugen keine Duplikate.
"""

import os
import sys
import time
import logging
from datetime import datetime, timezone, timedelta

import requests
import psycopg2
from psycopg2.extras import execute_values

# --------------------------------------------------------------------------- #
# Konfiguration aus Umgebungsvariablen (in Portainer gesetzt)
# --------------------------------------------------------------------------- #
API_BASE   = os.environ.get("SKVE_API_BASE", "https://www.speicher-kraftwerk.de/api")
API_KEY    = os.environ.get("SKVE_API_KEY", "").strip()

DB_HOST    = os.environ.get("DB_HOST", "timescaledb")
DB_PORT    = int(os.environ.get("DB_PORT", "5432"))
DB_NAME    = os.environ.get("DB_NAME", "skve")
DB_USER    = os.environ.get("DB_USER", "skve")
DB_PASS    = os.environ.get("DB_PASSWORD", "skve")

# Motoren, die abgefragt werden
CHP_IDS    = [int(x) for x in os.environ.get("SKVE_CHP_IDS", "499,500,501,502").split(",")]
# Preisreihen
PRICE_SERIES    = [s.strip() for s in os.environ.get("SKVE_PRICE_SERIES", "DAA,IDA").split(",") if s.strip()]
FORECAST_MARKETS = [s.strip() for s in os.environ.get("SKVE_FORECAST_MARKETS", "DAA").split(",") if s.strip()]

# Intervall zwischen zwei Läufen (Sekunden). Default: 1x täglich.
RUN_INTERVAL_SEC = int(os.environ.get("RUN_INTERVAL_SEC", str(24 * 3600)))
# Einmal laufen und beenden statt Dauerschleife? (für externen Cron)
RUN_ONCE   = os.environ.get("RUN_ONCE", "false").lower() in ("1", "true", "yes")

# Startdatum für den allerersten Lauf, falls kein State existiert (YYYY-MM-DD).
# Leer = ab heute minus BACKFILL_FALLBACK_DAYS.
INITIAL_START_DATE = os.environ.get("SKVE_INITIAL_START_DATE", "").strip()
BACKFILL_FALLBACK_DAYS = int(os.environ.get("BACKFILL_FALLBACK_DAYS", "3"))

# Überlappungs-Puffer: bei jedem Lauf etwas vor dem letzten Punkt neu anfragen,
# um Randlücken sicher zu füllen (UPSERT verhindert Duplikate).
OVERLAP_HOURS = int(os.environ.get("OVERLAP_HOURS", "6"))

# Wie weit maximal in die Zukunft ziehen (Fahrpläne/Prognose liegen voraus).
FUTURE_HORIZON_DAYS = int(os.environ.get("FUTURE_HORIZON_DAYS", "2"))

# Maximale Fenstergröße pro API-Abfrage (Tage). Größere Zeiträume werden in
# Stücke dieser Länge zerlegt – die API liefert mehr als ~1 Monat am Stück
# nicht zuverlässig. Gilt auch für den Backfill.
MAX_CHUNK_DAYS = int(os.environ.get("MAX_CHUNK_DAYS", "14"))

# Kurze Pause zwischen zwei Chunks, um die API nicht zu überlasten (Sekunden).
CHUNK_PAUSE_SEC = float(os.environ.get("CHUNK_PAUSE_SEC", "0.5"))

# Einmaliger Backfill ab diesem Datum (YYYY-MM-DD). Wenn gesetzt, wird der
# gespeicherte Fortschritt ignoriert und ab hier neu geladen (idempotent).
# Nach abgeschlossenem Backfill wieder leeren, damit der normale Betrieb greift.
BACKFILL_FROM = os.environ.get("SKVE_BACKFILL_FROM", "").strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("skve")


# --------------------------------------------------------------------------- #
# Hilfsfunktionen
# --------------------------------------------------------------------------- #
def epoch(dt: datetime) -> int:
    return int(dt.timestamp())


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def connect_db(retries: int = 10, delay: int = 3):
    """Warte beim Start, bis die DB erreichbar ist (Container-Reihenfolge)."""
    last = None
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASS,
            )
            conn.autocommit = False
            return conn
        except psycopg2.OperationalError as e:
            last = e
            log.info("DB noch nicht bereit (%d/%d) – warte %ds …", i + 1, retries, delay)
            time.sleep(delay)
    raise SystemExit(f"DB nicht erreichbar: {last}")


# Datenbankschema (idempotent). Wird bei jedem Start sichergestellt, damit der
# Collector nicht auf ein einmalig laufendes init.sql angewiesen ist – wichtig
# u.a. in Portainer/dind-Setups, in denen Bind-Mounts aus dem Git-Repo nicht
# ankommen. Deckungsgleich mit db/init.sql.
SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS prices (
    ts        TIMESTAMPTZ   NOT NULL,
    series    TEXT          NOT NULL,
    eur_mwh   DOUBLE PRECISION,
    PRIMARY KEY (ts, series)
);
SELECT create_hypertable('prices', 'ts', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS chp_schedule (
    ts        TIMESTAMPTZ   NOT NULL,
    chp_id    INTEGER       NOT NULL,
    kw        DOUBLE PRECISION,
    PRIMARY KEY (ts, chp_id)
);
SELECT create_hypertable('chp_schedule', 'ts', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS chp_master (
    chp_id    INTEGER PRIMARY KEY,
    name      TEXT NOT NULL,
    section   TEXT NOT NULL,
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

CREATE TABLE IF NOT EXISTS ingest_state (
    stream        TEXT PRIMARY KEY,
    last_ts       TIMESTAMPTZ,
    last_run      TIMESTAMPTZ,
    last_status   TEXT
);

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
"""


def ensure_schema(conn):
    """Legt Tabellen/Views idempotent an. Läuft bei jedem Start."""
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()
    log.info("DB-Schema sichergestellt (Tabellen/Views vorhanden).")


def api_get(path: str, start: datetime, end: datetime) -> dict:
    """GET mit x-api-key. Gibt geparstes JSON zurück oder wirft."""
    url = f"{API_BASE}{path}"
    params = {"start_date": epoch(start), "end_date": epoch(end)}
    headers = {"x-api-key": API_KEY, "Content-Type": "application/json"}
    r = requests.get(url, params=params, headers=headers, timeout=60)
    if r.status_code == 401:
        raise RuntimeError("401 – API-Key ungültig oder abgelaufen")
    if r.status_code == 403:
        raise RuntimeError(f"403 – Zugriff verweigert auf {path}")
    r.raise_for_status()
    return r.json()


def get_state(cur, stream: str):
    cur.execute("SELECT last_ts FROM ingest_state WHERE stream = %s", (stream,))
    row = cur.fetchone()
    return row[0] if row else None


def set_state(cur, stream: str, last_ts, status: str):
    cur.execute(
        """
        INSERT INTO ingest_state (stream, last_ts, last_run, last_status)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (stream) DO UPDATE
          SET last_ts = COALESCE(EXCLUDED.last_ts, ingest_state.last_ts),
              last_run = EXCLUDED.last_run,
              last_status = EXCLUDED.last_status
        """,
        (stream, last_ts, now_utc(), status),
    )


def window_for(cur, stream: str):
    """Berechne (start, end) für den nächsten Abruf eines Streams."""
    end = now_utc() + timedelta(days=FUTURE_HORIZON_DAYS)
    # Einmaliger Backfill: ignoriert den gespeicherten Fortschritt.
    if BACKFILL_FROM:
        start = datetime.fromisoformat(BACKFILL_FROM).replace(tzinfo=timezone.utc)
        return start, end
    last_ts = get_state(cur, stream)
    if last_ts is None:
        if INITIAL_START_DATE:
            start = datetime.fromisoformat(INITIAL_START_DATE).replace(tzinfo=timezone.utc)
        else:
            start = now_utc() - timedelta(days=BACKFILL_FALLBACK_DAYS)
    else:
        start = last_ts - timedelta(hours=OVERLAP_HOURS)
    return start, end


def daterange_chunks(start: datetime, end: datetime, days: int):
    """Zerlegt [start, end) in Stücke von höchstens `days` Tagen."""
    if days <= 0:
        yield start, end
        return
    step = timedelta(days=days)
    cur = start
    while cur < end:
        nxt = min(cur + step, end)
        yield cur, nxt
        cur = nxt


# --------------------------------------------------------------------------- #
# Parser: API liefert {"times":[...], "values":[...]} bzw. timeSeries-Varianten
# --------------------------------------------------------------------------- #
def extract_series(payload: dict):
    """
    Robust gegen die beiden gesehenen Formate:
      A) {"times":[...], "values":[...]}
      B) {"timeSeries": {"price": {"timestamps":[...], "values":[...]}}} o.ä.
    Gibt Liste von (epoch_seconds, value) zurück.
    """
    # Format A
    if "times" in payload and "values" in payload:
        return list(zip(payload["times"], payload["values"]))
    # Format B – erste Reihe mit timestamps/values finden
    def find(node):
        if isinstance(node, dict):
            if "timestamps" in node and "values" in node:
                return list(zip(node["timestamps"], node["values"]))
            for v in node.values():
                r = find(v)
                if r:
                    return r
        return None
    r = find(payload)
    return r or []


def upsert_prices(cur, series: str, rows):
    if not rows:
        return 0, None
    data = [(datetime.fromtimestamp(t, timezone.utc), series, v) for t, v in rows]
    execute_values(
        cur,
        "INSERT INTO prices (ts, series, eur_mwh) VALUES %s "
        "ON CONFLICT (ts, series) DO UPDATE SET eur_mwh = EXCLUDED.eur_mwh",
        data,
    )
    return len(data), max(d[0] for d in data)


def upsert_chp(cur, chp_id: int, rows):
    if not rows:
        return 0, None
    data = [(datetime.fromtimestamp(t, timezone.utc), chp_id, v) for t, v in rows]
    execute_values(
        cur,
        "INSERT INTO chp_schedule (ts, chp_id, kw) VALUES %s "
        "ON CONFLICT (ts, chp_id) DO UPDATE SET kw = EXCLUDED.kw",
        data,
    )
    return len(data), max(d[0] for d in data)


# --------------------------------------------------------------------------- #
# Ein Stream, chunk-weise geladen (fortsetzbar)
# --------------------------------------------------------------------------- #
def fetch_stream(conn, cur, stream, path, upsert_fn, label):
    """Lädt einen Datenstrom im Zeitfenster in Stücken von MAX_CHUNK_DAYS.

    Nach jedem Chunk wird committet und der Fortschritt gespeichert – bricht ein
    langer Backfill ab, macht der nächste Lauf beim letzten Chunk weiter.
    Gibt (Gesamtanzahl, letzter_zeitstempel) zurück.
    """
    start, end = window_for(cur, stream)
    total = 0
    last_overall = get_state(cur, stream)
    chunks = list(daterange_chunks(start, end, MAX_CHUNK_DAYS))
    multi = len(chunks) > 1
    for i, (cs, ce) in enumerate(chunks):
        payload = api_get(path, cs, ce)
        rows = extract_series(payload)
        n, last = upsert_fn(cur, rows)
        total += n
        if last and (last_overall is None or last > last_overall):
            last_overall = last
        set_state(cur, stream, last_overall, "ok")
        conn.commit()
        if multi:
            log.info("  %-14s Chunk %2d/%d  %s–%s → %5d Werte",
                     label, i + 1, len(chunks), cs.date(), ce.date(), n)
            if CHUNK_PAUSE_SEC and i < len(chunks) - 1:
                time.sleep(CHUNK_PAUSE_SEC)
    return total, last_overall


# --------------------------------------------------------------------------- #
# Ein kompletter Durchlauf
# --------------------------------------------------------------------------- #
def run_once(conn):
    cur = conn.cursor()

    # 1) Preise (DAA, IDA)
    for series in PRICE_SERIES:
        stream = f"price:{series}"
        try:
            n, last = fetch_stream(
                conn, cur, stream, f"/module/data-service/prices/{series}",
                lambda c, rows, s=series: upsert_prices(c, s, rows),
                f"Preise {series}",
            )
            log.info("Preise %-4s: %5d Werte (bis %s)", series, n,
                     last.isoformat() if last else "—")
        except Exception as e:
            conn.rollback()
            with conn.cursor() as c2:
                set_state(c2, stream, None, str(e)[:200]); conn.commit()
            log.error("Preise %s fehlgeschlagen: %s", series, e)

    # 2) Prognose (forecast/DAA)
    for market in FORECAST_MARKETS:
        stream = f"forecast:{market}"
        series = f"FORECAST_{market}"
        try:
            n, last = fetch_stream(
                conn, cur, stream, f"/module/data-service/prices/forecast/{market}",
                lambda c, rows, s=series: upsert_prices(c, s, rows),
                f"Prognose {market}",
            )
            log.info("Prognose %-4s: %5d Werte (bis %s)", market, n,
                     last.isoformat() if last else "—")
        except Exception as e:
            conn.rollback()
            with conn.cursor() as c2:
                set_state(c2, stream, None, str(e)[:200]); conn.commit()
            log.error("Prognose %s fehlgeschlagen: %s", market, e)

    # 3) Fahrpläne je Motor
    for chp_id in CHP_IDS:
        stream = f"chp:{chp_id}"
        try:
            n, last = fetch_stream(
                conn, cur, stream, f"/module/data-service/chp/{chp_id}/schedule",
                lambda c, rows, cid=chp_id: upsert_chp(c, cid, rows),
                f"Fahrplan {chp_id}",
            )
            log.info("Fahrplan %d: %5d Werte (bis %s)", chp_id, n,
                     last.isoformat() if last else "—")
        except Exception as e:
            conn.rollback()
            with conn.cursor() as c2:
                set_state(c2, stream, None, str(e)[:200]); conn.commit()
            log.error("Fahrplan %d fehlgeschlagen: %s", chp_id, e)

    cur.close()


def main():
    if not API_KEY:
        log.error("SKVE_API_KEY ist leer – bitte in Portainer setzen. Beende.")
        sys.exit(1)
    log.info("SKVE Collector startet. Basis=%s  Motoren=%s  Preise=%s",
             API_BASE, CHP_IDS, PRICE_SERIES)
    conn = connect_db()
    ensure_schema(conn)

    while True:
        t0 = time.time()
        log.info("=== Lauf beginnt ===")
        try:
            run_once(conn)
        except Exception as e:
            log.exception("Unerwarteter Fehler im Lauf: %s", e)
            # Verbindung ggf. neu aufbauen
            try:
                conn.close()
            except Exception:
                pass
            conn = connect_db()
        log.info("=== Lauf fertig in %.1fs ===", time.time() - t0)

        if RUN_ONCE:
            break
        time.sleep(RUN_INTERVAL_SEC)


if __name__ == "__main__":
    main()
