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
    last_ts = get_state(cur, stream)
    if last_ts is None:
        if INITIAL_START_DATE:
            start = datetime.fromisoformat(INITIAL_START_DATE).replace(tzinfo=timezone.utc)
        else:
            start = now_utc() - timedelta(days=BACKFILL_FALLBACK_DAYS)
    else:
        start = last_ts - timedelta(hours=OVERLAP_HOURS)
    end = now_utc() + timedelta(days=FUTURE_HORIZON_DAYS)
    return start, end


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
# Ein kompletter Durchlauf
# --------------------------------------------------------------------------- #
def run_once(conn):
    cur = conn.cursor()

    # 1) Preise (DAA, IDA)
    for series in PRICE_SERIES:
        stream = f"price:{series}"
        try:
            start, end = window_for(cur, stream)
            payload = api_get(f"/module/data-service/prices/{series}", start, end)
            rows = extract_series(payload)
            n, last = upsert_prices(cur, series, rows)
            set_state(cur, stream, last, "ok")
            conn.commit()
            log.info("Preise %-4s: %4d Werte (bis %s)", series, n,
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
            start, end = window_for(cur, stream)
            payload = api_get(f"/module/data-service/prices/forecast/{market}", start, end)
            rows = extract_series(payload)
            n, last = upsert_prices(cur, series, rows)
            set_state(cur, stream, last, "ok")
            conn.commit()
            log.info("Prognose %-4s: %4d Werte (bis %s)", market, n,
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
            start, end = window_for(cur, stream)
            payload = api_get(f"/module/data-service/chp/{chp_id}/schedule", start, end)
            rows = extract_series(payload)
            n, last = upsert_chp(cur, chp_id, rows)
            set_state(cur, stream, last, "ok")
            conn.commit()
            log.info("Fahrplan %d: %4d Werte (bis %s)", chp_id, n,
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
