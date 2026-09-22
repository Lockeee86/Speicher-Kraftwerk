#!/usr/bin/env python3
"""
SKVE Reporter
-------------
Erstellt täglich (Standard: 09:00 Uhr) einen KI-geschriebenen Tagesbericht:
- holt die relevanten Kennzahlen aus TimescaleDB,
- lässt Claude daraus einen kompakten Klartext-Report formulieren
  (Preis-Trend, geplante Produktion, Erlös, Auffälligkeiten, Empfehlung),
- stellt ihn per Telegram und/oder E-Mail zu (sonst nur ins Log).

Nur lesender DB-Zugriff.
"""

import os
import sys
import ssl
import time
import json
import smtplib
import logging
from email.mime.text import MIMEText
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import requests
import psycopg2
import anthropic

# --------------------------------------------------------------------------- #
# Konfiguration
# --------------------------------------------------------------------------- #
DB_HOST = os.environ.get("DB_HOST", "timescaledb")
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ.get("DB_NAME", "skve")
DB_USER = os.environ.get("DB_USER", "skve")
DB_PASS = os.environ.get("DB_PASSWORD", "skve")

TZ = ZoneInfo(os.environ.get("TZ", "Europe/Berlin"))

# Uhrzeit des täglichen Reports (lokale Zeit).
REPORT_HOUR = int(os.environ.get("REPORT_HOUR", "9"))
REPORT_MINUTE = int(os.environ.get("REPORT_MINUTE", "0"))
# Einmal laufen und beenden (zum Testen).
RUN_ONCE = os.environ.get("RUN_ONCE", "false").lower() in ("1", "true", "yes")

# Claude
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()
REPORT_MODEL = os.environ.get("REPORT_MODEL", "claude-opus-5")
REPORT_EFFORT = os.environ.get("REPORT_EFFORT", "medium")  # low|medium|high|xhigh|max

# Optionale Preisschwelle (€/MWh), ab der sich Produktion "lohnt".
# Leer = keine feste Schwelle; Claude schätzt anhand des Preisniveaus.
PRICE_THRESHOLD = os.environ.get("REPORT_PRICE_THRESHOLD", "").strip()

# Zustellung – Telegram
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "").strip()

# Zustellung – E-Mail (SMTP)
SMTP_HOST = os.environ.get("SMTP_HOST", "").strip()
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER = os.environ.get("SMTP_USER", "").strip()
SMTP_PASS = os.environ.get("SMTP_PASSWORD", "").strip()
MAIL_FROM = os.environ.get("MAIL_FROM", SMTP_USER).strip()
MAIL_TO = os.environ.get("MAIL_TO", "").strip()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("skve-reporter")


# --------------------------------------------------------------------------- #
# DB
# --------------------------------------------------------------------------- #
def connect_db(retries: int = 10, delay: int = 3):
    last = None
    for i in range(retries):
        try:
            conn = psycopg2.connect(
                host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                user=DB_USER, password=DB_PASS,
            )
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as e:
            last = e
            log.info("DB noch nicht bereit (%d/%d) – warte %ds …", i + 1, retries, delay)
            time.sleep(delay)
    raise SystemExit(f"DB nicht erreichbar: {last}")


def _one(cur, sql, params):
    cur.execute(sql, params)
    return cur.fetchone()


def _price_stats(cur, series, start, end):
    row = _one(cur,
               "SELECT avg(eur_mwh), min(eur_mwh), max(eur_mwh), count(*) "
               "FROM prices WHERE series=%s AND ts >= %s AND ts < %s",
               (series, start, end))
    avg, mn, mx, n = row
    return {
        "avg": round(float(avg), 2) if avg is not None else None,
        "min": round(float(mn), 2) if mn is not None else None,
        "max": round(float(mx), 2) if mx is not None else None,
        "n": int(n or 0),
    }


def _extreme_hour(cur, start, end, order):
    row = _one(cur,
               f"SELECT ts, eur_mwh FROM prices "
               f"WHERE series='DAA' AND ts >= %s AND ts < %s AND eur_mwh IS NOT NULL "
               f"ORDER BY eur_mwh {order} LIMIT 1",
               (start, end))
    if not row:
        return None
    ts, val = row
    return {"zeit": ts.astimezone(TZ).strftime("%H:%M"), "eur_mwh": round(float(val), 2)}


def gather_data(conn) -> dict:
    now_local = datetime.now(TZ)
    today0 = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow0 = today0 + timedelta(days=1)
    yest0 = today0 - timedelta(days=1)
    day_after0 = tomorrow0 + timedelta(days=1)

    cur = conn.cursor()

    # Preise
    daa_today = _price_stats(cur, "DAA", today0, tomorrow0)
    daa_yest = _price_stats(cur, "DAA", yest0, today0)
    ida_today = _price_stats(cur, "IDA", today0, tomorrow0)
    forecast_tomorrow = _price_stats(cur, "FORECAST_DAA", tomorrow0, day_after0)

    teuerste = _extreme_hour(cur, today0, tomorrow0, "DESC")
    guenstigste = _extreme_hour(cur, today0, tomorrow0, "ASC")

    # negative / sehr niedrige Preisviertelstunden heute
    neg = _one(cur,
               "SELECT count(*) FROM prices WHERE series='DAA' "
               "AND ts >= %s AND ts < %s AND eur_mwh < 0", (today0, tomorrow0))[0]

    # Produktion je Motor heute (Fahrplan)
    cur.execute(
        "SELECT m.name, m.section, m.nenn_kw, "
        "       coalesce(sum(s.kw*0.25/1000.0),0) AS mwh, "
        "       coalesce(avg(s.kw),0) AS avg_kw, "
        "       coalesce(max(s.kw),0) AS max_kw, "
        "       coalesce(count(*) FILTER (WHERE s.kw > 0)*0.25, 0) AS betriebsstunden "
        "FROM chp_master m "
        "LEFT JOIN chp_schedule s ON s.chp_id = m.chp_id AND s.ts >= %s AND s.ts < %s "
        "GROUP BY m.name, m.section, m.nenn_kw ORDER BY mwh DESC",
        (today0, tomorrow0))
    motoren = []
    for name, section, nenn_kw, mwh, avg_kw, max_kw, bh in cur.fetchall():
        motoren.append({
            "name": name, "standort": section, "nenn_kw": int(nenn_kw),
            "mwh": round(float(mwh), 3), "avg_kw": round(float(avg_kw), 1),
            "max_kw": round(float(max_kw), 1),
            "betriebsstunden": round(float(bh), 2),
            "auslastung_pct": round(float(avg_kw) / nenn_kw * 100, 1) if nenn_kw else None,
        })

    # Erlös heute / gestern (Spot, DAA)
    def erloes(start, end):
        row = _one(cur,
                   "SELECT coalesce(sum(erloes_eur),0), coalesce(sum(mwh),0) "
                   "FROM v_erloes WHERE ts >= %s AND ts < %s", (start, end))
        return {"erloes_eur": round(float(row[0]), 2), "mwh": round(float(row[1]), 3)}

    cur.close()

    return {
        "stand": now_local.strftime("%Y-%m-%d %H:%M"),
        "datum_heute": today0.strftime("%Y-%m-%d"),
        "preise": {
            "daa_heute": daa_today,
            "daa_gestern": daa_yest,
            "ida_heute": ida_today,
            "prognose_morgen_daa": forecast_tomorrow,
            "teuerste_stunde_heute": teuerste,
            "guenstigste_stunde_heute": guenstigste,
            "negative_viertelstunden_heute": int(neg or 0),
        },
        "produktion_heute": {
            "motoren": motoren,
            "summe_mwh": round(sum(m["mwh"] for m in motoren), 3),
        },
        "erloes": {
            "heute": erloes(today0, tomorrow0),
            "gestern": erloes(yest0, today0),
        },
        "preisschwelle_eur_mwh": float(PRICE_THRESHOLD) if PRICE_THRESHOLD else None,
    }


# --------------------------------------------------------------------------- #
# Claude
# --------------------------------------------------------------------------- #
SYSTEM_PROMPT = """\
Du bist Energie-Analyst für eine Biogas-BHKW-Anlage (Speicher-Kraftwerk / \
virtuelles Kraftwerk). Du bekommst tägliche Kennzahlen als JSON und schreibst \
daraus einen kurzen, konkreten Tagesbericht auf Deutsch für den Anlagenbetreiber.

Regeln:
- Kompakt und sachlich, keine Floskeln. Zahlen mit Einheiten (€/MWh, MWh, kW, %).
- Struktur mit kurzen Abschnitten/Überschriften und Stichpunkten.
- Nenne den Preis-Trend heute vs. gestern (in % und Richtung) und die \
teuerste/günstigste Stunde.
- Beurteile die geplante Produktion je Motor (MWh, Betriebsstunden, Auslastung) \
und ob sie in den teuren Stunden liegt.
- Gib eine klare Empfehlung: Lohnt sich Produktion heute, und grob wie viel? \
Wenn eine Preisschwelle angegeben ist, nutze sie; sonst beurteile anhand des \
Preisniveaus. Weise auf negative/sehr niedrige Preise hin (dann nicht einspeisen).
- Wenn Daten fehlen (Werte null/0), sag das kurz, statt zu spekulieren.
- Maximal ~250 Wörter. Beginne mit einer Zeile: "SKVE Tagesreport <Datum>".
"""


def build_report(data: dict) -> str:
    if not ANTHROPIC_API_KEY:
        raise RuntimeError("ANTHROPIC_API_KEY ist leer – bitte in Portainer setzen.")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    user_msg = (
        "Hier sind die heutigen Kennzahlen als JSON. Schreibe den Tagesreport.\n\n"
        + json.dumps(data, ensure_ascii=False, indent=2)
    )
    base = dict(
        model=REPORT_MODEL,
        max_tokens=4000,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    # Adaptive Thinking + Effort nutzen, aber robust bleiben, falls die
    # installierte SDK-/Modellversion diese Parameter (noch) nicht kennt.
    try:
        resp = client.messages.create(
            thinking={"type": "adaptive"},
            output_config={"effort": REPORT_EFFORT},
            **base,
        )
    except (TypeError, anthropic.BadRequestError) as e:
        log.warning("Erweiterte Parameter nicht unterstützt (%s) – Standardaufruf.", e)
        resp = client.messages.create(**base)
    text = "".join(b.text for b in resp.content if b.type == "text").strip()
    if not text:
        raise RuntimeError(f"Leere Antwort von Claude (stop_reason={resp.stop_reason}).")
    return text


# --------------------------------------------------------------------------- #
# Zustellung
# --------------------------------------------------------------------------- #
def send_telegram(text: str):
    if not (TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID):
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    # Telegram-Limit: 4096 Zeichen pro Nachricht -> ggf. stückeln.
    for i in range(0, len(text), 3900):
        chunk = text[i:i + 3900]
        r = requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": chunk}, timeout=30)
        r.raise_for_status()
    log.info("Report per Telegram gesendet.")
    return True


def send_email(text: str):
    if not (SMTP_HOST and MAIL_TO and MAIL_FROM):
        return False
    subject = text.splitlines()[0] if text else "SKVE Tagesreport"
    msg = MIMEText(text, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = MAIL_FROM
    msg["To"] = MAIL_TO
    ctx = ssl.create_default_context()
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as s:
        s.ehlo()
        try:
            s.starttls(context=ctx)
            s.ehlo()
        except smtplib.SMTPException:
            pass  # Server ohne STARTTLS (z.B. reines Intranet-Relay)
        if SMTP_USER:
            s.login(SMTP_USER, SMTP_PASS)
        s.sendmail(MAIL_FROM, [a.strip() for a in MAIL_TO.split(",")], msg.as_string())
    log.info("Report per E-Mail an %s gesendet.", MAIL_TO)
    return True


def deliver(text: str):
    sent = False
    try:
        sent = send_telegram(text) or sent
    except Exception as e:
        log.error("Telegram-Versand fehlgeschlagen: %s", e)
    try:
        sent = send_email(text) or sent
    except Exception as e:
        log.error("E-Mail-Versand fehlgeschlagen: %s", e)
    if not sent:
        log.warning("Kein Zustellkanal konfiguriert – Report nur im Log:")
    log.info("\n===== TAGESREPORT =====\n%s\n=======================", text)


# --------------------------------------------------------------------------- #
# Ein Durchlauf + Zeitplan
# --------------------------------------------------------------------------- #
def run_once():
    conn = connect_db()
    try:
        data = gather_data(conn)
        log.info("Kennzahlen erhoben (Stand %s).", data["stand"])
        report = build_report(data)
        deliver(report)
    finally:
        conn.close()


def seconds_until_next_run() -> float:
    now = datetime.now(TZ)
    target = now.replace(hour=REPORT_HOUR, minute=REPORT_MINUTE, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def main():
    log.info("SKVE Reporter startet. Report täglich um %02d:%02d (%s), Modell=%s.",
             REPORT_HOUR, REPORT_MINUTE, TZ.key, REPORT_MODEL)
    if RUN_ONCE:
        try:
            run_once()
        except Exception as e:
            log.exception("Report fehlgeschlagen: %s", e)
        return
    while True:
        wait = seconds_until_next_run()
        log.info("Nächster Report in %.1f h.", wait / 3600.0)
        time.sleep(wait)
        try:
            run_once()
        except Exception as e:
            log.exception("Report fehlgeschlagen: %s", e)
        time.sleep(60)  # kleine Pause, damit wir 09:00 nicht doppelt treffen


if __name__ == "__main__":
    main()
