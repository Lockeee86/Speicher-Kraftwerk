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

# Wochentag + Uhrzeit des Reports (lokale Zeit). Wochentag: Mo=0 … So=6.
REPORT_WEEKDAY = int(os.environ.get("REPORT_WEEKDAY", "0"))  # Montag
REPORT_HOUR = int(os.environ.get("REPORT_HOUR", "8"))
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

# Zustellung – MS Teams (Incoming Webhook / Power Automate "Workflows")
TEAMS_WEBHOOK_URL = os.environ.get("TEAMS_WEBHOOK_URL", "").strip()

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


def _motoren(cur, start, end):
    cur.execute(
        "SELECT m.name, m.section, m.nenn_kw, "
        "       coalesce(sum(s.kw*0.25/1000.0),0) AS mwh, "
        "       coalesce(avg(s.kw),0) AS avg_kw, "
        "       coalesce(max(s.kw),0) AS max_kw, "
        "       coalesce(count(*) FILTER (WHERE s.kw > 0)*0.25, 0) AS betriebsstunden "
        "FROM chp_master m "
        "LEFT JOIN chp_schedule s ON s.chp_id = m.chp_id AND s.ts >= %s AND s.ts < %s "
        "GROUP BY m.name, m.section, m.nenn_kw ORDER BY mwh DESC",
        (start, end))
    out = []
    for name, section, nenn_kw, mwh, avg_kw, max_kw, bh in cur.fetchall():
        out.append({
            "name": name, "standort": section, "nenn_kw": int(nenn_kw),
            "mwh": round(float(mwh), 3), "avg_kw": round(float(avg_kw), 1),
            "max_kw": round(float(max_kw), 1),
            "betriebsstunden": round(float(bh), 2),
            "auslastung_pct": round(float(avg_kw) / nenn_kw * 100, 1) if nenn_kw else None,
        })
    return out


def _erloes(cur, start, end):
    row = _one(cur,
               "SELECT coalesce(sum(erloes_eur),0), coalesce(sum(mwh),0) "
               "FROM v_erloes WHERE ts >= %s AND ts < %s", (start, end))
    return {"erloes_eur": round(float(row[0]), 2), "mwh": round(float(row[1]), 3)}


def _period(cur, label, start, end):
    """Kennzahlen für einen Zeitraum [start, end)."""
    neg = _one(cur, "SELECT count(*) FROM prices WHERE series='DAA' "
                    "AND ts >= %s AND ts < %s AND eur_mwh < 0", (start, end))[0]
    motoren = _motoren(cur, start, end)
    return {
        "zeitraum": label,
        "von": start.strftime("%Y-%m-%d"),
        "bis": (end - timedelta(seconds=1)).strftime("%Y-%m-%d"),
        "preise": {
            "daa": _price_stats(cur, "DAA", start, end),
            "ida": _price_stats(cur, "IDA", start, end),
            "negative_viertelstunden": int(neg or 0),
        },
        "produktion": {
            "motoren": motoren,
            "summe_mwh": round(sum(m["mwh"] for m in motoren), 3),
        },
        "erloes": _erloes(cur, start, end),
    }


def gather_data(conn) -> dict:
    now_local = datetime.now(TZ)
    today0 = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    # Beginn der laufenden (Kalender-)Woche = Montag 00:00.
    this_monday = today0 - timedelta(days=today0.weekday())
    last_week_start = this_monday - timedelta(days=7)   # letzte volle KW
    four_weeks_start = this_monday - timedelta(days=28)  # letzte 4 Wochen

    cur = conn.cursor()

    letzte_woche = _period(cur, "letzte Kalenderwoche", last_week_start, this_monday)
    letzte_woche["kw"] = last_week_start.isocalendar().week
    letzte_4_wochen = _period(cur, "letzte 4 Wochen", four_weeks_start, this_monday)

    # Wochen-Trend: die 4 Wochen einzeln (älteste zuerst) für den Verlauf.
    wochen_trend = []
    for i in range(4):
        w_start = four_weeks_start + timedelta(days=7 * i)
        w_end = w_start + timedelta(days=7)
        er = _erloes(cur, w_start, w_end)
        daa = _price_stats(cur, "DAA", w_start, w_end)
        wochen_trend.append({
            "kw": w_start.isocalendar().week,
            "von": w_start.strftime("%Y-%m-%d"),
            "daa_avg": daa["avg"],
            "erloes_eur": er["erloes_eur"],
            "mwh": er["mwh"],
        })

    # Preis-Ausblick kommende Woche (Prognose FORECAST_DAA, soweit vorhanden).
    komm_start = this_monday
    komm_end = this_monday + timedelta(days=7)
    prog = _price_stats(cur, "FORECAST_DAA", komm_start, komm_end)
    maxfc = _one(cur, "SELECT max(ts) FROM prices WHERE series='FORECAST_DAA'", ())[0]
    if maxfc and maxfc > komm_start:
        abdeckung = round((min(maxfc, komm_end) - komm_start).total_seconds() / 86400, 1)
        reichweite = maxfc.astimezone(TZ).strftime("%Y-%m-%d %H:%M")
    else:
        abdeckung, reichweite = 0.0, None
    prognose_kommende = {
        "daa_forecast": prog,
        "prognose_reicht_bis": reichweite,
        "abgedeckte_tage": abdeckung,
        "hinweis": "Day-Ahead-Prognose reicht meist nur 1-2 Tage voraus.",
    }

    cur.close()

    return {
        "stand": now_local.strftime("%Y-%m-%d %H:%M"),
        "letzte_woche": letzte_woche,
        "letzte_4_wochen": letzte_4_wochen,
        "wochen_trend": wochen_trend,
        "prognose_kommende_woche": prognose_kommende,
        "preisschwelle_eur_mwh": float(PRICE_THRESHOLD) if PRICE_THRESHOLD else None,
    }


# --------------------------------------------------------------------------- #
# Claude
# --------------------------------------------------------------------------- #
SYSTEM_PROMPT = """\
Du bist Energie-Analyst für eine Biogas-BHKW-Anlage (Speicher-Kraftwerk / \
virtuelles Kraftwerk). Du bekommst wöchentliche Kennzahlen als JSON und schreibst \
daraus einen kompakten Wochenrückblick auf Deutsch für den Anlagenbetreiber, \
montagmorgens für die abgeschlossene Kalenderwoche.

Der Report hat zwei Blickwinkel:
1) die letzte Kalenderwoche (Detail),
2) die letzten 4 Wochen als Einordnung/Trend (Feld "wochen_trend" enthält die \
Wochen einzeln, älteste zuerst).

Regeln:
- Kompakt und sachlich, keine Floskeln. Zahlen mit Einheiten (€/MWh, MWh, %).
- Struktur mit kurzen Überschriften und Stichpunkten.
- Letzte Woche: Ø/Min/Max DAA-Preis, erzeugte MWh und Spot-Erlös gesamt, \
Erlös/Produktion je Motor (Betriebsstunden, Auslastung), negative Preisphasen.
- Trend: Wie liegt die letzte Woche im Vergleich zum 4-Wochen-Schnitt und zum \
Verlauf der Einzelwochen (steigt/fällt Preis, Erlös, Produktion – mit % oder \
Richtung)? Nenne beste/schwächste Woche.
- Preis-Ausblick kommende Woche: Nutze "prognose_kommende_woche" (DAA-Prognose). \
Nenne das erwartete Preisniveau und den Trend ggü. der letzten Woche (Richtung/%). \
Sei transparent, wie weit die Prognose reicht ("abgedeckte_tage"/"prognose_reicht_bis") \
- wenn nur 1-2 Tage abgedeckt sind, sag das klar und spekuliere nicht über den Rest.
- Kurzer Ausblick/Empfehlung: Lohnt sich Produktion aktuell bzw. in den nächsten \
Tagen (Preisniveau + Prognose; falls eine Preisschwelle angegeben ist, nutze sie)? \
Auffälligkeiten hervorheben.
- Wenn Daten fehlen (Werte null/0), sag das kurz, statt zu spekulieren.
- Maximal ~300 Wörter. Beginne mit einer Zeile: "SKVE Wochenreport – KW <kw der letzten Woche>".
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


def send_teams(text: str):
    if not TEAMS_WEBHOOK_URL:
        return False
    lines = text.splitlines()
    title = lines[0] if lines else "SKVE Tagesreport"
    body = "\n".join(lines[1:]).strip() or text
    # Adaptive-Card im "type: message"-Format – funktioniert mit dem modernen
    # Power-Automate-"Workflows"-Webhook und den Graph-Incoming-Webhooks.
    card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "type": "AdaptiveCard",
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "version": "1.4",
                "body": [
                    {"type": "TextBlock", "text": title, "weight": "Bolder",
                     "size": "Medium", "wrap": True},
                    {"type": "TextBlock", "text": body, "wrap": True},
                ],
            },
        }],
    }
    r = requests.post(TEAMS_WEBHOOK_URL, json=card, timeout=30)
    r.raise_for_status()
    log.info("Report an MS Teams gesendet.")
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
        sent = send_teams(text) or sent
    except Exception as e:
        log.error("Teams-Versand fehlgeschlagen: %s", e)
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


_WOCHENTAGE = ["Montag", "Dienstag", "Mittwoch", "Donnerstag",
               "Freitag", "Samstag", "Sonntag"]


def seconds_until_next_run() -> float:
    now = datetime.now(TZ)
    target = now.replace(hour=REPORT_HOUR, minute=REPORT_MINUTE, second=0, microsecond=0)
    days_ahead = (REPORT_WEEKDAY - now.weekday()) % 7
    if days_ahead == 0 and target <= now:
        days_ahead = 7
    target += timedelta(days=days_ahead)
    return (target - now).total_seconds()


def main():
    tag = _WOCHENTAGE[REPORT_WEEKDAY % 7]
    log.info("SKVE Reporter startet. Wochenreport %s %02d:%02d (%s), Modell=%s.",
             tag, REPORT_HOUR, REPORT_MINUTE, TZ.key, REPORT_MODEL)
    if RUN_ONCE:
        try:
            run_once()
        except Exception as e:
            log.exception("Report fehlgeschlagen: %s", e)
        # Nicht beenden: sonst würde 'restart: unless-stopped' den Container
        # sofort neu starten und den Report in Dauerschleife feuern. Stattdessen
        # idle bleiben, bis RUN_ONCE wieder auf false steht und neu deployt wird.
        log.info("Testlauf fertig. RUN_ONCE=true → Container idlet (kein weiterer "
                 "Report). Für den Normalbetrieb REPORT_RUN_ONCE=false setzen und "
                 "neu deployen.")
        while True:
            time.sleep(3600)
    while True:
        wait = seconds_until_next_run()
        log.info("Nächster Report in %.1f Tagen.", wait / 86400.0)
        time.sleep(wait)
        try:
            run_once()
        except Exception as e:
            log.exception("Report fehlgeschlagen: %s", e)
        time.sleep(60)  # kleine Pause, damit wir 09:00 nicht doppelt treffen


if __name__ == "__main__":
    main()
