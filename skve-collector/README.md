# SKVE Collector – Datensammler & Grafana-Dashboard

Zieht täglich Preise, Fahrpläne und Prognosen von der Speicher-Kraftwerk-API,
speichert sie in TimescaleDB und zeigt sie in Grafana. Läuft als Portainer-Stack
auf dem Raspberry Pi 5 (ARM64). **Nur lesende API-Zugriffe** – der VPP-/POST-
Schreibpfad wird bewusst nicht verwendet.

## Was drin ist

| Container      | Zweck                                              |
|----------------|---------------------------------------------------|
| `timescaledb`  | PostgreSQL + Zeitreihen-Erweiterung (Datenspeicher)|
| `collector`    | Python-Dienst, holt die Daten und schreibt sie    |
| `grafana`      | Dashboard, erreichbar im LAN unter Port 3000      |

## Nahtloser Betrieb

Der Collector merkt sich pro Datenstrom den zuletzt geladenen Zeitstempel
(`ingest_state`). Beim nächsten Lauf holt er alles seit dann – war der Pi
tagelang aus, werden die Tage nachgeholt. Geschrieben wird per `UPSERT`,
Doppelläufe erzeugen also keine Duplikate.

## Einrichtung in Portainer

1. **Dieses Repo** in dein Git legen (GitHub/Gitea/…).
2. In Portainer: **Stacks → Add stack → Repository**.
   - Repository-URL eintragen, Branch `main`.
   - Compose-Pfad: `docker-compose.yml`.
3. **Environment variables** setzen (siehe `.env.example`). Mindestens:
   - `SKVE_API_KEY` – dein API-Schlüssel
   - `DB_PASSWORD` – frei wählbar
   - `GF_ADMIN_PASSWORD` – frei wählbar
4. **Deploy the stack**. Beim ersten Start:
   - legt Timescale das Schema an (`db/init.sql`),
   - baut Portainer das Collector-Image,
   - startet Grafana mit Datenquelle + Dashboard vorkonfiguriert.

## Zugriff

- **Grafana:** `http://<raspi-ip>:3000` – Login mit `admin` / dein `GF_ADMIN_PASSWORD`.
  Das Dashboard „SKVE – BGA Ribbesbüttel" liegt im Ordner *SKVE*.
- **Power BI später anbinden:** in `docker-compose.yml` beim `timescaledb`-Dienst
  die Zeile `ports: ["5432:5432"]` einkommentieren, Stack neu deployen. Dann aus
  dem LAN als PostgreSQL-Quelle verbinden (Host = Raspi-IP, DB/User/Passwort wie gesetzt).

## Erster Lauf / Nachladen

- Standard: Der Collector startet und holt die letzten `BACKFILL_FALLBACK_DAYS`
  Tage, danach täglich das Neue.
- Möchtest du beim ersten Mal weiter zurück laden, setze einmalig
  `SKVE_INITIAL_START_DATE=2026-01-01`, deploye, lass einen Lauf durch, entferne
  die Variable wieder. (Große Zeiträume kann die API begrenzen – im Zweifel in
  Etappen, z. B. monatsweise, das Startdatum schrittweise vorziehen.)
- Test-Lauf ohne 24 h warten: `RUN_ONCE=true` setzen und den `collector`-Container
  neu starten – er läuft einmal durch und beendet sich; im Log siehst du die Zahlen.

## Prüfen, ob Daten ankommen

Im Portainer-Log des `collector` erscheinen Zeilen wie:

```
Preise DAA : 384 Werte (bis 2026-…)
Fahrplan 499:  96 Werte (bis 2026-…)
```

Oder in der DB (Portainer → `skve-db` → Console, oder psql):

```sql
SELECT stream, last_ts, last_status FROM ingest_state;
SELECT count(*) FROM prices;
SELECT count(*) FROM chp_schedule;
```

## API-Key-Ablauf

Der Schlüssel hat ein Ablaufdatum. Läuft er ab, meldet der Collector im Log
`401 – API-Key ungültig oder abgelaufen`. Dann in der Weboberfläche einen neuen
Key erzeugen und in Portainer die Variable `SKVE_API_KEY` aktualisieren +
`collector` neu starten. Sonst ändert sich nichts.

## Datenmodell (Kurz)

- `prices(ts, series, eur_mwh)` – `series` ∈ {DAA, IDA, FORECAST_DAA}
- `chp_schedule(ts, chp_id, kw)` – ein Datensatz je Motor und Zeitpunkt
- `chp_master(chp_id, name, section, nenn_kw)` – Stammdaten der 4 Motoren
- `v_erloes` – View: Spot-Erlös je Viertelstunde/Motor (DAA × Leistung)
- `ingest_state` – Fortschrittsmarker für den nahtlosen Wiederanlauf

## Hinweis

Alle Auswertungen sind Spotmarkt-Rohertrag. Der tatsächliche Erlös (EEG-Vergütung
+ Flex-Anteil) wird hier nicht abgebildet, weil er aus den Netzbetreiber- und
Direktvermarkter-Abrechnungen stammt, nicht aus der API.
