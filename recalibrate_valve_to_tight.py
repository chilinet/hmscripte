#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DNT VALVE_TO_TIGHT: Rekalibrierung
==================================
Sucht DNT-Geräte (Device-Typ oder Profil beginnt mit ``dnt``) nach dem
Fehlercode VALVE_TO_TIGHT.

- Liegt der aktuelle Fehler in den letzten 20 Minuten, geht ein
  Rekalibrier-Downlink an das Gerät: Befehl 52, COMMAND_ID_PERFORM_ADAPTION_RUN,
  Hex-Payload ``34``.
- Jedes neue Auftreten zählt (Wechsel auf VALVE_TO_TIGHT, nicht jede
  Wiederholmeldung). Mindestens 3 solche Auftreten in 7 Tagen werden
  zusätzlich nach ``output/valve_to_tight_wiederholt.log`` geschrieben.
- Ohne ``--customer-id`` gelten alle Customers. Die Option kann mehrfach
  oder kommagetrennt gesetzt werden.

Gedacht für einen stündlichen Cron. Das 20-Minuten-Fenster erwischt einen
Fehler nur, wenn die letzte Meldung jünger als 20 Minuten ist. Für die
volle Stunde seit dem letzten Lauf ``--recent-minutes 70`` verwenden.

  python3 recalibrate_valve_to_tight.py --dry-run
  python3 recalibrate_valve_to_tight.py --customer-id <UUID>
  python3 recalibrate_valve_to_tight.py --customer-id <UUID> --customer-id <UUID>

  5 * * * * cd /pfad/pythonscripts && python3 recalibrate_valve_to_tight.py

.env: PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD, PG_SSL,
      LNS_API_URL, LNS_API_KEY
"""

import argparse
import csv
import fcntl
import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DATABASE = os.getenv("PG_DATABASE", "thingsboard")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_SSL = (os.getenv("PG_SSL") or "").strip().lower() in ("1", "true", "yes", "require")

LNS_API_URL = os.getenv("LNS_API_URL", "http://localhost:3000/api/lns/downlink")
LNS_API_KEY = os.getenv("LNS_API_KEY")

ERROR_CODE = "VALVE_TO_TIGHT"
TELEMETRY_KEY = "error_code"
# DNT-Befehl 52 = COMMAND_ID_PERFORM_ADAPTION_RUN (Adaptionslauf / Rekalibrierung).
RECALIBRATION_HEX = "34"
RECENT_MINUTES = 20
HISTORY_DAYS = 7
MIN_OCCURRENCES = 3

LOG_PATH = os.path.join("output", "valve_to_tight.log")
REPEAT_LOG_PATH = os.path.join("output", "valve_to_tight_wiederholt.log")
STATE_PATH = os.path.join("output", "valve_to_tight_sent.json")
LOCK_PATH = os.path.join("output", "valve_to_tight.lock")

CSV_COLUMNS = [
    "customer_id",
    "customer_name",
    "struktur",
    "device_id",
    "device_name",
    "device_label",
    "device_type",
    "vorkommen_7d",
    "letzter_fehler_utc",
    "aktuell_letzte_minuten",
    "wiederholt",
    "downlink",
    "hex_payload",
]

log = logging.getLogger("valve_to_tight")
repeat_log = logging.getLogger("valve_to_tight.wiederholt")


def setup_logging():
    """Schreibt den Lauf auf die Konsole, ins Hauptlog und Wiederholungen extra."""
    os.makedirs("output", exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
    log.setLevel(logging.INFO)
    log.handlers.clear()
    log.propagate = False

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    main_file = logging.FileHandler(LOG_PATH, encoding="utf-8")
    main_file.setFormatter(fmt)
    log.addHandler(stream)
    log.addHandler(main_file)

    repeat_log.setLevel(logging.WARNING)
    repeat_log.handlers.clear()
    repeat_log.propagate = True
    repeat_file = logging.FileHandler(REPEAT_LOG_PATH, encoding="utf-8")
    repeat_file.setFormatter(fmt)
    repeat_log.addHandler(repeat_file)


def parse_customer_ids(values):
    """Liest eine oder mehrere Customer-UUIDs, auch kommagetrennt."""
    found = []
    seen = set()
    for raw in values or []:
        for part in str(raw).split(","):
            part = part.strip()
            if not part:
                continue
            try:
                customer_id = str(uuid.UUID(part))
            except ValueError as exc:
                raise argparse.ArgumentTypeError(f"Ungültige Customer-ID: {part}") from exc
            if customer_id not in seen:
                seen.add(customer_id)
                found.append(customer_id)
    return found


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Sucht DNT-Geräte mit VALVE_TO_TIGHT, sendet bei einem Fehler der "
            "letzten 20 Minuten eine Rekalibrierung und protokolliert Geräte "
            "mit mindestens 3 Auftreten in 7 Tagen."
        )
    )
    parser.add_argument(
        "--customer-id",
        action="append",
        default=[],
        metavar="UUID",
        help="Nur dieser Customer. Mehrfach oder kommagetrennt. Ohne Angabe: alle Customers.",
    )
    parser.add_argument(
        "--recent-minutes",
        type=int,
        default=RECENT_MINUTES,
        help=f"Rekalibrierung, wenn der aktuelle Fehler jünger ist (Standard: {RECENT_MINUTES})",
    )
    parser.add_argument(
        "--history-days",
        type=int,
        default=HISTORY_DAYS,
        help=f"Zeitraum für die Wiederholungszählung (Standard: {HISTORY_DAYS})",
    )
    parser.add_argument(
        "--min-occurrences",
        type=int,
        default=MIN_OCCURRENCES,
        help=f"Ab dieser Zahl neuer Auftreten gesondert protokollieren (Standard: {MIN_OCCURRENCES})",
    )
    parser.add_argument(
        "--payload",
        default=RECALIBRATION_HEX,
        help=f"Hex-Payload des Downlinks (Standard: {RECALIBRATION_HEX}, Adaptionslauf)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Nur anzeigen, keine Downlinks senden")
    parser.add_argument("--output", help="CSV der Treffer (Standard: output/valve_to_tight_<scope>_<zeit>.csv)")
    parser.add_argument("--pg-host", default=PG_HOST, help=f"PostgreSQL Host (Standard: {PG_HOST})")
    parser.add_argument("--pg-port", default=PG_PORT, help=f"PostgreSQL Port (Standard: {PG_PORT})")
    parser.add_argument("--pg-database", default=PG_DATABASE, help=f"PostgreSQL Datenbank (Standard: {PG_DATABASE})")
    parser.add_argument("--pg-user", default=PG_USER, help="PostgreSQL Benutzer (Standard: aus .env)")
    parser.add_argument("--pg-password", default=PG_PASSWORD, help="PostgreSQL Passwort (Standard: aus .env)")
    parser.add_argument("--lns-api-url", default=LNS_API_URL, help=f"LNS API URL (Standard: {LNS_API_URL})")
    parser.add_argument("--lns-api-key", default=LNS_API_KEY, help="LNS API Key (Standard: aus .env)")
    args = parser.parse_args()
    if args.recent_minutes < 1 or args.history_days < 1 or args.min_occurrences < 1:
        parser.error("--recent-minutes, --history-days und --min-occurrences müssen mindestens 1 sein")
    payload = (args.payload or "").strip().replace(" ", "")
    if len(payload) < 2 or len(payload) % 2 or any(c not in "0123456789abcdefABCDEF" for c in payload):
        parser.error("--payload muss ein geradzahliger Hex-String sein")
    args.payload = payload.upper()
    try:
        args.customer_ids = parse_customer_ids(args.customer_id)
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))
    return args


def connect(host, port, database, user, password):
    import psycopg2

    if not all([host, database, user, password]):
        log.error("PG_HOST, PG_DATABASE, PG_USER und PG_PASSWORD müssen gesetzt sein")
        return None
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
            sslmode="require" if PG_SSL else "prefer",
            connect_timeout=20,
            application_name="recalibrate_valve_to_tight",
        )
    except Exception as exc:
        log.error("PostgreSQL-Verbindung fehlgeschlagen: %s", exc)
        return None
    log.info("PostgreSQL verbunden: %s:%s/%s", host, port, database)
    return conn


def customer_clause(column, customer_ids):
    if not customer_ids:
        return "", {}
    return f" AND {column}::text = ANY(%(customers)s)", {"customers": customer_ids}


def fetch_recent_from_view(conn, recent_minutes, customer_ids):
    """Aktueller VALVE_TO_TIGHT aus der Reporting-View, nur DNT-Geräte."""
    extra_sql, extra_params = customer_clause("v.customer_id", customer_ids)
    query = f"""
        SELECT v.*
        FROM hmreporting.v_device_error_code v
        JOIN device d ON d.id::text = v.device_id::text
        LEFT JOIN device_profile dp ON dp.id = d.device_profile_id
        WHERE v.error_code = %(error_code)s
          AND v.error_code_ts_utc > now() - make_interval(mins => %(recent_minutes)s)
          AND (
                COALESCE(d.type, '') ILIKE 'dnt%%'
                OR COALESCE(dp.name, '') ILIKE 'dnt%%'
              )
          {extra_sql}
    """
    params = {
        "error_code": ERROR_CODE,
        "recent_minutes": recent_minutes,
        **extra_params,
    }
    cursor = conn.cursor()
    cursor.execute(query, params)
    columns = [desc[0] for desc in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    return rows


def resolve_error_key(conn):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT key_id FROM ts_kv_dictionary WHERE key = %s",
        (TELEMETRY_KEY,),
    )
    row = cursor.fetchone()
    if row:
        cursor.close()
        return row[0]
    cursor.execute(
        """
        SELECT key, key_id
        FROM ts_kv_dictionary
        WHERE key ILIKE '%%error%%'
        ORDER BY key
        """
    )
    found = cursor.fetchall()
    cursor.close()
    if found:
        listed = ", ".join(key for key, _key_id in found)
        log.warning(
            "Telemetrie-Key '%s' fehlt. Ähnliche Keys: %s. 7-Tage-Zählung entfällt.",
            TELEMETRY_KEY,
            listed,
        )
    else:
        log.warning("Telemetrie-Key '%s' fehlt. 7-Tage-Zählung entfällt.", TELEMETRY_KEY)
    return None


def error_value_expr(conn):
    """Baut den Vergleich auf str_v und, falls vorhanden, json_v."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'ts_kv'
          AND column_name = 'json_v'
        """
    )
    has_json = cursor.fetchone() is not None
    cursor.close()
    parts = ["COALESCE(t.str_v, '')"]
    if has_json:
        parts.append("COALESCE(t.json_v::text, '')")
    return " OR ".join(f"{part} ILIKE %(pattern)s" for part in parts)


def fetch_occurrences(conn, key_id, history_days, recent_minutes, customer_ids):
    """
    Zählt Wechsel auf VALVE_TO_TIGHT.

    Ein Tag vor dem Fenster liefert den Vorwert, damit ein schon anliegender
    Fehler nicht als neues Auftreten am Fensterrand zählt.
    """
    extra_sql, extra_params = customer_clause("d.customer_id", customer_ids)
    value_expr = error_value_expr(conn)
    query = f"""
        WITH samples AS (
            SELECT
                t.entity_id,
                t.ts,
                ({value_expr}) AS is_tight
            FROM ts_kv t
            JOIN device d ON d.id = t.entity_id
            LEFT JOIN device_profile dp ON dp.id = d.device_profile_id
            WHERE t.key = %(key_id)s
              AND t.ts >= (EXTRACT(EPOCH FROM now() - make_interval(days => %(lag_days)s)) * 1000)::bigint
              AND (
                    COALESCE(d.type, '') ILIKE 'dnt%%'
                    OR COALESCE(dp.name, '') ILIKE 'dnt%%'
                  )
              {extra_sql}
        ),
        marked AS (
            SELECT
                entity_id,
                ts,
                is_tight,
                LAG(is_tight) OVER (PARTITION BY entity_id ORDER BY ts) AS prev_tight
            FROM samples
        ),
        latest AS (
            SELECT DISTINCT ON (entity_id)
                entity_id,
                ts AS latest_ts,
                is_tight AS latest_is_tight
            FROM marked
            ORDER BY entity_id, ts DESC
        ),
        counts AS (
            SELECT entity_id, COUNT(*) AS occurrences, MAX(ts) AS last_tight_ts
            FROM marked
            WHERE is_tight
              AND prev_tight IS DISTINCT FROM TRUE
              AND ts >= (EXTRACT(EPOCH FROM now() - make_interval(days => %(history_days)s)) * 1000)::bigint
            GROUP BY entity_id
        )
        SELECT
            d.id::text AS device_id,
            d.name AS device_name,
            d.label AS device_label,
            COALESCE(NULLIF(d.type, ''), dp.name) AS device_type,
            d.customer_id::text AS customer_id,
            c.title AS customer_name,
            COALESCE(counts.occurrences, 0) AS occurrences,
            counts.last_tight_ts,
            latest.latest_ts,
            latest.latest_is_tight,
            (
                latest.latest_is_tight
                AND latest.latest_ts >= (
                    EXTRACT(EPOCH FROM now() - make_interval(mins => %(recent_minutes)s)) * 1000
                )::bigint
            ) AS recent
        FROM latest
        JOIN device d ON d.id = latest.entity_id
        LEFT JOIN device_profile dp ON dp.id = d.device_profile_id
        LEFT JOIN customer c ON c.id = d.customer_id
        LEFT JOIN counts ON counts.entity_id = latest.entity_id
        WHERE COALESCE(counts.occurrences, 0) > 0
           OR (
                latest.latest_is_tight
                AND latest.latest_ts >= (
                    EXTRACT(EPOCH FROM now() - make_interval(mins => %(recent_minutes)s)) * 1000
                )::bigint
              )
    """
    params = {
        "pattern": f"%{ERROR_CODE}%",
        "key_id": key_id,
        "lag_days": history_days + 1,
        "history_days": history_days,
        "recent_minutes": recent_minutes,
        **extra_params,
    }
    cursor = conn.cursor()
    cursor.execute("SET statement_timeout = '180s'")
    cursor.execute(query, params)
    columns = [desc[0] for desc in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    return rows


def device_asset_map(conn, device_ids):
    if not device_ids:
        return {}
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT DISTINCT ON (r.to_id) r.to_id::text, r.from_id::text
        FROM relation r
        WHERE r.to_id = ANY(%s::uuid[])
          AND r.to_type = 'DEVICE'
          AND r.from_type = 'ASSET'
          AND r.relation_type = 'Contains'
        ORDER BY r.to_id, r.from_id
        """,
        (device_ids,),
    )
    mapping = {device_id: asset_id for device_id, asset_id in cursor.fetchall()}
    cursor.close()
    return mapping


def get_asset_structures(conn, asset_ids):
    unique_ids = []
    for asset_id in asset_ids:
        if not asset_id:
            continue
        asset_id = str(asset_id)
        if asset_id not in unique_ids:
            unique_ids.append(asset_id)
    if not unique_ids:
        return {}

    query = """
    WITH RECURSIVE chain AS (
        SELECT
            a.id,
            a.name,
            a.type,
            a.label,
            a.id AS start_id,
            0 AS depth,
            ARRAY[a.id] AS seen
        FROM asset a
        WHERE a.id = ANY(%s::uuid[])
        UNION ALL
        SELECT
            p.id,
            p.name,
            p.type,
            p.label,
            c.start_id,
            c.depth + 1,
            c.seen || p.id
        FROM chain c
        JOIN LATERAL (
            SELECT r.from_id
            FROM relation r
            WHERE r.to_id = c.id
              AND r.to_type = 'ASSET'
              AND r.from_type = 'ASSET'
              AND r.relation_type = 'Contains'
              AND r.relation_type_group = 'COMMON'
              AND NOT (r.from_id = ANY(c.seen))
            ORDER BY r.from_id
            LIMIT 1
        ) parent ON true
        JOIN asset p ON p.id = parent.from_id
        WHERE c.depth < 12
    )
    SELECT start_id::text, name, type, label
    FROM chain
    ORDER BY start_id, depth DESC
    """
    try:
        cursor = conn.cursor()
        cursor.execute(query, (unique_ids,))
        paths = {}
        for start_id, name, asset_type, label in cursor.fetchall():
            paths.setdefault(start_id, []).append((name, asset_type, label))
        cursor.close()
        return paths
    except Exception as exc:
        log.warning("Strukturpfad konnte nicht geladen werden: %s", exc)
        return {}


def format_structure(nodes):
    if not nodes:
        return ""
    parts = []
    for name, _asset_type, label in nodes:
        text = (label or "").strip() or (name or "").strip()
        if text:
            parts.append(text)
    return " / ".join(parts)


def ts_text(value):
    if value is None:
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return str(value)


def state_key(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value) / 1000, tz=timezone.utc).isoformat()
    return str(value or "")


def load_state():
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, encoding="utf-8") as handle:
            data = json.load(handle)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("Sende-Status konnte nicht gelesen werden: %s", exc)
        return {}


def save_state(state):
    tmp = STATE_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(tmp, STATE_PATH)


def device_id_of(record):
    return str(record.get("device_id") or record.get("deviceId") or record.get("id") or "")


def describe(record):
    return (
        f"device={device_id_of(record)} "
        f"name={record.get('device_name') or ''} "
        f"label={record.get('device_label') or ''} "
        f"typ={record.get('device_type') or ''} "
        f"customer={record.get('customer_name') or ''} ({record.get('customer_id') or ''}) "
        f"struktur={record.get('struktur') or '-'}"
    )


def send_downlink(device_id, hex_payload, api_url, api_key, dry_run):
    payload = {
        "deviceId": device_id,
        "frm_payload": hex_payload,
        "confirmed": False,
        "priority": "NORMAL",
    }
    if dry_run:
        log.info("DRY-RUN Rekalibrierung an %s Payload %s", device_id, json.dumps(payload))
        return True
    try:
        response = requests.post(
            api_url,
            json=payload,
            headers={"Content-Type": "application/json", "X-API-Key": api_key},
            timeout=30,
        )
    except requests.exceptions.RequestException as exc:
        log.error("LNS-Aufruf für %s fehlgeschlagen: %s", device_id, exc)
        return False
    if response.status_code in (200, 201, 202):
        return True
    log.error("LNS-Fehler für %s: HTTP %s %s", device_id, response.status_code, response.text)
    return False


def default_csv_path(customer_ids):
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if not customer_ids:
        scope = "all"
    elif len(customer_ids) == 1:
        scope = customer_ids[0][:8]
    else:
        scope = f"{len(customer_ids)}customers"
    return os.path.join("output", f"valve_to_tight_{scope}_{stamp}.csv")


def write_csv(path, records):
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS, delimiter=";", extrasaction="ignore")
        writer.writeheader()
        for record in records:
            writer.writerow({column: record.get(column, "") for column in CSV_COLUMNS})
    return len(records)


def merge_records(recent_rows, history_rows):
    """Führt View-Treffer und die 7-Tage-Zählung pro Gerät zusammen."""
    merged = {}

    for row in history_rows:
        device_id = device_id_of(row)
        if not device_id:
            continue
        merged[device_id] = {
            "device_id": device_id,
            "device_name": row.get("device_name") or "",
            "device_label": row.get("device_label") or "",
            "device_type": row.get("device_type") or "",
            "customer_id": row.get("customer_id") or "",
            "customer_name": row.get("customer_name") or "",
            "asset_id": "",
            "occurrences": int(row.get("occurrences") or 0),
            "last_ts": row.get("last_tight_ts") or row.get("latest_ts"),
            "recent": bool(row.get("recent")),
            "history_known": True,
        }

    for row in recent_rows:
        device_id = device_id_of(row)
        if not device_id:
            continue
        entry = merged.get(device_id)
        if entry is None:
            entry = {
                "device_id": device_id,
                "occurrences": 0,
                "history_known": False,
                "last_ts": row.get("error_code_ts_utc"),
            }
            merged[device_id] = entry
        entry["recent"] = True
        entry["device_name"] = row.get("device_name") or entry.get("device_name") or ""
        entry["device_label"] = row.get("device_label") or entry.get("device_label") or ""
        entry["device_type"] = row.get("device_type") or row.get("type") or entry.get("device_type") or ""
        entry["customer_id"] = str(row.get("customer_id") or entry.get("customer_id") or "")
        entry["customer_name"] = row.get("customer_name") or entry.get("customer_name") or ""
        entry["asset_id"] = str(row.get("asset_id") or entry.get("asset_id") or "")
        if row.get("error_code_ts_utc") is not None:
            entry["last_ts"] = row.get("error_code_ts_utc")

    for entry in merged.values():
        entry["recent"] = bool(entry.get("recent"))
        entry["occurrences"] = int(entry.get("occurrences") or 0)
    return merged


def attach_structures(conn, records):
    missing = [device_id for device_id, record in records.items() if not record.get("asset_id")]
    assets_by_device = device_asset_map(conn, missing)
    for device_id, asset_id in assets_by_device.items():
        if device_id in records and not records[device_id].get("asset_id"):
            records[device_id]["asset_id"] = asset_id
    structures = get_asset_structures(conn, [record.get("asset_id") for record in records.values()])
    for record in records.values():
        record["struktur"] = format_structure(structures.get(str(record.get("asset_id") or "")))


def run(args):
    os.makedirs("output", exist_ok=True)
    lock_handle = open(LOCK_PATH, "a", encoding="utf-8")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        log.error("Ein anderer Lauf ist noch aktiv. Dieser Start wird beendet.")
        lock_handle.close()
        return 1
    try:
        return execute(args)
    finally:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
        lock_handle.close()


def execute(args):
    if not args.dry_run and not args.lns_api_key:
        log.error("LNS_API_KEY fehlt. In .env setzen oder --lns-api-key angeben.")
        return 1

    scope = "alle Customers" if not args.customer_ids else ", ".join(args.customer_ids)
    log.info(
        "Start VALVE_TO_TIGHT Rekalibrierung | Customer: %s | Fenster: %s Min | "
        "Wiederholung: >= %s in %s Tagen | Payload: %s%s",
        scope,
        args.recent_minutes,
        args.min_occurrences,
        args.history_days,
        args.payload,
        " | DRY-RUN" if args.dry_run else "",
    )

    conn = connect(args.pg_host, args.pg_port, args.pg_database, args.pg_user, args.pg_password)
    if conn is None:
        return 1

    try:
        recent_rows = fetch_recent_from_view(conn, args.recent_minutes, args.customer_ids)
        log.info(
            "%s Gerät(e) mit aktuellem %s in den letzten %s Minuten",
            len(recent_rows),
            ERROR_CODE,
            args.recent_minutes,
        )

        history_rows = []
        try:
            key_id = resolve_error_key(conn)
            if key_id is not None:
                history_rows = fetch_occurrences(
                    conn,
                    key_id,
                    args.history_days,
                    args.recent_minutes,
                    args.customer_ids,
                )
                log.info("Historie: %s Gerät(e) mit %s im Betrachtungszeitraum", len(history_rows), ERROR_CODE)
        except Exception as exc:
            conn.rollback()
            history_rows = []
            log.warning("7-Tage-Zählung fehlgeschlagen, Rekalibrierung läuft weiter: %s", exc)

        records = merge_records(recent_rows, history_rows)
        attach_structures(conn, records)
    except Exception as exc:
        conn.rollback()
        log.error("Abfrage fehlgeschlagen: %s", exc)
        return 1
    finally:
        conn.close()

    interesting = [
        record for record in records.values()
        if record["recent"] or record["occurrences"] >= args.min_occurrences
    ]
    interesting.sort(key=lambda record: (record.get("customer_name") or "", record.get("device_name") or ""))

    if not interesting:
        log.info("Keine aktuellen oder wiederholten %s-Fehler.", ERROR_CODE)
        return 0

    state = load_state()
    sent = 0
    skipped = 0
    failed = 0
    repeated = 0
    csv_rows = []

    for record in interesting:
        device_id = record["device_id"]
        count = record["occurrences"]
        count_text = str(count) if record.get("history_known") else "unbekannt"
        repeated_now = record.get("history_known") and count >= args.min_occurrences
        downlink_state = ""

        if record["recent"]:
            event_key = state_key(record.get("last_ts"))
            previous = state.get(device_id) or {}
            if event_key and previous.get("error_ts") == event_key:
                downlink_state = "bereits gesendet"
                skipped += 1
                log.info(
                    "Rekalibrierung schon für diesen Fehlerzeitpunkt gesendet | %s | zeit=%s | vorkommen_7d=%s",
                    describe(record),
                    ts_text(record.get("last_ts")),
                    count_text,
                )
            elif send_downlink(device_id, args.payload, args.lns_api_url, args.lns_api_key, args.dry_run):
                downlink_state = "dry-run" if args.dry_run else "gesendet"
                sent += 1
                log.info(
                    "Rekalibrierung %s | %s | zeit=%s | vorkommen_7d=%s | payload=%s",
                    downlink_state,
                    describe(record),
                    ts_text(record.get("last_ts")),
                    count_text,
                    args.payload,
                )
                if not args.dry_run and event_key:
                    state[device_id] = {
                        "error_ts": event_key,
                        "sent_at": datetime.now(timezone.utc).isoformat(),
                    }
            else:
                downlink_state = "fehler"
                failed += 1
        else:
            downlink_state = "kein aktueller Fehler"

        if repeated_now:
            repeated += 1
            repeat_log.warning(
                "WIEDERHOLT %s %sx in %s Tagen | downlink=%s | zeit=%s | %s",
                ERROR_CODE,
                count,
                args.history_days,
                downlink_state,
                ts_text(record.get("last_ts")),
                describe(record),
            )

        csv_rows.append({
            "customer_id": record.get("customer_id") or "",
            "customer_name": record.get("customer_name") or "",
            "struktur": record.get("struktur") or "",
            "device_id": device_id,
            "device_name": record.get("device_name") or "",
            "device_label": record.get("device_label") or "",
            "device_type": record.get("device_type") or "",
            "vorkommen_7d": count_text,
            "letzter_fehler_utc": ts_text(record.get("last_ts")),
            "aktuell_letzte_minuten": "ja" if record["recent"] else "nein",
            "wiederholt": "ja" if repeated_now else "nein",
            "downlink": downlink_state,
            "hex_payload": args.payload if record["recent"] else "",
        })

    if not args.dry_run:
        save_state(state)

    csv_path = args.output or default_csv_path(args.customer_ids)
    written = write_csv(csv_path, csv_rows)
    log.info(
        "Ende | aktuell: %s | Rekalibrierung gesendet: %s | übersprungen: %s | "
        "Fehler: %s | wiederholt (>= %s/%s Tage): %s | CSV: %s (%s Zeilen) | "
        "Wiederholungslog: %s",
        sum(1 for record in interesting if record["recent"]),
        sent,
        skipped,
        failed,
        args.min_occurrences,
        args.history_days,
        repeated,
        csv_path,
        written,
        REPEAT_LOG_PATH,
    )
    return 1 if failed else 0


def main():
    setup_logging()
    args = parse_arguments()
    sys.exit(run(args))


if __name__ == "__main__":
    main()
