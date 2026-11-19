#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Check Occupied (REST)
====================
Gleiche Daten wie check_occupied (PIR/ws202), aber über ThingsBoard Web-API.

- Customer-Assets: GET /api/customer/{customer_id}/assets
- Pro Asset: Attribut hasPir = true prüfen (Asset-Attribute)
- Related Devices zum Asset holen, Profile ws202 und ws203
- ws202: letzter Telemetriewert „pir“ (Regel 1–3)
- ws203: Telemetrie „occupancy“ (occupied/vacant). 30 Min occupied am Stück
  markiert das Asset für 2 Stunden als belegt (Regel 4), danach wieder frei (Regel 5)
- Asset-Attribut „occupied“ ausgeben
- setOccupied: "true" wenn PIR (z.B. trigger) und ts älter als 5 Min, sonst leer
- Optional: --set-occupied-when-pir-active → occupied per API auf true setzen (Regel 1/2/4)
- Wechselt occupied (true oder false): derselbe Wert zusätzlich als Asset-Telemetrie „occupied“

Verwendung:
  python check_occupied_rest.py --customer-id <UUID>   # TB-Login aus Profil tbconnections (Standard)
  python check_occupied_rest.py --customer-id <UUID> --no-credentials-from-db   # TB aus .env/getpass
  python check_occupied_rest.py --customer-id <UUID> --output csv -o occupied_rest.csv
  python check_occupied_rest.py --customer-id <UUID> --set-occupied-when-pir-active

.env: THINGBOARD_URL, THINGBOARD_USERNAME, THINGBOARD_PASSWORD
  Standard: ThingsBoard-Login aus MSSQL-Profil tbconnections
  (customer_settings.tbconnection_id → tb_username, tb_password, tb_url).
  Mit --no-credentials-from-db: Anmeldung aus .env/CLI.
  MSSQL: MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USER, MSSQL_PASSWORD
"""

import os
import sys
import argparse
import getpass
import json
import csv
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests


def _log_line(msg: str) -> None:
    """Schreibt eine Zeile mit Timestamp auf stderr (Logausgabe)."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", file=sys.stderr)
from dotenv import load_dotenv

load_dotenv()

THINGBOARD_URL = (os.getenv("THINGBOARD_URL") or os.getenv("TB_BASE_URL") or "https://webapp02.heatmanager.cloud").strip()
THINGBOARD_USERNAME = (os.getenv("THINGBOARD_USERNAME") or os.getenv("TB_USERNAME") or "").strip()
THINGBOARD_PASSWORD = os.getenv("THINGBOARD_PASSWORD") or os.getenv("TB_PASSWORD") or ""
TB_VERIFY_SSL = os.getenv("TB_VERIFY_SSL", "true").strip().lower() not in ("0", "false", "no")
MSSQL_SERVER = os.getenv("MSSQL_SERVER", "")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE", "")
MSSQL_USER = os.getenv("MSSQL_USER", "")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD", "")
REQUEST_TIMEOUT = 30
DEVICE_PROFILE_WS202 = "ws202"
DEVICE_PROFILE_WS203 = "ws203"
ASSET_ATTR_HAS_PIR = "hasPir"
ASSET_ATTR_OCCUPIED = "occupied"
ASSET_ATTR_OCCUPIED_UNTIL = "occupiedUntil"
ASSET_ATTR_SCOPE = "SERVER_SCOPE"
TELEMETRY_KEY_PIR = "pir"
TELEMETRY_KEY_OCCUPANCY = "occupancy"
PIR_ACTIVE_MINUTES_DEFAULT = 5
# Regel 2: setOccupied = true wenn in den letzten RULE2_WINDOW_MINUTES Min die PIR in Summe RULE2_TRIGGER_MINUTES Min auf trigger stand
RULE2_WINDOW_MINUTES = 10
RULE2_TRIGGER_MINUTES = 5
# Regel 3: setOccupied = false wenn occupied = true und PIR seit mehr als 60 Min auf "normal" steht
RULE3_WINDOW_MINUTES = 60
# Regel 4: ws203 occupancy durchgehend occupied → Asset 2 h belegt
# Regel 5: Haltezeit abgelaufen und kein 30-Minuten-Stück mehr → Asset frei
WS203_STREAK_MINUTES = 30
WS203_HOLD_MINUTES = 120
WS203_HISTORY_MINUTES = WS203_HOLD_MINUTES + WS203_STREAK_MINUTES


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Check Occupied über ThingsBoard REST: Assets mit hasPir, ws202 (pir) und ws203 (occupancy)."
    )
    parser.add_argument(
        "--customer-id",
        required=True,
        metavar="UUID",
        help="ThingsBoard Customer-UUID",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="Seitengröße für Assets (Standard: 1000)",
    )
    parser.add_argument(
        "--output",
        choices=["text", "csv", "json"],
        default="text",
        help="Ausgabeformat (Standard: text)",
    )
    parser.add_argument(
        "-o",
        "--output-file",
        default="",
        help="Ausgabedatei. Ohne Angabe: Stdout",
    )
    parser.add_argument(
        "--thingboard-url",
        default=THINGBOARD_URL,
        help="ThingsBoard Base-URL",
    )
    parser.add_argument(
        "--thingboard-username",
        default=THINGBOARD_USERNAME,
        help="ThingsBoard Benutzer",
    )
    parser.add_argument(
        "--thingboard-password",
        default=THINGBOARD_PASSWORD,
        help="ThingsBoard Passwort",
    )
    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help="SSL nicht prüfen",
    )
    parser.add_argument(
        "--no-credentials-from-db",
        action="store_true",
        dest="no_credentials_from_db",
        help="ThingsBoard-Anmeldung aus .env/CLI statt aus dem Profil tbconnections",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Debug-Ausgaben (z.B. MSSQL-Verbindung, TB-Anmeldedaten aus DB)",
    )
    parser.add_argument(
        "--mssql-server",
        default=MSSQL_SERVER,
        help="MSSQL Server",
    )
    parser.add_argument(
        "--mssql-database",
        default=MSSQL_DATABASE,
        help="MSSQL Datenbank",
    )
    parser.add_argument(
        "--mssql-user",
        default=MSSQL_USER,
        help="MSSQL Benutzer",
    )
    parser.add_argument(
        "--mssql-password",
        default=MSSQL_PASSWORD,
        help="MSSQL Passwort",
    )
    parser.add_argument(
        "--set-occupied-when-pir-active",
        action="store_true",
        dest="set_occupied_when_pir_active",
        help="Regel 1/2 (PIR) und Regel 4 (ws203, 30 Min occupied): Asset-Attribut occupied auf true setzen",
    )
    parser.add_argument(
        "--pir-minutes",
        type=int,
        default=PIR_ACTIVE_MINUTES_DEFAULT,
        metavar="MINUTES",
        help=f"Mindestdauer PIR aktiv (Minuten) für setOccupied/occupied (Standard: {PIR_ACTIVE_MINUTES_DEFAULT})",
    )
    return parser.parse_args()


def normalize_base_url(url: str) -> str:
    u = (url or "").strip()
    return u.rstrip("/") if u else ""


def get_tb_credentials_from_mssql(
    customer_id: str,
    server: str,
    database: str,
    user: str,
    password: str,
    debug: bool = False,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Holt tb_username, tb_password und tb_url aus dem Profil tbconnections des Customers."""
    def _log(msg: str) -> None:
        if debug:
            _log_line(f"   [DEBUG] TB aus MSSQL: {msg}")

    def _fail(reason: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        _log_line(f"   Ursache: {reason}")
        return None, None, None

    try:
        import pyodbc
    except ImportError as e:
        _log(f"pyodbc nicht installiert: {e}")
        return _fail(f"pyodbc nicht installiert: {e}")

    missing = [k for k, v in [("server", server), ("database", database), ("user", user), ("password", password)] if not (v or "").strip()]
    if missing:
        _log(f"Fehlende Verbindungsdaten: {', '.join(missing)}")
        return _fail(f"Fehlende Verbindungsdaten: {', '.join(missing)} (MSSQL_SERVER, MSSQL_DATABASE, MSSQL_USER, MSSQL_PASSWORD in .env?)")

    drivers = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "FreeTDS",
    ]
    conn = None
    last_err: Optional[Exception] = None
    for driver in drivers:
        try:
            conn_str = (
                f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
                f"UID={user};PWD={password};Encrypt=yes;TrustServerCertificate=no;"
            )
            conn = pyodbc.connect(conn_str)
            if debug:
                _log(f"Verbindung mit Treiber '{driver}' hergestellt.")
            break
        except Exception as e:
            last_err = e
            _log(f"Treiber '{driver}' (Encrypt=yes): {e}")
            try:
                conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={user};PWD={password};"
                conn = pyodbc.connect(conn_str)
                if debug:
                    _log(f"Verbindung mit Treiber '{driver}' (ohne Encrypt) hergestellt.")
                break
            except Exception as e2:
                last_err = e2
                _log(f"Treiber '{driver}' (ohne Encrypt): {e2}")
                continue
    if not conn:
        _log(f"Kein Treiber konnte verbinden. Letzter Fehler: {last_err}")
        return _fail(f"MSSQL-Verbindung fehlgeschlagen. Letzter Fehler: {last_err}")

    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT cs.tbconnection_id, tc.tb_username, tc.tb_password, tc.tb_url
            FROM customer_settings cs
            LEFT JOIN tbconnections tc ON cs.tbconnection_id = tc.id
            WHERE cs.customer_id = ?
            """,
            (customer_id,),
        )
        row = cursor.fetchone()
        cursor.close()
        if not row:
            _log(f"Kein Eintrag in customer_settings für customer_id={customer_id}.")
            return _fail(f"Kein Eintrag in customer_settings für customer_id={customer_id}.")
        connection_id, raw_user, raw_password, raw_url = row[0], row[1], row[2], row[3]
        if connection_id is None:
            _log(f"customer_id={customer_id} hat kein tbconnection_id.")
            return _fail("customer_settings.tbconnection_id ist leer (kein Profil tbconnections).")
        uname = str(raw_user).strip() if raw_user else None
        pwd = str(raw_password) if raw_password else None
        tb_url = str(raw_url).strip() if raw_url else None
        if not uname or not pwd:
            _log(f"Profil tbconnections id={connection_id} hat leeres tb_username oder tb_password.")
            return _fail(f"Profil tbconnections id={connection_id} hat leeres tb_username oder tb_password.")
        return (uname, pwd, tb_url or None)
    except Exception as e:
        _log(f"Abfrage fehlgeschlagen: {e}")
        return _fail(f"Abfrage fehlgeschlagen: {e}")
    finally:
        if conn:
            conn.close()


def tb_login(session: requests.Session, base_url: str, username: str, password: str) -> str:
    url = f"{base_url}/api/auth/login"
    r = session.post(url, json={"username": username, "password": password}, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Login fehlgeschlagen: HTTP {r.status_code} - {r.text}")
    token = r.json().get("token")
    if not token:
        raise RuntimeError("Login-Antwort enthält kein Token")
    return token


def get_customer_assets(
    session: requests.Session,
    base_url: str,
    customer_id: str,
    page_size: int,
) -> List[Dict[str, Any]]:
    url = f"{base_url}/api/customer/{customer_id}/assets"
    all_assets: List[Dict[str, Any]] = []
    page = 0
    while True:
        params = {"pageSize": page_size, "page": page}
        r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"Customer-Assets: HTTP {r.status_code} - {r.text}")
        data = r.json()
        assets = data.get("data") or []
        all_assets.extend(assets)
        if not data.get("hasNext", False):
            break
        page += 1
    return all_assets


def get_asset_attributes(
    session: requests.Session,
    base_url: str,
    asset_id: str,
    keys: Optional[str] = None,
) -> Dict[str, Any]:
    url = f"{base_url}/api/plugins/telemetry/ASSET/{asset_id}/values/attributes"
    params = {}
    if keys:
        params["keys"] = keys
    r = session.get(url, params=params or None, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        return {}
    raw = r.json()
    out: Dict[str, Any] = {}
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict) and "key" in item and "value" in item:
                out[item["key"]] = item["value"]
    elif isinstance(raw, dict):
        out = raw
    return out


def is_has_pir_true(value: Any) -> bool:
    if value is True:
        return True
    if isinstance(value, str) and value.strip().lower() == "true":
        return True
    return False


def is_pir_active(value: Any) -> bool:
    """True wenn PIR als aktiv gilt (z.B. trigger, true, 1)."""
    if value is True:
        return True
    if value is False or value is None:
        return False
    if isinstance(value, (int, float)) and value != 0:
        return True
    if isinstance(value, str):
        v = value.strip().lower()
        if v in ("true", "1", "on", "yes", "trigger"):
            return True
    return False


def is_occupancy_occupied(value: Any) -> bool:
    """True wenn der ws203-Wert „occupied“ ist (Raum belegt). „vacant“ ist frei."""
    if value is None:
        return False
    return str(value).strip().lower() == "occupied"


def occupancy_streak_ms(entries: List[Dict[str, Any]], now_ms: int) -> int:
    """
    Dauer der aktuellen occupied-Phase bis now_ms.
    Die Phase beginnt beim ersten „occupied“ nach dem letzten „vacant“
    und gilt nur, wenn der neueste Wert „occupied“ ist.
    """
    if not entries:
        return 0
    sorted_entries = sorted(entries, key=lambda x: x.get("ts") or 0)
    if not is_occupancy_occupied(sorted_entries[-1].get("value")):
        return 0
    streak_start = None
    for ev in reversed(sorted_entries):
        if not is_occupancy_occupied(ev.get("value")):
            break
        ts = ev.get("ts")
        if ts is not None:
            streak_start = int(ts)
    if streak_start is None:
        return 0
    return max(0, now_ms - streak_start)


def as_epoch_ms(value: Any) -> Optional[int]:
    """Liest einen Zeitstempel in ms aus einem Attributwert."""
    if value is None or value is False or value is True:
        return None
    if isinstance(value, (int, float)):
        ms = int(value)
        return ms if ms > 0 else None
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            ms = int(float(s))
        except ValueError:
            return None
        return ms if ms > 0 else None
    return None


def set_asset_attribute(
    session: requests.Session,
    base_url: str,
    asset_id: str,
    key: str,
    value: Any,
    scope: str = ASSET_ATTR_SCOPE,
) -> bool:
    """Setzt ein Asset-Attribut. Returns True bei Erfolg."""
    url = f"{base_url}/api/plugins/telemetry/ASSET/{asset_id}/attributes/{scope}"
    payload = {key: value}
    try:
        r = session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        return r.status_code == 200
    except Exception:
        return False


def set_asset_attributes(
    session: requests.Session,
    base_url: str,
    asset_id: str,
    values: Dict[str, Any],
    scope: str = ASSET_ATTR_SCOPE,
) -> bool:
    """Setzt mehrere Asset-Attribute in einem Aufruf. Returns True bei Erfolg."""
    url = f"{base_url}/api/plugins/telemetry/ASSET/{asset_id}/attributes/{scope}"
    try:
        r = session.post(url, json=values, timeout=REQUEST_TIMEOUT)
        return r.status_code == 200
    except Exception:
        return False


def save_asset_telemetry(
    session: requests.Session,
    base_url: str,
    asset_id: str,
    values: Dict[str, Any],
    ts_ms: Optional[int] = None,
) -> bool:
    """Schreibt Telemetriewerte auf ein Asset. Returns True bei Erfolg."""
    url = f"{base_url}/api/plugins/telemetry/ASSET/{asset_id}/timeseries/ANY"
    payload = {"ts": ts_ms if ts_ms is not None else int(time.time() * 1000), "values": values}
    try:
        r = session.post(url, json=payload, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            _log_line(f"   Telemetrie HTTP {r.status_code}: {r.text[:300]}")
            return False
        return True
    except Exception as e:
        _log_line(f"   Telemetrie fehlgeschlagen: {e}")
        return False


def get_relations_from_asset(
    session: requests.Session,
    base_url: str,
    asset_id: str,
    to_type: str = "DEVICE",
) -> List[Dict[str, Any]]:
    url = f"{base_url}/api/relations"
    params = {"fromId": asset_id, "fromType": "ASSET", "toType": to_type}
    r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        return []
    result = r.json()
    return result if isinstance(result, list) else []


def get_device(session: requests.Session, base_url: str, device_id: str) -> Optional[Dict[str, Any]]:
    url = f"{base_url}/api/device/{device_id}"
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        return None
    return r.json()


def get_device_profile_map(session: requests.Session, base_url: str, page_size: int = 200) -> Dict[str, str]:
    url = f"{base_url}/api/deviceProfiles"
    result: Dict[str, str] = {}
    page = 0
    while True:
        params = {"pageSize": page_size, "page": page}
        r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            break
        data = r.json()
        for p in data.get("data") or []:
            pid = p.get("id")
            if isinstance(pid, dict):
                pid = pid.get("id")
            if pid:
                result[str(pid)] = (p.get("name") or "").strip()
        if not data.get("hasNext", False):
            break
        page += 1
    return result


def get_latest_telemetry(
    session: requests.Session,
    base_url: str,
    device_id: str,
    key: str,
) -> Tuple[Optional[Any], Optional[int]]:
    """Liefert (letzter Wert, ts_ms) oder (None, None)."""
    url = f"{base_url}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"
    params = {"keys": key}
    r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        return None, None
    data = r.json()
    entries = data.get(key) if isinstance(data, dict) else []
    if not entries or not isinstance(entries, list):
        return None, None
    best = max(entries, key=lambda x: x.get("ts", 0))
    return best.get("value"), best.get("ts")


def get_telemetry_timeseries(
    session: requests.Session,
    base_url: str,
    device_id: str,
    key: str,
    start_ts_ms: int,
    end_ts_ms: int,
) -> List[Dict[str, Any]]:
    """Liefert PIR-Telemetrie im Zeitfenster [start_ts_ms, end_ts_ms] als Liste von {ts, value}."""
    url = f"{base_url}/api/plugins/telemetry/DEVICE/{device_id}/values/timeseries"
    params = {"keys": key, "startTs": start_ts_ms, "endTs": end_ts_ms, "limit": 1000, "orderBy": "ASC"}
    r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        return []
    data = r.json()
    entries = data.get(key) if isinstance(data, dict) else []
    if not entries or not isinstance(entries, list):
        return []
    return entries


def trigger_duration_ms(entries: List[Dict[str, Any]], end_ts_ms: int) -> int:
    """
    Summiert die Dauer (in ms), die der PIR im Zustand "trigger" war.
    entries: Liste von {"ts": ms, "value": "trigger"|"normal"|...}, sortiert nach ts aufsteigend.
    end_ts_ms: Ende des Fensters (jetzt). Ein letzter "trigger" dauert bis end_ts_ms.
    """
    if not entries:
        return 0
    sorted_entries = sorted(entries, key=lambda x: x.get("ts", 0))
    total_ms = 0
    for i, ev in enumerate(sorted_entries):
        val = ev.get("value")
        if not is_pir_active(val):
            continue
        ts = ev.get("ts")
        if ts is None:
            continue
        # Ende dieser Trigger-Phase: nächstes Event oder end_ts_ms
        if i + 1 < len(sorted_entries):
            segment_end = min(sorted_entries[i + 1].get("ts", ts), end_ts_ms)
        else:
            segment_end = end_ts_ms
        segment_end = min(segment_end, end_ts_ms)
        if segment_end > ts:
            total_ms += segment_end - ts
    return total_ms


def asset_uid(asset: Dict[str, Any]) -> str:
    aid = asset.get("id")
    if isinstance(aid, dict):
        return (aid.get("id") or "").strip()
    return (aid or "").strip()


def device_uid(device: Dict[str, Any]) -> str:
    did = device.get("id")
    if isinstance(did, dict):
        return (did.get("id") or "").strip()
    return (did or "").strip()


def main():
    _log_line("=" * 80)
    _log_line("CHECK OCCUPIED (REST)")
    _log_line("=" * 80)

    args = parse_arguments()
    customer_id = (args.customer_id or "").strip()
    if not customer_id:
        _log_line("❌ --customer-id fehlt.")
        sys.exit(1)

    base_url = normalize_base_url(args.thingboard_url)

    credentials_from_db = not getattr(args, "no_credentials_from_db", False)
    if credentials_from_db:
        _log_line(f"📋 Hole ThingsBoard-Anmeldedaten aus MSSQL (tbconnections, customer_id={customer_id})...")
        username, password, profile_url = get_tb_credentials_from_mssql(
            customer_id,
            args.mssql_server,
            args.mssql_database,
            args.mssql_user,
            args.mssql_password,
            debug=getattr(args, "debug", False),
        )

        if not username or not password:
            _log_line("❌ ThingsBoard-Anmeldung aus Datenbank fehlgeschlagen.")
            sys.exit(1)
        if profile_url:
            base_url = normalize_base_url(profile_url)
        _log_line("   ✅ Anmeldedaten aus Profil tbconnections geladen")
    else:
        username = (args.thingboard_username or "").strip()
        if not username:
            _log_line("❌ THINGBOARD_USERNAME fehlt (oder --thingboard-username).")
            sys.exit(1)
        password = args.thingboard_password
        if not password:
            password = getpass.getpass("ThingsBoard Passwort: ")

    if not base_url:
        _log_line("❌ ThingsBoard-URL fehlt (tbconnections.tb_url oder THINGBOARD_URL).")
        sys.exit(1)

    verify_ssl = TB_VERIFY_SSL and not args.no_verify_ssl
    session = requests.Session()
    session.verify = verify_ssl

    try:
        token = tb_login(session, base_url, username, password)
        session.headers["X-Authorization"] = f"Bearer {token}"
    except Exception as e:
        _log_line(f"❌ Login: {e}")
        sys.exit(1)

    _log_line(f"📋 Lade Assets für Customer {customer_id}...")
    try:
        assets = get_customer_assets(session, base_url, customer_id, args.page_size)
    except Exception as e:
        _log_line(f"❌ {e}")
        sys.exit(1)
    _log_line(f"   {len(assets)} Assets gefunden")

    _log_line("📋 Lade Device-Profile...")
    profile_map = get_device_profile_map(session, base_url, args.page_size)

    rows: List[Dict[str, Any]] = []
    for asset in assets:
        asset_id = asset_uid(asset)
        if not asset_id:
            continue
        attrs = get_asset_attributes(
            session,
            base_url,
            asset_id,
            keys=f"{ASSET_ATTR_HAS_PIR},{ASSET_ATTR_OCCUPIED},{ASSET_ATTR_OCCUPIED_UNTIL}",
        )
        if not is_has_pir_true(attrs.get(ASSET_ATTR_HAS_PIR)):
            continue

        occ_val = attrs.get(ASSET_ATTR_OCCUPIED)
        if occ_val is True or (isinstance(occ_val, str) and occ_val.strip().lower() == "true"):
            occupied = True
        elif occ_val is False or (isinstance(occ_val, str) and occ_val.strip().lower() == "false"):
            occupied = False
        else:
            occupied = None

        asset_name = (asset.get("name") or "").strip()
        asset_label = (asset.get("label") or "").strip()
        relations = get_relations_from_asset(session, base_url, asset_id)
        now_ms = int(time.time() * 1000)
        ws202_devices: List[Dict[str, Any]] = []
        ws203_devices: List[Dict[str, Any]] = []
        for rel in relations:
            to_ent = rel.get("to") or {}
            dev_id = to_ent.get("id") if isinstance(to_ent, dict) else None
            if not dev_id:
                continue
            device = get_device(session, base_url, dev_id)
            if not device:
                continue
            pid = device.get("deviceProfileId")
            if isinstance(pid, dict):
                pid = pid.get("id")
            profile_name = (profile_map.get(str(pid or "")) or "").strip()
            profile_key = profile_name.lower()
            if profile_key not in (DEVICE_PROFILE_WS202, DEVICE_PROFILE_WS203):
                continue
            dev_name = (device.get("name") or "").strip()
            dev_label = (device.get("label") or "").strip()
            dev_uid = device_uid(device)
            if profile_key == DEVICE_PROFILE_WS202:
                pir_value, pir_ts = get_latest_telemetry(session, base_url, dev_uid, TELEMETRY_KEY_PIR)
                window_start_ms = now_ms - RULE2_WINDOW_MINUTES * 60 * 1000
                pir_entries = get_telemetry_timeseries(
                    session, base_url, dev_uid, TELEMETRY_KEY_PIR, window_start_ms, now_ms
                )
                trigger_sum_ms = trigger_duration_ms(pir_entries, now_ms)
                ws202_devices.append({
                    "device_id": dev_uid,
                    "device_name": dev_name,
                    "device_label": dev_label,
                    "profile": DEVICE_PROFILE_WS202,
                    "pir_value": pir_value,
                    "pir_ts": pir_ts,
                    "trigger_sum_ms": trigger_sum_ms,
                })
                continue
            occ_value, occ_ts = get_latest_telemetry(session, base_url, dev_uid, TELEMETRY_KEY_OCCUPANCY)
            hist_start_ms = now_ms - WS203_HISTORY_MINUTES * 60 * 1000
            occ_entries = get_telemetry_timeseries(
                session, base_url, dev_uid, TELEMETRY_KEY_OCCUPANCY, hist_start_ms, now_ms
            )
            streak_ms = occupancy_streak_ms(occ_entries, now_ms)
            ws203_devices.append({
                "device_id": dev_uid,
                "device_name": dev_name,
                "device_label": dev_label,
                "profile": DEVICE_PROFILE_WS203,
                "occupancy_value": occ_value,
                "occupancy_ts": occ_ts,
                "occupancy_streak_ms": streak_ms,
            })

        # ws203 (Regel 4/5) gilt für das Asset, sobald ein ws203 hängt.
        # ws202 (Regel 1–3) nur, wenn kein ws203 vorhanden ist.
        # Regel 4: occupancy mindestens 30 Min am Stück „occupied“ → 2 h belegt
        # Regel 5: Haltezeit vorbei und kein 30-Minuten-Stück mehr → frei
        set_occupied = ""
        set_occupied_rule = ""
        ws203_action = ""
        occupied_until_ms: Optional[int] = None
        if ws203_devices:
            streak_ms = max(d.get("occupancy_streak_ms") or 0 for d in ws203_devices)
            streak_ok = streak_ms >= WS203_STREAK_MINUTES * 60 * 1000
            hold_until = as_epoch_ms(attrs.get(ASSET_ATTR_OCCUPIED_UNTIL))
            hold_active = hold_until is not None and hold_until > now_ms
            new_until = now_ms + WS203_HOLD_MINUTES * 60 * 1000
            if streak_ok and occupied is not True:
                ws203_action = "set_true"
                set_occupied = "true"
                set_occupied_rule = "Regel 4"
                occupied_until_ms = new_until
            elif streak_ok and occupied is True and not hold_active:
                ws203_action = "renew"
                set_occupied_rule = "Regel 4"
                occupied_until_ms = new_until
            elif occupied is True and not hold_active and not streak_ok:
                ws203_action = "set_false"
                set_occupied = "false"
                set_occupied_rule = "Regel 5"
                occupied_until_ms = 0
            elif hold_active:
                occupied_until_ms = hold_until
            threshold_ms = WS203_STREAK_MINUTES * 60 * 1000
            for d in ws203_devices:
                if ws203_action == "set_false":
                    d["setOccupied_rule"] = "Regel 5"
                elif ws203_action in ("set_true", "renew") and (d.get("occupancy_streak_ms") or 0) >= threshold_ms:
                    d["setOccupied_rule"] = "Regel 4"
                else:
                    d["setOccupied_rule"] = ""
            for d in ws202_devices:
                d["setOccupied_rule"] = ""
        elif occupied is False:
            pir_minutes_ms = args.pir_minutes * 60 * 1000
            rule2_threshold_ms = RULE2_TRIGGER_MINUTES * 60 * 1000
            rules_applied: set = set()
            for d in ws202_devices:
                pv, pts = d.get("pir_value"), d.get("pir_ts")
                rule1 = bool(is_pir_active(pv) and pts is not None and (now_ms - int(pts)) >= pir_minutes_ms)
                rule2 = d.get("trigger_sum_ms", 0) >= rule2_threshold_ms
                dev_rules = []
                if rule1:
                    dev_rules.append("Regel 1")
                    rules_applied.add("Regel 1")
                if rule2:
                    dev_rules.append("Regel 2")
                    rules_applied.add("Regel 2")
                d["setOccupied_rule"] = ", ".join(dev_rules) if dev_rules else ""
                if rule1 or rule2:
                    set_occupied = "true"
            if rules_applied:
                set_occupied_rule = ", ".join(sorted(rules_applied))
        elif occupied is True and ws202_devices:
            # Regel 3: PIR seit mehr als 60 Min "normal" = letztes PIR-Ereignis (raumweit) ist "normal" und älter als 60 Min
            rule3_min_ms = RULE3_WINDOW_MINUTES * 60 * 1000
            latest_ts = max((d.get("pir_ts") or 0) for d in ws202_devices)
            latest_device = next((d for d in ws202_devices if (d.get("pir_ts") or 0) == latest_ts), None)
            rule3_applies = (
                latest_device is not None
                and latest_ts > 0
                and not is_pir_active(latest_device.get("pir_value"))
                and (now_ms - latest_ts) >= rule3_min_ms
            )
            if rule3_applies:
                set_occupied = "false"
                set_occupied_rule = "Regel 3"
                for d in ws202_devices:
                    d["setOccupied_rule"] = "Regel 3"
            else:
                for d in ws202_devices:
                    d["setOccupied_rule"] = ""
        else:
            for d in ws202_devices:
                d["setOccupied_rule"] = ""

        rows.append({
            "customer_id": customer_id,
            "asset_id": asset_id,
            "asset_name": asset_name,
            "asset_label": asset_label,
            "occupied": occupied,
            "setOccupied": set_occupied,
            "setOccupied_rule": set_occupied_rule,
            "ws203_action": ws203_action,
            "occupied_until_ms": occupied_until_ms,
            "hasPir": True,
            "ws202_devices": ws202_devices,
            "ws203_devices": ws203_devices,
        })

    # Optional: occupied auf true setzen (Regel 1/2 für ws202, Regel 4 für ws203)
    write_true = getattr(args, "set_occupied_when_pir_active", False)
    for r in rows:
        if r.get("setOccupied") != "true" or r.get("ws203_action"):
            continue
        if not write_true:
            continue
        if set_asset_attribute(session, base_url, r["asset_id"], ASSET_ATTR_OCCUPIED, True):
            r["occupied"] = True
            if save_asset_telemetry(session, base_url, r["asset_id"], {ASSET_ATTR_OCCUPIED: True}):
                _log_line(f"   ✅ {r['asset_name']}: occupied auf true gesetzt, Telemetrie geschrieben (PIR > {args.pir_minutes} Min aktiv)")
            else:
                _log_line(f"   ⚠️ {r['asset_name']}: occupied auf true gesetzt, Telemetrie fehlgeschlagen (PIR > {args.pir_minutes} Min aktiv)")
        else:
            _log_line(f"   ❌ {r['asset_name']}: occupied setzen fehlgeschlagen")

    for r in rows:
        action = r.get("ws203_action")
        if action not in ("set_true", "renew"):
            continue
        if not write_true:
            continue
        until_ms = int(r.get("occupied_until_ms") or 0)
        until_str = datetime.fromtimestamp(until_ms / 1000).strftime("%Y-%m-%d %H:%M:%S") if until_ms else "—"
        if action == "set_true":
            ok = set_asset_attributes(
                session,
                base_url,
                r["asset_id"],
                {ASSET_ATTR_OCCUPIED: True, ASSET_ATTR_OCCUPIED_UNTIL: until_ms},
            )
            if not ok:
                _log_line(f"   ❌ {r['asset_name']}: occupied setzen fehlgeschlagen (Regel 4)")
                continue
            r["occupied"] = True
            if save_asset_telemetry(session, base_url, r["asset_id"], {ASSET_ATTR_OCCUPIED: True}):
                _log_line(f"   ✅ {r['asset_name']}: occupied auf true gesetzt, Telemetrie geschrieben (Regel 4: 30 Min occupied, belegt bis {until_str})")
            else:
                _log_line(f"   ⚠️ {r['asset_name']}: occupied auf true gesetzt, Telemetrie fehlgeschlagen (Regel 4, belegt bis {until_str})")
        elif set_asset_attribute(session, base_url, r["asset_id"], ASSET_ATTR_OCCUPIED_UNTIL, until_ms):
            if save_asset_telemetry(session, base_url, r["asset_id"], {ASSET_ATTR_OCCUPIED: True}):
                _log_line(f"   ✅ {r['asset_name']}: Belegung um 2 h verlängert, Telemetrie occupied=true (Regel 4, belegt bis {until_str})")
            else:
                _log_line(f"   ⚠️ {r['asset_name']}: Belegung um 2 h verlängert, Telemetrie fehlgeschlagen (Regel 4, belegt bis {until_str})")
        else:
            _log_line(f"   ❌ {r['asset_name']}: occupiedUntil verlängern fehlgeschlagen (Regel 4)")

    # Regel 3: occupied auf false setzen, wenn 60 Min PIR "normal"
    # Regel 5: ws203-Haltezeit abgelaufen und kein 30-Minuten-Stück occupied mehr
    for r in rows:
        if r.get("setOccupied") != "false":
            continue
        rule = r.get("setOccupied_rule")
        if rule == "Regel 3":
            if set_asset_attribute(session, base_url, r["asset_id"], ASSET_ATTR_OCCUPIED, False):
                r["occupied"] = False
                if save_asset_telemetry(session, base_url, r["asset_id"], {ASSET_ATTR_OCCUPIED: False}):
                    _log_line(f"   ✅ {r['asset_name']}: occupied auf false gesetzt, Telemetrie geschrieben (Regel 3: 60 Min PIR normal)")
                else:
                    _log_line(f"   ⚠️ {r['asset_name']}: occupied auf false gesetzt, Telemetrie fehlgeschlagen (Regel 3: 60 Min PIR normal)")
            else:
                _log_line(f"   ❌ {r['asset_name']}: occupied auf false setzen fehlgeschlagen (Regel 3)")
        elif rule == "Regel 5":
            if set_asset_attributes(
                session,
                base_url,
                r["asset_id"],
                {ASSET_ATTR_OCCUPIED: False, ASSET_ATTR_OCCUPIED_UNTIL: 0},
            ):
                r["occupied"] = False
                if save_asset_telemetry(session, base_url, r["asset_id"], {ASSET_ATTR_OCCUPIED: False}):
                    _log_line(f"   ✅ {r['asset_name']}: occupied auf false gesetzt, Telemetrie geschrieben (Regel 5: 2 h abgelaufen)")
                else:
                    _log_line(f"   ⚠️ {r['asset_name']}: occupied auf false gesetzt, Telemetrie fehlgeschlagen (Regel 5: 2 h abgelaufen)")
            else:
                _log_line(f"   ❌ {r['asset_name']}: occupied auf false setzen fehlgeschlagen (Regel 5)")

    # Ausgabe
    out_file = open(args.output_file, "w", encoding="utf-8", newline="") if args.output_file else sys.stdout
    try:
        if args.output == "csv":
            flat = []
            for r in rows:
                sensors = list(r.get("ws202_devices") or []) + list(r.get("ws203_devices") or [])
                if not sensors:
                    sensors = [{}]
                for d in sensors:
                    streak_ms = d.get("occupancy_streak_ms")
                    flat.append({
                        "customer_id": r["customer_id"],
                        "asset_id": r["asset_id"],
                        "asset_name": r["asset_name"],
                        "asset_label": r["asset_label"],
                        "occupied": r.get("occupied"),
                        "setOccupied": r.get("setOccupied", ""),
                        "setOccupied_rule": r.get("setOccupied_rule", ""),
                        "occupied_until_ms": r.get("occupied_until_ms") or "",
                        "device_id": d.get("device_id", ""),
                        "device_name": d.get("device_name", ""),
                        "device_label": d.get("device_label", ""),
                        "device_profile": d.get("profile", ""),
                        "setOccupied_rule_device": d.get("setOccupied_rule", ""),
                        "pir_value": d.get("pir_value", ""),
                        "pir_ts": d.get("pir_ts", ""),
                        "occupancy_value": d.get("occupancy_value", ""),
                        "occupancy_streak_min": int(streak_ms / 60000) if streak_ms else "",
                    })
            fieldnames = [
                "customer_id", "asset_id", "asset_name", "asset_label", "occupied", "setOccupied", "setOccupied_rule",
                "occupied_until_ms", "device_id", "device_name", "device_label", "device_profile",
                "setOccupied_rule_device", "pir_value", "pir_ts", "occupancy_value", "occupancy_streak_min",
            ]
            writer = csv.DictWriter(out_file, fieldnames=fieldnames, delimiter=";")
            writer.writeheader()
            writer.writerows(flat)
        elif args.output == "json":
            json.dump(rows, out_file, indent=2, ensure_ascii=False)
        else:
            print(f"Customer-ID: {customer_id}  –  {len(rows)} Asset(s) mit hasPir=true\n", file=out_file)
            for r in rows:
                occ = r.get("occupied")
                occ_str = "true" if occ is True else ("false" if occ is False else "—")
                set_occ = r.get("setOccupied") or ""
                set_occ_rule = r.get("setOccupied_rule") or "—"
                until_ms = r.get("occupied_until_ms") or 0
                until_str = ""
                if until_ms:
                    try:
                        until_str = "  occupiedUntil=" + datetime.fromtimestamp(int(until_ms) / 1000).strftime("%Y-%m-%d %H:%M:%S")
                    except Exception:
                        until_str = f"  occupiedUntil={until_ms}"
                print(f"Asset: {r['asset_name']}  (Label: {r['asset_label']}  ID: {r['asset_id']})  occupied={occ_str}  setOccupied={set_occ or '—'}  setOccupied_rule={set_occ_rule}{until_str}", file=out_file)
                if not r.get("ws202_devices") and not r.get("ws203_devices"):
                    print("   Keine ws202/ws203-Devices (Related)", file=out_file)
                for d in r.get("ws202_devices") or []:
                    ts_str = ""
                    if d.get("pir_ts") is not None:
                        try:
                            ts_str = datetime.utcfromtimestamp(d["pir_ts"] / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")
                        except Exception:
                            ts_str = str(d["pir_ts"])
                    set_occ_line = ""
                    if occ is False and d.get("setOccupied_rule"):
                        set_occ_line = f"  setOccupied = true ({d.get('setOccupied_rule')})"
                    elif occ is True and d.get("setOccupied_rule") == "Regel 3":
                        set_occ_line = "  setOccupied = false (Regel 3)"
                    print(f"   ws202: {d.get('device_name') or d.get('device_id')}  (Label: {d.get('device_label')})  device_id={d.get('device_id', '')}  pir={d.get('pir_value')}  ts={ts_str}{set_occ_line}", file=out_file)
                for d in r.get("ws203_devices") or []:
                    ts_str = ""
                    if d.get("occupancy_ts") is not None:
                        try:
                            ts_str = datetime.utcfromtimestamp(d["occupancy_ts"] / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")
                        except Exception:
                            ts_str = str(d["occupancy_ts"])
                    streak_min = int((d.get("occupancy_streak_ms") or 0) / 60000)
                    set_occ_line = ""
                    if d.get("setOccupied_rule") == "Regel 4":
                        set_occ_line = "  setOccupied = true (Regel 4, 2 h)" if r.get("ws203_action") == "set_true" else "  Haltezeit +2 h (Regel 4)"
                    elif d.get("setOccupied_rule") == "Regel 5":
                        set_occ_line = "  setOccupied = false (Regel 5)"
                    print(
                        f"   ws203: {d.get('device_name') or d.get('device_id')}  (Label: {d.get('device_label')})  "
                        f"device_id={d.get('device_id', '')}  occupancy={d.get('occupancy_value')}  "
                        f"streak={streak_min} min  ts={ts_str}{set_occ_line}",
                        file=out_file,
                    )
                print(file=out_file)
    finally:
        if args.output_file and out_file is not sys.stdout:
            out_file.close()

    if args.output_file:
        _log_line(f"Ausgabe: {args.output_file}  ({len(rows)} Assets mit hasPir)")
    _log_line(f"✅ Fertig. {len(rows)} Asset(s) mit hasPir=true.")


if __name__ == "__main__":
    main()
