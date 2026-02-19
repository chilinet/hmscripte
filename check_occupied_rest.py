#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Check Occupied (REST)
====================
Gleiche Daten wie check_occupied (PIR/ws202), aber über ThingsBoard Web-API.

- Customer-Assets: GET /api/customer/{customer_id}/assets
- Pro Asset: Attribut hasPir = true prüfen (Asset-Attribute)
- Related Devices zum Asset holen, nur Profil ws202
- Von jedem ws202-Device: letzten Telemetriewert „pir“ anzeigen
- Asset-Attribut „occupied“ ausgeben
- setOccupied: "true" wenn PIR (z.B. trigger) und ts älter als 5 Min, sonst leer
- Optional: --set-occupied-when-pir-active → bei PIR > 5 Min aktiv: occupied per API auf true setzen

Verwendung:
  python check_occupied_rest.py --customer-id <UUID>   # TB-Login aus MSSQL (Standard)
  python check_occupied_rest.py --customer-id <UUID> --no-credentials-from-db   # TB aus .env/getpass
  python check_occupied_rest.py --customer-id <UUID> --output csv -o occupied_rest.csv
  python check_occupied_rest.py --customer-id <UUID> --set-occupied-when-pir-active

.env: THINGBOARD_URL, THINGBOARD_USERNAME, THINGBOARD_PASSWORD
  Standard: ThingsBoard-Login aus MSSQL customer_settings (tb_username, tb_password) per customer_id.
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
ASSET_ATTR_HAS_PIR = "hasPir"
ASSET_ATTR_OCCUPIED = "occupied"
ASSET_ATTR_SCOPE = "SERVER_SCOPE"
TELEMETRY_KEY_PIR = "pir"
PIR_ACTIVE_MINUTES_DEFAULT = 5
# Regel 2: setOccupied = true wenn in den letzten RULE2_WINDOW_MINUTES Min die PIR in Summe RULE2_TRIGGER_MINUTES Min auf trigger stand
RULE2_WINDOW_MINUTES = 10
RULE2_TRIGGER_MINUTES = 5
# Regel 3: setOccupied = false wenn occupied = true und in den letzten 30 Min kein Trigger ausgelöst wurde
RULE3_WINDOW_MINUTES = 30


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Check Occupied über ThingsBoard REST: Assets mit hasPir, ws202-Devices, letzter pir-Wert."
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
        help="ThingsBoard-Anmeldung aus .env/CLI statt aus MSSQL",
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
        help="Wenn PIR länger als --pir-minutes aktiv: Asset-Attribut occupied auf true setzen",
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
) -> Tuple[Optional[str], Optional[str]]:
    """Holt tb_username und tb_password aus MSSQL customer_settings für die gegebene customer_id."""
    def _log(msg: str) -> None:
        if debug:
            print(f"   [DEBUG] TB aus MSSQL: {msg}", file=sys.stderr)

    def _fail(reason: str) -> Tuple[Optional[str], Optional[str]]:
        print(f"   Ursache: {reason}", file=sys.stderr)
        return None, None

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
            "SELECT tb_username, tb_password FROM customer_settings WHERE customer_id = ?",
            (customer_id,),
        )
        row = cursor.fetchone()
        cursor.close()
        if row and len(row) >= 2:
            uname = str(row[0]).strip() if row[0] else None
            pwd = str(row[1]) if row[1] else None
            if not uname or not pwd:
                _log(f"Eintrag für customer_id={customer_id} gefunden, aber tb_username oder tb_password leer.")
                return _fail("Eintrag in customer_settings hat leeres tb_username oder tb_password.")
            return (uname, pwd)
        _log(f"Kein Eintrag in customer_settings für customer_id={customer_id} (SELECT tb_username, tb_password).")
        return _fail(f"Kein Eintrag in customer_settings für customer_id={customer_id} (Spalten tb_username, tb_password).")
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
    params = {"keys": key, "startTs": start_ts_ms, "endTs": end_ts_ms}
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
    print("=" * 80)
    print("CHECK OCCUPIED (REST)")
    print("=" * 80)

    args = parse_arguments()
    
    print("args: ", args)
    
    customer_id = (args.customer_id or "").strip()
    if not customer_id:
        print("❌ --customer-id fehlt.", file=sys.stderr)
        sys.exit(1)

    base_url = normalize_base_url(args.thingboard_url)
    if not base_url:
        print("❌ ThingsBoard-URL fehlt.", file=sys.stderr)
        sys.exit(1)

    credentials_from_db = not getattr(args, "no_credentials_from_db", False)
    if credentials_from_db:
        print(f"\n📋 Hole ThingsBoard-Anmeldedaten aus MSSQL (customer_settings, customer_id={customer_id})...")
        username, password = get_tb_credentials_from_mssql(
            customer_id,
            args.mssql_server,
            args.mssql_database,
            args.mssql_user,
            args.mssql_password,
            debug=getattr(args, "debug", False),
        )


        if not username or not password:
            print("❌ ThingsBoard-Anmeldung aus Datenbank fehlgeschlagen.", file=sys.stderr)
            sys.exit(1)
        print("   ✅ Anmeldedaten aus customer_settings geladen")
    else:
        username = (args.thingboard_username or "").strip()
        if not username:
            print("❌ THINGBOARD_USERNAME fehlt (oder --thingboard-username).", file=sys.stderr)
            sys.exit(1)
        password = args.thingboard_password
        if not password:
            password = getpass.getpass("ThingsBoard Passwort: ")

    verify_ssl = TB_VERIFY_SSL and not args.no_verify_ssl
    session = requests.Session()
    session.verify = verify_ssl

    try:
        token = tb_login(session, base_url, username, password)
        session.headers["X-Authorization"] = f"Bearer {token}"
    except Exception as e:
        print(f"❌ Login: {e}", file=sys.stderr)
        sys.exit(1)

    print(f"\n📋 Lade Assets für Customer {customer_id}...")
    try:
        assets = get_customer_assets(session, base_url, customer_id, args.page_size)
    except Exception as e:
        print(f"❌ {e}", file=sys.stderr)
        sys.exit(1)
    print(f"   {len(assets)} Assets gefunden")

    print(f"\n📋 Lade Device-Profile...")
    profile_map = get_device_profile_map(session, base_url, args.page_size)

    rows: List[Dict[str, Any]] = []
    for asset in assets:
        asset_id = asset_uid(asset)
        if not asset_id:
            continue
        attrs = get_asset_attributes(session, base_url, asset_id, keys=f"{ASSET_ATTR_HAS_PIR},{ASSET_ATTR_OCCUPIED}")
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
        ws202_devices: List[Dict[str, Any]] = []
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
            if profile_name != DEVICE_PROFILE_WS202:
                continue
            dev_name = (device.get("name") or "").strip()
            dev_label = (device.get("label") or "").strip()
            dev_uid = device_uid(device)
            pir_value, pir_ts = get_latest_telemetry(session, base_url, dev_uid, TELEMETRY_KEY_PIR)
            # Regel 2: Telemetrie letzte 10 Min, Summe Trigger-Dauer
            now_ms = int(time.time() * 1000)
            window_start_ms = now_ms - RULE2_WINDOW_MINUTES * 60 * 1000
            pir_entries = get_telemetry_timeseries(
                session, base_url, dev_uid, TELEMETRY_KEY_PIR, window_start_ms, now_ms
            )
            trigger_sum_ms = trigger_duration_ms(pir_entries, now_ms)
            # Regel 3 (nur bei occupied = true): Trigger in den letzten 30 Min?
            trigger_in_last_30min = None
            if occupied is True:
                window_30_start_ms = now_ms - RULE3_WINDOW_MINUTES * 60 * 1000
                entries_30 = get_telemetry_timeseries(
                    session, base_url, dev_uid, TELEMETRY_KEY_PIR, window_30_start_ms, now_ms
                )
                trigger_in_last_30min = any(
                    is_pir_active(e.get("value")) for e in entries_30 if isinstance(e, dict)
                )
            ws202_devices.append({
                "device_id": dev_uid,
                "device_name": dev_name,
                "device_label": dev_label,
                "pir_value": pir_value,
                "pir_ts": pir_ts,
                "trigger_sum_ms": trigger_sum_ms,
                "trigger_in_last_30min": trigger_in_last_30min,
            })

        # setOccupied: Regel 1 und 2 nur wenn occupied = false; Regel 3 nur wenn occupied = true
        # Regel 1: PIR aktiv und ts älter als pir_minutes
        # Regel 2: In den letzten 10 Min war PIR in Summe >= 5 Min auf trigger
        # Regel 3: In den letzten 30 Min kein Trigger → setOccupied = false
        set_occupied = ""
        set_occupied_rule = ""
        if occupied is False:
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
        elif occupied is True:
            # Regel 3: kein Trigger in den letzten 30 Min → setOccupied = false
            any_trigger_in_30min = any(
                d.get("trigger_in_last_30min", False) for d in ws202_devices
            )
            if ws202_devices and not any_trigger_in_30min:
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
            "hasPir": True,
            "ws202_devices": ws202_devices,
        })

    # Optional: occupied auf true setzen, wenn PIR länger als pir_minutes aktiv (Regel 1/2)
    for r in rows:
        if r.get("setOccupied") != "true":
            continue
        if not getattr(args, "set_occupied_when_pir_active", False):
            continue
        if set_asset_attribute(session, base_url, r["asset_id"], ASSET_ATTR_OCCUPIED, True):
            r["occupied"] = True
            print(f"   ✅ {r['asset_name']}: occupied auf true gesetzt (PIR > {args.pir_minutes} Min aktiv)", file=sys.stderr)
        else:
            print(f"   ❌ {r['asset_name']}: occupied setzen fehlgeschlagen", file=sys.stderr)

    # Regel 3: occupied auf false setzen, wenn 30 Min kein Trigger
    for r in rows:
        if r.get("setOccupied") != "false" or r.get("setOccupied_rule") != "Regel 3":
            continue
        if set_asset_attribute(session, base_url, r["asset_id"], ASSET_ATTR_OCCUPIED, False):
            r["occupied"] = False
            print(f"   ✅ {r['asset_name']}: occupied auf false gesetzt (Regel 3: 30 Min kein Trigger)", file=sys.stderr)
        else:
            print(f"   ❌ {r['asset_name']}: occupied auf false setzen fehlgeschlagen (Regel 3)", file=sys.stderr)

    # Ausgabe
    out_file = open(args.output_file, "w", encoding="utf-8", newline="") if args.output_file else sys.stdout
    try:
        if args.output == "csv":
            flat = []
            for r in rows:
                if r["ws202_devices"]:
                    for d in r["ws202_devices"]:
                        flat.append({
                            "customer_id": r["customer_id"],
                            "asset_id": r["asset_id"],
                            "asset_name": r["asset_name"],
                            "asset_label": r["asset_label"],
                            "occupied": r.get("occupied"),
                            "setOccupied": r.get("setOccupied", ""),
                            "setOccupied_rule": r.get("setOccupied_rule", ""),
                            "device_id": d["device_id"],
                            "device_name": d["device_name"],
                            "device_label": d["device_label"],
                            "setOccupied_rule_device": d.get("setOccupied_rule", ""),
                            "pir_value": d.get("pir_value"),
                            "pir_ts": d.get("pir_ts"),
                        })
                else:
                    flat.append({
                        "customer_id": r["customer_id"],
                        "asset_id": r["asset_id"],
                        "asset_name": r["asset_name"],
                        "asset_label": r["asset_label"],
                        "occupied": r.get("occupied"),
                        "setOccupied": r.get("setOccupied", ""),
                        "setOccupied_rule": r.get("setOccupied_rule", ""),
                        "device_id": "",
                        "device_name": "",
                        "device_label": "",
                        "setOccupied_rule_device": "",
                        "pir_value": "",
                        "pir_ts": "",
                    })
            fieldnames = [
                "customer_id", "asset_id", "asset_name", "asset_label", "occupied", "setOccupied", "setOccupied_rule",
                "device_id", "device_name", "device_label", "setOccupied_rule_device", "pir_value", "pir_ts",
            ]
            writer = csv.DictWriter(out_file, fieldnames=fieldnames, delimiter=";")
            writer.writeheader()
            writer.writerows(flat)
        elif args.output == "json":
            json.dump(rows, out_file, indent=2, ensure_ascii=False)
        else:
            pir_minutes_ms = args.pir_minutes * 60 * 1000
            now_ms = int(time.time() * 1000)
            print(f"Customer-ID: {customer_id}  –  {len(rows)} Asset(s) mit hasPir=true\n", file=out_file)
            for r in rows:
                occ = r.get("occupied")
                occ_str = "true" if occ is True else ("false" if occ is False else "—")
                set_occ = r.get("setOccupied") or ""
                set_occ_rule = r.get("setOccupied_rule") or "—"
                print(f"Asset: {r['asset_name']}  (Label: {r['asset_label']}  ID: {r['asset_id']})  occupied={occ_str}  setOccupied={set_occ or '—'}  setOccupied_rule={set_occ_rule}", file=out_file)
                if not r["ws202_devices"]:
                    print("   Keine ws202-Devices (Related)", file=out_file)
                else:
                    for d in r["ws202_devices"]:
                        ts_str = ""
                        if d.get("pir_ts") is not None:
                            try:
                                ts_str = datetime.utcfromtimestamp(d["pir_ts"] / 1000).strftime("%Y-%m-%d %H:%M:%S UTC")
                            except Exception:
                                ts_str = str(d["pir_ts"])
                        # setOccupied + Regel anzeigen: Regel 1/2 (occupied=false) → true; Regel 3 (occupied=true) → false
                        set_occ_line = ""
                        if occ is False and d.get("setOccupied_rule"):
                            set_occ_line = f"  setOccupied = true ({d.get('setOccupied_rule')})"
                        elif occ is True and d.get("setOccupied_rule") == "Regel 3":
                            set_occ_line = "  setOccupied = false (Regel 3)"
                        print(f"   ws202: {d.get('device_name') or d.get('device_id')}  (Label: {d.get('device_label')})  device_id={d.get('device_id', '')}  pir={d.get('pir_value')}  ts={ts_str}{set_occ_line}", file=out_file)
                print(file=out_file)
    finally:
        if args.output_file and out_file is not sys.stdout:
            out_file.close()

    if args.output_file:
        print(f"Ausgabe: {args.output_file}  ({len(rows)} Assets mit hasPir)", file=sys.stderr)
    print(f"\n✅ Fertig. {len(rows)} Asset(s) mit hasPir=true.")


if __name__ == "__main__":
    main()
