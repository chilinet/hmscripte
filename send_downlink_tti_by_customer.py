#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Downlink per LNS nach Customer-Filter (DevEUI, Name, Device-Profil)
====================================================================
Sendet einen Hex-Payload über die LNS API an Devices, die über ThingsBoard pro Customer
ausgewählt werden. Filter: einzelne DevEUI(s), Name(s), Device-Profil/Type.

Gleiches Muster wie check_dnt_deviceerror.py: LNS_API_URL, LNS_API_KEY aus .env,
keine TTI-Konfiguration nötig. Argumente, .env, klare Schritte, Dry-Run, Zusammenfassung.

- ThingsBoard: Login, Customer-Devices laden, nach DevEUI / Name / Device-Profil filtern.
- LNS: Downlink an die gematchten Devices senden (deviceId = ThingsBoard Device-UUID).
"""

import os
import sys
import argparse
import getpass
import json
import requests
from dotenv import load_dotenv
from typing import Any, Dict, List, Optional

load_dotenv()

# ThingsBoard
THINGBOARD_URL = (os.getenv("THINGBOARD_URL") or os.getenv("TB_BASE_URL") or "").strip()
THINGBOARD_USERNAME = (os.getenv("THINGBOARD_USERNAME") or os.getenv("TB_USERNAME") or "").strip()
THINGBOARD_PASSWORD = os.getenv("THINGBOARD_PASSWORD") or os.getenv("TB_PASSWORD") or ""
TB_VERIFY_SSL = os.getenv("TB_VERIFY_SSL", "true").strip().lower() not in ("0", "false", "no")

# LNS API (wie check_dnt_deviceerror.py)
LNS_API_URL = os.getenv("LNS_API_URL", "http://localhost:3000/api/lns/downlink")
LNS_API_KEY = os.getenv("LNS_API_KEY")
REQUEST_TIMEOUT = 30


def parse_arguments():
    """Parst die Kommandozeilenargumente (Muster wie check_dnt_deviceerror.py)."""
    parser = argparse.ArgumentParser(
        description="Sendet Downlink-Payload per LNS für Devices eines Customers (Filter: DevEUI, Name, Device-Profil)."
    )
    parser.add_argument(
        "customer_id",
        help="ThingsBoard Customer-ID (UUID)",
    )
    parser.add_argument(
        "--payload",
        required=True,
        help="Hex-Payload (z.B. '2232' oder '35')",
    )
    parser.add_argument(
        "--deveui",
        action="append",
        dest="deveuis",
        metavar="DEVEUIs",
        help="Nur Devices mit diesem Label/DevEUI (kann mehrfach angegeben werden)",
    )
    parser.add_argument(
        "--name",
        action="append",
        dest="names",
        metavar="NAME",
        help="Nur Devices mit diesem Namen (exakt; kann mehrfach angegeben werden)",
    )
    parser.add_argument(
        "--device-profile",
        dest="device_profile",
        metavar="PROFILNAME",
        help="Nur Devices mit diesem Device-Profil (z.B. dnt-LW-WSCI)",
    )
    parser.add_argument(
        "--lns-api-url",
        default=LNS_API_URL,
        help="LNS API URL (Standard: LNS_API_URL aus .env)",
    )
    parser.add_argument(
        "--lns-api-key",
        default=LNS_API_KEY,
        help="LNS API Key (Standard: LNS_API_KEY aus .env)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Nur anzeigen, keine Downlinks senden",
    )
    parser.add_argument(
        "--thingboard-url",
        default=THINGBOARD_URL,
        help="ThingsBoard Base-URL (Standard: THINGBOARD_URL aus .env)",
    )
    parser.add_argument(
        "--thingboard-username",
        default=THINGBOARD_USERNAME,
        help="ThingsBoard Benutzer (Standard: THINGBOARD_USERNAME aus .env)",
    )
    parser.add_argument(
        "--thingboard-password",
        default=THINGBOARD_PASSWORD,
        help="ThingsBoard Passwort (Standard: THINGBOARD_PASSWORD aus .env)",
    )
    parser.add_argument(
        "--no-verify-ssl",
        action="store_true",
        help="SSL bei ThingsBoard nicht prüfen",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=200,
        help="Seitengröße für ThingsBoard Customer-Devices (Standard: 200)",
    )
    return parser.parse_args()


def normalize_base_url(url: str) -> str:
    u = (url or "").strip()
    return u.rstrip("/") if u else ""


# ---------- ThingsBoard ----------


def tb_login(session: requests.Session, base_url: str, username: str, password: str) -> str:
    url = f"{base_url}/api/auth/login"
    r = session.post(url, json={"username": username, "password": password}, timeout=REQUEST_TIMEOUT)
    if r.status_code != 200:
        raise RuntimeError(f"Login fehlgeschlagen: HTTP {r.status_code} - {r.text}")
    token = r.json().get("token")
    if not token:
        raise RuntimeError("Login-Antwort enthält kein Token")
    return token


def get_device_profile_map(session: requests.Session, base_url: str, page_size: int) -> Dict[str, str]:
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


def get_customer_devices(
    session: requests.Session,
    base_url: str,
    customer_id: str,
    page_size: int,
) -> List[Dict[str, Any]]:
    url = f"{base_url}/api/customer/{customer_id}/devices"
    all_devices: List[Dict[str, Any]] = []
    page = 0
    while True:
        params = {"pageSize": page_size, "page": page}
        r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"Customer-Devices fehlgeschlagen: HTTP {r.status_code} - {r.text}")
        data = r.json()
        devices = data.get("data") or []
        all_devices.extend(devices)
        if not data.get("hasNext", False):
            break
        page += 1
    return all_devices


def device_profile_id(device: Dict[str, Any]) -> Optional[str]:
    pid = device.get("deviceProfileId")
    if pid is None:
        return None
    if isinstance(pid, dict):
        return pid.get("id")
    return str(pid)


def device_label(device: Dict[str, Any]) -> str:
    return (device.get("label") or "").strip()


def device_name(device: Dict[str, Any]) -> str:
    return (device.get("name") or "").strip()


def normalize_deveui(s: str) -> str:
    """Normalisiert DevEUI/Label für Vergleich (lowercase, ohne Trennzeichen)."""
    return (s or "").strip().lower().replace(" ", "").replace("-", "").replace(":", "")


def filter_devices(
    devices: List[Dict[str, Any]],
    profile_map: Dict[str, str],
    deveuis: Optional[List[str]],
    names: Optional[List[str]],
    device_profile: Optional[str],
) -> List[Dict[str, Any]]:
    out = []
    for d in devices:
        if deveuis:
            label_norm = normalize_deveui(device_label(d))
            if not any(normalize_deveui(du) == label_norm for du in deveuis):
                continue
        if names:
            name = device_name(d)
            if not any(n.strip() == name for n in names):
                continue
        if device_profile:
            pid = device_profile_id(d)
            if (profile_map.get(pid or "") or "").strip() != (device_profile or "").strip():
                continue
        out.append(d)
    return out


# ---------- LNS ----------


def send_downlink_lns(
    device_id: str,
    hex_payload: str,
    api_url: str,
    api_key: str,
    dry_run: bool,
    name: str = "",
    label: str = "",
) -> bool:
    """Sendet Downlink über die LNS API (wie check_dnt_deviceerror). device_id = ThingsBoard Device-UUID."""
    if not api_key:
        print("❌ LNS_API_KEY fehlt in .env", file=sys.stderr)
        return False
    if not hex_payload:
        print(f"   ⚠️  Leerer Payload – überspringe Device {device_id}", file=sys.stderr)
        return False

    payload = {
        "deviceId": device_id,
        "frm_payload": hex_payload,
        "confirmed": False,
        "priority": "NORMAL",
    }
    headers = {"Content-Type": "application/json", "X-API-Key": api_key}

    if dry_run:
        info = f" (Name: {name}, Label: {label})" if (name or label) else ""
        print(f"   🔍 DRY-RUN: Würde Downlink senden für Device {device_id}{info}")
        print(f"      Hex-Payload: {hex_payload}")
        print(f"      Payload: {json.dumps(payload, indent=2)}")
        return True

    try:
        r = requests.post(api_url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
        if r.status_code in (200, 201, 202):
            return True
        print(f"   ❌ Fehler beim Senden für Device {device_id}: {r.status_code} - {r.text}", file=sys.stderr)
        return False
    except requests.exceptions.RequestException as e:
        print(f"   ❌ Request-Fehler für Device {device_id}: {e}", file=sys.stderr)
        return False


def _device_uid(device: Dict[str, Any]) -> str:
    """ThingsBoard Device-UUID (für LNS deviceId)."""
    did = device.get("id")
    if isinstance(did, dict):
        return (did.get("id") or "").strip()
    return (did or "").strip()


def main():
    print("=" * 80)
    print("DOWNLINK PER LNS NACH CUSTOMER-FILTER (DevEUI / Name / Device-Profil)")
    print("=" * 80)

    args = parse_arguments()

    # Pflicht: Customer, Payload, LNS
    customer_id = (args.customer_id or "").strip()
    if not customer_id:
        print("❌ customer_id fehlt.", file=sys.stderr)
        sys.exit(1)
    if not args.payload:
        print("❌ --payload fehlt.", file=sys.stderr)
        sys.exit(1)
    if not args.lns_api_key:
        print("❌ LNS_API_KEY fehlt. Bitte in .env setzen oder --lns-api-key angeben.", file=sys.stderr)
        sys.exit(1)

    # ThingsBoard
    tb_url = normalize_base_url(args.thingboard_url)
    tb_user = (args.thingboard_username or "").strip()
    if not tb_url or not tb_user:
        print("❌ ThingsBoard: THINGBOARD_URL und THINGBOARD_USERNAME (oder --thingboard-url, --thingboard-username) setzen.", file=sys.stderr)
        sys.exit(1)
    tb_password = args.thingboard_password
    if not tb_password:
        tb_password = getpass.getpass("ThingsBoard Passwort: ")

    verify_ssl = TB_VERIFY_SSL and not args.no_verify_ssl
    session = requests.Session()
    session.verify = verify_ssl

    print(f"\n1. ThingsBoard: Verbinde mit {tb_url} ...")
    try:
        token = tb_login(session, tb_url, tb_user, tb_password)
        session.headers["X-Authorization"] = f"Bearer {token}"
    except Exception as e:
        print(f"❌ ThingsBoard Login: {e}", file=sys.stderr)
        sys.exit(1)
    print("   ✅ ThingsBoard Login erfolgreich")

    print(f"\n2. Lade Device-Profile und Customer-Devices (Customer-ID: {customer_id}) ...")
    try:
        profile_map = get_device_profile_map(session, tb_url, args.page_size)
        devices = get_customer_devices(session, tb_url, customer_id, args.page_size)
    except Exception as e:
        print(f"❌ {e}", file=sys.stderr)
        sys.exit(1)
    print(f"   ✅ {len(devices)} Devices im Customer gefunden")

    # Filter: DevEUI(s), Name(s), Device-Profil (alle optional; ohne Filter = alle Devices des Customers)
    deveuis = getattr(args, "deveuis", None) or None
    names = getattr(args, "names", None) or None
    profile_name = (args.device_profile or "").strip() or None
    filtered = filter_devices(devices, profile_map, deveuis, names, profile_name)

    if not filtered:
        print("\n❌ Keine Devices entsprechen den Filtern (--deveui, --name, --device-profile).")
        sys.exit(1)
    print(f"\n3. Gefilterte Devices: {len(filtered)}")
    for i, d in enumerate(filtered, 1):
        print(f"   {i}. Name: {device_name(d) or '—'}  Label/DevEUI: {device_label(d) or '—'}  Profil: {profile_map.get(device_profile_id(d) or '', '—')}")

    # LNS: deviceId = ThingsBoard Device-UUID
    print(f"\n4. Sende Downlink per LNS (Payload: {args.payload}) ...")
    if args.dry_run:
        print("   🔍 DRY-RUN – keine echten Nachrichten.")

    success_count = 0
    error_count = 0
    for d in filtered:
        device_id = _device_uid(d)
        if not device_id:
            print(f"   ⚠️  Device ohne ID übersprungen: {device_name(d) or device_label(d)}")
            error_count += 1
            continue
        ok = send_downlink_lns(
            device_id,
            args.payload,
            args.lns_api_url,
            args.lns_api_key,
            args.dry_run,
            name=device_name(d),
            label=device_label(d),
        )
        if ok:
            success_count += 1
            if not args.dry_run:
                print(f"   ✅ Downlink gesendet für Device {device_id} (Name: {device_name(d) or '—'}, Label: {device_label(d) or '—'})")
        else:
            error_count += 1

    print(f"\n{'='*80}")
    print("ZUSAMMENFASSUNG")
    print(f"{'='*80}")
    print(f"Customer-ID: {customer_id}")
    print(f"Filter: DevEUI={deveuis or '—'}, Name={names or '—'}, Device-Profil={profile_name or '—'}")
    print(f"Gefilterte Devices: {len(filtered)}")
    print(f"Erfolgreich gesendet: {success_count}")
    print(f"Fehler: {error_count}")
    print(f"Payload (Hex): {args.payload}")
    if args.dry_run:
        print("   🔍 DRY-RUN – keine tatsächlichen Downlinks gesendet.")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
