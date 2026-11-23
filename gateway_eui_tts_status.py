#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ThingsBoard + The Things Stack + Wanesy: Gateway-Status
=======================================================
Durchsucht alle ThingsBoard-Devices nach dem Telemetrie-Key "gateway_eui"
und fragt für jede gefundene Gateway-EUI den Verbindungsstatus bei
The Things Stack ab (online / offline / nicht registriert).

Ist ein Gateway auf ThingsStack offline, wird der Status im Wanesy
Management Cockpit geprüft (https://wmc.wanesy.com). Operational oder
Warning gilt als aktiv: dann wird die Reboot-Aktion gesendet. Unreachable,
Unknown oder nicht vorhanden wird nur ausgegeben.

Verwendung:
  python gateway_eui_tts_status.py
  python gateway_eui_tts_status.py --output csv -o output/gateway_eui_tts_status.csv
  python gateway_eui_tts_status.py --no-reboot
  python gateway_eui_tts_status.py --details

.env:
  THINGBOARD_URL, THINGBOARD_USERNAME, THINGBOARD_PASSWORD
  TTS_BASE_URL
  Thingsstack_API_KEY   User-API-Key mit Recht, Gateways zu lesen
  TTS_API_KEY           Fallback (Application-Keys sehen oft keine Gateways)
  WMC_USERNAME, WMC_PASSWORD
"""

import argparse
import base64
import csv
import getpass
import json
import os
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

load_dotenv()

THINGBOARD_URL = (os.getenv("THINGBOARD_URL") or os.getenv("TB_BASE_URL") or "").strip()
THINGBOARD_USERNAME = (os.getenv("THINGBOARD_USERNAME") or os.getenv("TB_USERNAME") or "").strip()
THINGBOARD_PASSWORD = os.getenv("THINGBOARD_PASSWORD") or os.getenv("TB_PASSWORD") or ""
TB_VERIFY_SSL = os.getenv("TB_VERIFY_SSL", "true").strip().lower() not in ("0", "false", "no")

TTS_BASE_URL = (os.getenv("TTS_BASE_URL") or "").strip()
# User-Key zuerst: Application-Keys (TTS_API_KEY) liefern die Gateway-Liste oft leer.
TTS_API_KEY = (
    os.getenv("Thingsstack_API_KEY")
    or os.getenv("TTS_GATEWAY_API_KEY")
    or os.getenv("TTS_API_KEY")
    or ""
).strip()

WMC_BASE_URL = (os.getenv("WMC_BASE_URL") or "https://wmc.wanesy.com/api/v1").strip()
WMC_USERNAME = (os.getenv("WMC_USERNAME") or os.getenv("WANESY_USERNAME") or "").strip()
WMC_PASSWORD = os.getenv("WMC_PASSWORD") or os.getenv("WANESY_PASSWORD") or ""
WMC_ACTIVE_STATUSES = {"operational", "warning"}

REQUEST_TIMEOUT = 60


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Sucht gateway_eui in allen ThingsBoard-Devices und fragt den Gateway-Status bei The Things Stack ab."
    )
    parser.add_argument("--key", default="gateway_eui", help="Telemetrie-Key (Standard: gateway_eui)")
    parser.add_argument(
        "--customer-id",
        default="",
        metavar="UUID",
        help="Nur Devices dieses Customers (Standard: alle Tenant-Devices)",
    )
    parser.add_argument(
        "--details",
        action="store_true",
        help="Zusätzlich jedes Device mit seiner Gateway-EUI ausgeben",
    )
    parser.add_argument("--output", choices=["text", "csv", "json"], default="text", help="Ausgabeformat (Standard: text)")
    parser.add_argument("--output-file", "-o", default="", help="Ausgabedatei. Standard: Stdout")
    parser.add_argument("--page-size", type=int, default=500, help="Seitengröße der ThingsBoard-Abfrage (Standard: 500)")
    parser.add_argument("--max-pages", type=int, default=0, help="Max. Seiten (0 = alle)")
    parser.add_argument("--thingboard-url", default=THINGBOARD_URL, help="ThingsBoard Base-URL")
    parser.add_argument("--thingboard-username", default=THINGBOARD_USERNAME, help="ThingsBoard Benutzer")
    parser.add_argument("--thingboard-password", default=THINGBOARD_PASSWORD, help="ThingsBoard Passwort")
    parser.add_argument("--tts-base-url", default=TTS_BASE_URL, help="The Things Stack Base-URL")
    parser.add_argument("--tts-api-key", default=TTS_API_KEY, help="API-Key mit Gateway-Leserecht")
    parser.add_argument("--wmc-base-url", default=WMC_BASE_URL, help="Wanesy-API-Basis (Standard: https://wmc.wanesy.com/api/v1)")
    parser.add_argument("--wmc-username", default=WMC_USERNAME, help="Wanesy-Benutzer (Standard: WMC_USERNAME)")
    parser.add_argument("--wmc-password", default=WMC_PASSWORD, help="Wanesy-Passwort (Standard: WMC_PASSWORD)")
    parser.add_argument(
        "--no-reboot",
        action="store_true",
        help="Wanesy-Status nur ausgeben, keinen Reboot senden",
    )
    parser.add_argument("--no-verify-ssl", action="store_true", help="SSL-Zertifikat von ThingsBoard nicht prüfen")
    return parser.parse_args()


def normalize_base_url(url: str) -> str:
    return (url or "").strip().rstrip("/")


def normalize_wmc_base(url: str) -> str:
    """Ergänzt /api/v1, falls nur die Cockpit-Adresse gesetzt ist."""
    base = normalize_base_url(url) or "https://wmc.wanesy.com"
    if base.endswith("/api/v1"):
        return base
    if base.endswith("/api"):
        return base + "/v1"
    return base + "/api/v1"


def normalize_eui(value: str) -> Optional[str]:
    text = (value or "").strip().upper()
    text = re.sub(r"^EUI[\s:\-]*", "", text)
    compact = re.sub(r"[\s:\-]", "", text)
    if re.fullmatch(r"[0-9A-F]{16}", compact):
        return compact
    return None


def parse_euis(value: Any) -> List[str]:
    """Normalisiert einen Telemetriewert zu eindeutigen Gateway-EUIs."""
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        if text[0] in "[{":
            try:
                value = json.loads(text)
            except ValueError:
                pass
    if isinstance(value, dict):
        value = value.get("gateway_eui") or value.get("eui") or list(value.values())
    if isinstance(value, list):
        out: List[str] = []
        for item in value:
            out.extend(parse_euis(item))
        return list(dict.fromkeys(out))
    found: List[str] = []
    for part in re.split(r"[,;|\s]+", str(value).strip()):
        eui = normalize_eui(part)
        if eui and eui not in found:
            found.append(eui)
    return found


def format_tb_ts(ts: Any) -> str:
    try:
        millis = int(ts)
    except (TypeError, ValueError):
        return ""
    if millis <= 0:
        return ""
    try:
        return datetime.fromtimestamp(millis / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except (OverflowError, OSError, ValueError):
        return ""


def format_iso(value: Any) -> str:
    if not value:
        return ""
    text = str(value).strip()
    # Things Stack liefert Nanosekunden; fromisoformat akzeptiert höchstens Mikrosekunden.
    text = re.sub(r"(\.\d{6})\d+", r"\1", text)
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone()
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return str(value)


def tb_login(session: requests.Session, base_url: str, username: str, password: str) -> None:
    response = session.post(
        f"{base_url}/api/auth/login",
        json={"username": username, "password": password},
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code != 200:
        raise RuntimeError(f"ThingsBoard-Login fehlgeschlagen: HTTP {response.status_code} - {response.text}")
    token = (response.json() or {}).get("token")
    if not token:
        raise RuntimeError("ThingsBoard-Login ohne Token")
    session.headers["X-Authorization"] = f"Bearer {token}"


def latest_value(latest: Dict[str, Any], group: str, key: str) -> Tuple[str, int]:
    entry = ((latest.get(group) or {}).get(key) or {})
    value = entry.get("value")
    if value in (None, ""):
        return "", 0
    try:
        ts = int(entry.get("ts") or 0)
    except (TypeError, ValueError):
        ts = 0
    return str(value), ts


def query_devices(
    session: requests.Session,
    base_url: str,
    key: str,
    page_size: int,
    max_pages: int,
    customer_id: str,
) -> Tuple[List[Dict[str, Any]], int]:
    """Liefert Devices mit gesetzter gateway_eui und die Gesamtzahl geprüfter Devices."""
    url = f"{base_url}/api/entitiesQuery/find"
    devices: List[Dict[str, Any]] = []
    page = 0
    scanned = 0
    page_size = max(1, page_size)

    while True:
        if max_pages > 0 and page >= max_pages:
            break
        body: Dict[str, Any] = {
            "entityFilter": {
                "type": "entityType",
                "resolveMultiple": True,
                "entityType": "DEVICE",
            },
            "pageLink": {
                "page": page,
                "pageSize": page_size,
                "sortOrder": {
                    "key": {"type": "ENTITY_FIELD", "key": "name"},
                    "direction": "ASC",
                },
            },
            "entityFields": [
                {"type": "ENTITY_FIELD", "key": "name"},
                {"type": "ENTITY_FIELD", "key": "label"},
                {"type": "ENTITY_FIELD", "key": "type"},
            ],
            "latestValues": [
                {"type": "TIME_SERIES", "key": key},
                {"type": "ATTRIBUTE", "key": key},
            ],
        }
        if customer_id:
            body["keyFilters"] = [
                {
                    "key": {"type": "ENTITY_FIELD", "key": "customerId"},
                    "valueType": "STRING",
                    "predicate": {
                        "type": "STRING",
                        "operation": "EQUAL",
                        "value": {"defaultValue": customer_id},
                    },
                }
            ]

        response = session.post(url, json=body, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            raise RuntimeError(f"ThingsBoard-Abfrage: HTTP {response.status_code} - {response.text[:500]}")
        payload = response.json() or {}
        rows = payload.get("data") or []
        scanned += len(rows)

        for row in rows:
            entity = row.get("entityId") or {}
            latest = row.get("latest") or {}
            fields = latest.get("ENTITY_FIELD") or {}
            ts_value, ts_time = latest_value(latest, "TIME_SERIES", key)
            attr_value, attr_time = latest_value(latest, "ATTRIBUTE", key)
            raw_value, seen_ts = (ts_value, ts_time) if ts_value else (attr_value, attr_time)
            euis = parse_euis(raw_value)
            if not euis:
                continue
            devices.append({
                "device_id": (entity.get("id") or "").strip(),
                "name": ((fields.get("name") or {}).get("value") or "").strip(),
                "label": ((fields.get("label") or {}).get("value") or "").strip(),
                "type": ((fields.get("type") or {}).get("value") or "").strip(),
                "gateway_euis": euis,
                "last_seen_ts": seen_ts,
            })

        print(
            f"   Seite {page + 1}: {len(rows)} Devices, bisher {len(devices)} mit {key}",
            file=sys.stderr,
        )
        if not payload.get("hasNext", False):
            break
        page += 1

    return devices, scanned


def list_tts_gateways(session: requests.Session, base_url: str) -> Dict[str, Dict[str, Any]]:
    """Indexiert alle sichtbaren Gateways nach normalisierter EUI."""
    by_eui: Dict[str, Dict[str, Any]] = {}
    page_token = ""
    while True:
        params: Dict[str, Any] = {"limit": 100, "field_mask": "name,description"}
        if page_token:
            params["page"] = page_token
        response = session.get(f"{base_url}/api/v3/gateways", params=params, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            raise RuntimeError(f"ThingsStack-Gateway-Liste: HTTP {response.status_code} - {response.text[:500]}")
        payload = response.json() or {}
        for gateway in payload.get("gateways") or []:
            ids = gateway.get("ids") or {}
            eui = normalize_eui(ids.get("eui") or ids.get("gateway_id") or "")
            if eui:
                by_eui[eui] = gateway
        page_token = payload.get("next_page_token") or ""
        if not page_token:
            break
    return by_eui


def fetch_gateway_record(session: requests.Session, base_url: str, eui: str) -> Optional[Dict[str, Any]]:
    gateway_id = f"eui-{eui.lower()}"
    response = session.get(f"{base_url}/api/v3/gateways/{gateway_id}", timeout=REQUEST_TIMEOUT)
    if response.status_code == 200:
        return response.json() or {}
    return None


def fetch_connection_stats(session: requests.Session, base_url: str, gateway_id: str) -> Dict[str, Any]:
    response = session.get(
        f"{base_url}/api/v3/gs/gateways/{gateway_id}/connection/stats",
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code == 200:
        data = response.json() or {}
        remote = data.get("gateway_remote_address") or {}
        return {
            "tts_status": "online",
            "protocol": data.get("protocol") or "",
            "connected_at": format_iso(data.get("connected_at")),
            "last_status_at": format_iso(data.get("last_status_received_at")),
            "last_uplink_at": format_iso(data.get("last_uplink_received_at")),
            "last_downlink_at": format_iso(data.get("last_downlink_received_at")),
            "uplink_count": data.get("uplink_count") or "",
            "downlink_count": data.get("downlink_count") or "",
            "remote_ip": remote.get("ip") or "",
            "detail": "",
        }
    detail = ""
    try:
        detail = (response.json() or {}).get("message") or response.text
    except ValueError:
        detail = response.text
    if response.status_code == 404 and "not_connected" in detail:
        return empty_stats("offline", "nicht verbunden")
    if response.status_code == 404:
        return empty_stats("not_found", "Gateway auf dem Gateway Server nicht gefunden")
    return empty_stats("error", f"HTTP {response.status_code}: {detail[:180]}")


def empty_stats(status: str, detail: str) -> Dict[str, Any]:
    return {
        "tts_status": status,
        "protocol": "",
        "connected_at": "",
        "last_status_at": "",
        "last_uplink_at": "",
        "last_downlink_at": "",
        "uplink_count": "",
        "downlink_count": "",
        "remote_ip": "",
        "detail": detail,
    }


def empty_wmc(action: str = "", detail: str = "") -> Dict[str, str]:
    return {
        "wmc_status": "",
        "wmc_name": "",
        "wmc_customer_id": "",
        "wmc_last_update": "",
        "wmc_action": action,
        "wmc_detail": detail,
    }


def wmc_login(session: requests.Session, base_url: str, username: str, password: str) -> None:
    credentials = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
    response = session.post(
        f"{base_url}/users/token",
        headers={"Authorization": f"Basic {credentials}", "Accept": "application/json"},
        timeout=REQUEST_TIMEOUT,
    )
    if response.status_code not in (200, 201):
        body = response.text or ""
        if "<html" in body.lower() or "<!doctype" in body.lower():
            detail = "HTML-Sperrseite statt API. Die Basis-URL muss auf /api/v1 enden."
        else:
            detail = " ".join(body.split())[:180]
        raise RuntimeError(f"Wanesy-Login fehlgeschlagen: HTTP {response.status_code} - {detail}")
    token = ((response.json() or {}).get("data") or {}).get("AccessToken")
    if not token:
        raise RuntimeError("Wanesy-Login ohne AccessToken")
    session.headers["Authorization"] = f"Bearer {token}"
    session.headers["Accept"] = "application/json"
    session.headers["Content-Type"] = "application/json"


def wmc_get_paged(session: requests.Session, url: str) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    offset = 0
    limit = 100
    while True:
        response = session.get(url, params={"offset": offset, "limit": limit}, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            raise RuntimeError(f"GET {url}: HTTP {response.status_code} - {response.text[:300]}")
        payload = response.json() or {}
        rows = payload.get("data") or []
        items.extend(rows)
        offset += len(rows)
        total = (payload.get("metadata") or {}).get("totalCount")
        if not rows or len(rows) < limit or (total is not None and offset >= int(total)):
            break
    return items


def list_wmc_gateways(session: requests.Session, base_url: str) -> Dict[str, Dict[str, Any]]:
    """Liest Gateways je Customer. GET /gateways ist für Customer-Admins gesperrt."""
    customers = wmc_get_paged(session, f"{base_url}/customers")
    if not customers:
        raise RuntimeError("Keine Wanesy-Customers für diesen Benutzer sichtbar")
    indexed: Dict[str, Dict[str, Any]] = {}
    for customer in customers:
        customer_id = customer.get("id")
        customer_name = customer.get("name") or customer_id
        print(f"   Customer {customer_name} ({customer_id})", file=sys.stderr)
        for gateway in wmc_get_paged(session, f"{base_url}/customers/{customer_id}/gateways"):
            eui = normalize_eui(gateway.get("gwEui") or "")
            if eui:
                indexed[eui] = gateway
    return indexed


def find_reboot_action(session: requests.Session, base_url: str) -> Tuple[int, str]:
    definitions = wmc_get_paged(session, f"{base_url}/actions-definition")

    def rank(item: Dict[str, Any]) -> int:
        name = (item.get("name") or "").strip().lower()
        description = (item.get("description") or "").lower()
        if name == "reboot":
            return 0
        if "reboot" in name:
            return 1
        if "reboot" in description:
            return 2
        return 9

    candidates = [
        item for item in definitions
        if item.get("isEnabled", True) and item.get("scopeGateway", True) and rank(item) < 9 and item.get("id") is not None
    ]
    if not candidates:
        names = ", ".join(sorted({(item.get("name") or "") for item in definitions if item.get("name")}))
        raise RuntimeError(f"Keine Reboot-Aktion in Wanesy gefunden. Vorhandene Aktionen: {names or '—'}")
    chosen = sorted(candidates, key=rank)[0]
    mandatory = [
        (param.get("name") or "?")
        for param in (chosen.get("parametersDefinition") or [])
        if param.get("mandatory", True)
    ]
    if mandatory:
        raise RuntimeError(
            f"Reboot-Aktion '{chosen.get('name')}' verlangt Parameter: {', '.join(mandatory)}"
        )
    return int(chosen["id"]), (chosen.get("name") or "reboot")


def send_wmc_reboot(
    session: requests.Session, base_url: str, customer_id: Any, eui: str, action_id: int
) -> Dict[str, Any]:
    response = session.post(
        f"{base_url}/customers/{customer_id}/gateways/{eui}/actions",
        json={"actionId": action_id, "actionParameters": {}},
        timeout=REQUEST_TIMEOUT,
    )
    try:
        payload = response.json()
    except ValueError:
        payload = {}
    if response.status_code not in (200, 201):
        message = payload.get("message") or payload.get("detail") or response.text[:300]
        raise RuntimeError(f"HTTP {response.status_code}: {message}")
    return (payload.get("data") or payload)


def apply_wmc_checks(
    report: List[Dict[str, Any]],
    session: requests.Session,
    base_url: str,
    send_reboot: bool,
) -> None:
    offline = [row for row in report if row.get("tts_status") == "offline"]
    for row in report:
        if row.get("tts_status") != "offline":
            row.update(empty_wmc())
    if not offline:
        return

    print(f"Prüfe {len(offline)} offline Gateways in Wanesy ...", file=sys.stderr)
    gateways = list_wmc_gateways(session, base_url)
    print(f"   {len(gateways)} Gateways in Wanesy sichtbar", file=sys.stderr)
    reboot_action: Optional[Tuple[int, str]] = None

    for row in offline:
        eui = row["gateway_eui"]
        gateway = gateways.get(eui)
        if not gateway:
            row.update(empty_wmc("nicht in Wanesy", "Gateway-EUI ist im Wanesy Management Cockpit nicht vorhanden"))
            print(f"   {eui}: nicht in Wanesy", file=sys.stderr)
            continue
        status = ((gateway.get("connectionStatus") or {}).get("status") or "").strip()
        last_update = format_iso((gateway.get("connectionStatus") or {}).get("lastUpdateTime"))
        row.update({
            "wmc_status": status,
            "wmc_name": (gateway.get("name") or "").strip(),
            "wmc_customer_id": str(gateway.get("customerId") or ""),
            "wmc_last_update": last_update,
            "wmc_action": "",
            "wmc_detail": "",
        })
        if status.lower() not in WMC_ACTIVE_STATUSES:
            row["wmc_action"] = "inaktiv"
            row["wmc_detail"] = f"Wanesy-Status {status or 'unbekannt'}, zuletzt {last_update or '—'}"
            print(f"   {eui}: Wanesy {status or 'unbekannt'} (inaktiv)", file=sys.stderr)
            continue
        if not send_reboot:
            row["wmc_action"] = "aktiv, Reboot übersprungen"
            row["wmc_detail"] = f"Wanesy-Status {status}. Reboot nicht gesendet (--no-reboot)."
            print(f"   {eui}: Wanesy {status}, Reboot übersprungen", file=sys.stderr)
            continue
        try:
            if reboot_action is None:
                reboot_action = find_reboot_action(session, base_url)
                print(f"   Reboot-Aktion: {reboot_action[1]} (id {reboot_action[0]})", file=sys.stderr)
            created = send_wmc_reboot(session, base_url, gateway.get("customerId"), eui, reboot_action[0])
            action_status = created.get("status") or ""
            action_request_id = created.get("id") or ""
            row["wmc_action"] = "reboot gesendet"
            row["wmc_detail"] = f"Aktion {action_request_id} ({reboot_action[1]}), Status {action_status}"
            print(f"   {eui}: Reboot gesendet, Aktion {action_request_id} {action_status}", file=sys.stderr)
        except Exception as exc:
            text = str(exc)
            if "already running" in text.lower():
                row["wmc_action"] = "reboot läuft bereits"
                row["wmc_detail"] = "In Wanesy läuft bereits eine Aktion, kein weiterer Reboot gesendet."
                print(f"   {eui}: Reboot läuft bereits", file=sys.stderr)
            else:
                row["wmc_action"] = "fehler"
                row["wmc_detail"] = text
                print(f"   {eui}: Reboot fehlgeschlagen: {exc}", file=sys.stderr)


def build_gateway_report(
    devices: List[Dict[str, Any]],
    tts_session: requests.Session,
    tts_url: str,
    known_gateways: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, Any]] = {}
    for device in devices:
        for eui in device["gateway_euis"]:
            entry = grouped.setdefault(eui, {
                "gateway_eui": eui,
                "device_count": 0,
                "last_seen_ts": 0,
                "devices": [],
            })
            entry["device_count"] += 1
            entry["last_seen_ts"] = max(entry["last_seen_ts"], int(device.get("last_seen_ts") or 0))
            entry["devices"].append(device)

    report: List[Dict[str, Any]] = []
    ordered = sorted(grouped.values(), key=lambda item: (-item["device_count"], item["gateway_eui"]))
    for index, entry in enumerate(ordered, 1):
        eui = entry["gateway_eui"]
        print(f"   [{index}/{len(ordered)}] {eui}", file=sys.stderr)
        gateway = known_gateways.get(eui) or fetch_gateway_record(tts_session, tts_url, eui)
        gateway_id = ((gateway or {}).get("ids") or {}).get("gateway_id") or ""
        if not gateway or not gateway_id:
            stats = empty_stats("not_found", "Gateway-EUI ist in The Things Stack nicht registriert")
            name = ""
        else:
            stats = fetch_connection_stats(tts_session, tts_url, gateway_id)
            name = (gateway.get("name") or "").strip()
        report.append({
            "gateway_eui": eui,
            "tts_gateway_id": gateway_id,
            "tts_name": name,
            "tb_device_count": entry["device_count"],
            "tb_last_seen": format_tb_ts(entry["last_seen_ts"]),
            "devices": entry["devices"],
            **stats,
            **empty_wmc(),
        })
    return report


def write_text(report: List[Dict[str, Any]], out, details: bool, scanned: int, with_eui: int) -> None:
    online = sum(1 for row in report if row["tts_status"] == "online")
    offline = sum(1 for row in report if row["tts_status"] == "offline")
    missing = sum(1 for row in report if row["tts_status"] == "not_found")
    print(f"Devices geprüft: {scanned}", file=out)
    print(f"Devices mit Gateway-EUI: {with_eui}", file=out)
    print(f"Gateway-EUIs: {len(report)}  |  online: {online}  |  offline: {offline}  |  nicht in ThingsStack: {missing}", file=out)
    print(file=out)
    for row in report:
        print(
            f"{row['gateway_eui']}  {row['tts_status']:10}  "
            f"{row['tb_device_count']:5} Devices  "
            f"uplink {row['last_uplink_at'] or '—'}  "
            f"status {row['last_status_at'] or '—'}  "
            f"{row['detail']}",
            file=out,
        )
        if row.get("wmc_action"):
            print(
                f"   Wanesy: {row['wmc_status'] or '—'}  {row['wmc_name'] or ''}  "
                f"{row['wmc_action']}  {row['wmc_detail']}",
                file=out,
            )
        if details:
            for device in row["devices"]:
                print(f"   - {device['name'] or '—'}  {device['label'] or ''}  {device['type'] or ''}", file=out)
    inactive = [row for row in report if row.get("wmc_action") == "inaktiv" or row.get("wmc_action") == "nicht in Wanesy"]
    rebooted = [row for row in report if row.get("wmc_action") in ("reboot gesendet", "reboot läuft bereits")]
    if inactive or rebooted:
        print(file=out)
    if rebooted:
        print("Wanesy-Reboot:", file=out)
        for row in rebooted:
            print(f"   {row['gateway_eui']}  {row['wmc_name'] or '—'}  {row['wmc_detail']}", file=out)
    if inactive:
        print("Kein Reboot, in Wanesy nicht aktiv:", file=out)
        for row in inactive:
            print(
                f"   {row['gateway_eui']}  {row['wmc_name'] or '—'}  "
                f"{row['wmc_status'] or row['wmc_action']}  zuletzt {row['wmc_last_update'] or '—'}  "
                f"({row['tb_device_count']} Devices)",
                file=out,
            )
    print(file=out)


def csv_gateway_row(row: Dict[str, Any]) -> List[Any]:
    return [
        row["gateway_eui"],
        row["tts_gateway_id"],
        row["tts_name"],
        row["tts_status"],
        row["protocol"],
        row["connected_at"],
        row["last_status_at"],
        row["last_uplink_at"],
        row["last_downlink_at"],
        row["uplink_count"],
        row["downlink_count"],
        row["remote_ip"],
        row["tb_device_count"],
        row["tb_last_seen"],
        row["detail"],
        row.get("wmc_status", ""),
        row.get("wmc_name", ""),
        row.get("wmc_last_update", ""),
        row.get("wmc_action", ""),
        row.get("wmc_detail", ""),
    ]


GATEWAY_CSV_HEADER = [
    "gateway_eui",
    "tts_gateway_id",
    "tts_name",
    "tts_status",
    "protocol",
    "connected_at",
    "last_status_at",
    "last_uplink_at",
    "last_downlink_at",
    "uplink_count",
    "downlink_count",
    "remote_ip",
    "tb_device_count",
    "tb_last_seen",
    "detail",
    "wmc_status",
    "wmc_name",
    "wmc_last_update",
    "wmc_action",
    "wmc_detail",
]


def write_csv(report: List[Dict[str, Any]], out, details: bool) -> None:
    writer = csv.writer(out, delimiter=";")
    if not details:
        writer.writerow(GATEWAY_CSV_HEADER)
        for row in report:
            writer.writerow(csv_gateway_row(row))
        return
    writer.writerow([
        "gateway_eui", "tts_status", "tts_gateway_id", "device_id", "device_name",
        "device_label", "device_type", "tb_last_seen", "last_uplink_at", "detail",
    ])
    for row in report:
        for device in row["devices"]:
            writer.writerow([
                row["gateway_eui"],
                row["tts_status"],
                row["tts_gateway_id"],
                device["device_id"],
                device["name"],
                device["label"],
                device["type"],
                format_tb_ts(device.get("last_seen_ts")),
                row["last_uplink_at"],
                row["detail"],
            ])


def report_for_json(report: List[Dict[str, Any]], details: bool) -> List[Dict[str, Any]]:
    if details:
        return report
    cleaned = []
    for row in report:
        item = dict(row)
        item.pop("devices", None)
        cleaned.append(item)
    return cleaned


def main():
    args = parse_arguments()
    tb_url = normalize_base_url(args.thingboard_url)
    tts_url = normalize_base_url(args.tts_base_url)
    tb_user = (args.thingboard_username or "").strip()
    tts_key = (args.tts_api_key or "").strip()

    if not tb_url or not tb_user:
        print("THINGBOARD_URL und THINGBOARD_USERNAME fehlen.", file=sys.stderr)
        sys.exit(1)
    if not tts_url or not tts_key:
        print("TTS_BASE_URL und Thingsstack_API_KEY (oder TTS_API_KEY) fehlen.", file=sys.stderr)
        sys.exit(1)

    tb_password = args.thingboard_password
    if not tb_password:
        tb_password = getpass.getpass("ThingsBoard Passwort: ")

    tb_session = requests.Session()
    tb_session.verify = TB_VERIFY_SSL and not args.no_verify_ssl
    tts_session = requests.Session()
    tts_session.headers["Authorization"] = f"Bearer {tts_key}"
    tts_session.headers["Accept"] = "application/json"

    try:
        tb_login(tb_session, tb_url, tb_user, tb_password)
    except Exception as exc:
        print(f"ThingsBoard-Login: {exc}", file=sys.stderr)
        sys.exit(1)

    print("Suche gateway_eui in ThingsBoard ...", file=sys.stderr)
    try:
        devices, scanned = query_devices(
            tb_session,
            tb_url,
            args.key,
            args.page_size,
            args.max_pages,
            (args.customer_id or "").strip(),
        )
    except Exception as exc:
        print(f"ThingsBoard: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"Frage Gateway-Status bei The Things Stack ab ({tts_url}) ...", file=sys.stderr)
    try:
        known = list_tts_gateways(tts_session, tts_url)
    except Exception as exc:
        print(f"ThingsStack: {exc}", file=sys.stderr)
        sys.exit(1)
    if not known:
        print(
            "Warnung: Die Gateway-Liste ist leer. Application-API-Keys sehen keine Gateways. "
            "Thingsstack_API_KEY mit Gateway-Leserecht verwenden.",
            file=sys.stderr,
        )
    else:
        print(f"   {len(known)} Gateways in The Things Stack sichtbar", file=sys.stderr)

    try:
        report = build_gateway_report(devices, tts_session, tts_url, known)
    except Exception as exc:
        print(f"ThingsStack-Status: {exc}", file=sys.stderr)
        sys.exit(1)

    offline_rows = [row for row in report if row.get("tts_status") == "offline"]
    if offline_rows:
        wmc_user = (args.wmc_username or "").strip()
        wmc_password = args.wmc_password
        if not wmc_user or not wmc_password:
            if sys.stdin.isatty():
                if not wmc_user:
                    wmc_user = input("Wanesy Benutzer: ").strip()
                if not wmc_password:
                    wmc_password = getpass.getpass("Wanesy Passwort: ")
        if not wmc_user or not wmc_password:
            print(
                "WMC_USERNAME und WMC_PASSWORD fehlen. Offline-Gateways werden nicht in Wanesy geprüft.",
                file=sys.stderr,
            )
            for row in offline_rows:
                row.update(empty_wmc("nicht geprüft", "WMC_USERNAME oder WMC_PASSWORD fehlt"))
        else:
            wmc_session = requests.Session()
            try:
                wmc_base = normalize_wmc_base(args.wmc_base_url)
                wmc_login(wmc_session, wmc_base, wmc_user, wmc_password)
                apply_wmc_checks(report, wmc_session, wmc_base, not args.no_reboot)
            except Exception as exc:
                print(f"Wanesy: {exc}", file=sys.stderr)
                for row in offline_rows:
                    if not row.get("wmc_action"):
                        row.update(empty_wmc("fehler", str(exc)))

    out_file = open(args.output_file, "w", encoding="utf-8", newline="") if args.output_file else sys.stdout
    try:
        if args.output == "csv":
            write_csv(report, out_file, args.details)
        elif args.output == "json":
            json.dump(
                {
                    "scanned_devices": scanned,
                    "devices_with_gateway_eui": len(devices),
                    "gateways": report_for_json(report, args.details),
                },
                out_file,
                indent=2,
                ensure_ascii=False,
            )
            if out_file is not sys.stdout:
                out_file.write("\n")
        else:
            write_text(report, out_file, args.details, scanned, len(devices))
    finally:
        if args.output_file and out_file is not sys.stdout:
            out_file.close()

    if args.output_file or args.output != "text":
        online = sum(1 for row in report if row["tts_status"] == "online")
        offline = sum(1 for row in report if row["tts_status"] == "offline")
        missing = sum(1 for row in report if row["tts_status"] == "not_found")
        target = args.output_file or "Stdout"
        rebooted = sum(1 for row in report if row.get("wmc_action") == "reboot gesendet")
        inactive = sum(1 for row in report if row.get("wmc_action") in ("inaktiv", "nicht in Wanesy"))
        print(
            f"Fertig: {len(devices)} Devices mit Gateway-EUI, {len(report)} Gateways "
            f"(online {online}, offline {offline}, nicht gefunden {missing}, "
            f"Wanesy-Reboot {rebooted}, Wanesy inaktiv {inactive}). Ausgabe: {target}",
            file=sys.stderr,
        )


if __name__ == "__main__":
    main()
