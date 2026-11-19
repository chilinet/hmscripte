#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Customer Structure Lister (ThingsBoard)
=======================================
Gibt die komplette Asset-Struktur eines Customers als Baum aus und listet an
jedem Knoten die zugeordneten Devices mit Device Type, DevEUI (Name) und Label.

Die Daten werden live aus ThingsBoard geladen:
  - Customer-Assets:   /api/customer/{id}/assets
  - Customer-Devices:  /api/customer/{id}/devices
  - Hierarchie:        /api/relations  (Relation "Contains": from = Parent, to = Kind)

Verwendung:
  python customer_structure_thingsboard.py <customer-id>
  python customer_structure_thingsboard.py --name "Taunus Residenz"
  python customer_structure_thingsboard.py <customer-id> --output output/struktur.txt
  python customer_structure_thingsboard.py <customer-id> --csv output/struktur.csv

.env: THINGBOARD_USERNAME, THINGBOARD_PASSWORD (wie die übrigen Skripte)
"""

from __future__ import annotations

import argparse
import csv
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

from heatmanager_common.config import (
    THINGSBOARD_BASE_URL,
    THINGSBOARD_USERNAME,
    THINGSBOARD_PASSWORD,
)

TOKEN: str = ""
SESSION = requests.Session()

# Parallele Verbindungen für das Laden der Relationen (ein Request pro Asset).
RELATION_WORKERS = 16


def _auth_headers() -> Dict[str, str]:
    return {
        "X-Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
    }


def login_to_thingsboard() -> bool:
    """Loggt sich bei ThingsBoard ein und speichert den JWT Token global."""
    global TOKEN
    url = f"{THINGSBOARD_BASE_URL}/api/auth/login"
    login_data = {"username": THINGSBOARD_USERNAME, "password": THINGSBOARD_PASSWORD}
    try:
        response = SESSION.post(
            url, json=login_data, headers={"Content-Type": "application/json"}, timeout=30
        )
        if response.status_code == 200:
            TOKEN = response.json().get("token", "")
            print("✅ Login erfolgreich")
            return True
        print(f"❌ Login fehlgeschlagen: {response.status_code} - {response.text}")
        return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Fehler beim Login: {e}")
        return False


def _entity_id(entity: Dict[str, Any]) -> Optional[str]:
    """Extrahiert die UUID aus einem ThingsBoard EntityId-Objekt oder String."""
    eid = entity.get("id")
    if isinstance(eid, dict):
        return eid.get("id")
    if isinstance(eid, str):
        return eid
    return None


def resolve_customer_by_name(name: str) -> Optional[Dict[str, Any]]:
    """Sucht einen Customer anhand des Titels (textSearch)."""
    url = f"{THINGSBOARD_BASE_URL}/api/customers"
    page = 0
    while True:
        params = {"pageSize": 100, "page": page, "textSearch": name}
        response = SESSION.get(url, headers=_auth_headers(), params=params, timeout=30)
        if response.status_code != 200:
            print(f"❌ Fehler bei Customer-Suche: {response.status_code} - {response.text}")
            return None
        data = response.json()
        for customer in data.get("data", []):
            if customer.get("title", "").strip().lower() == name.strip().lower():
                return customer
        if not data.get("hasNext", False):
            break
        page += 1
    # Kein exakter Treffer: ersten Teiltreffer zurückliefern
    response = SESSION.get(
        url, headers=_auth_headers(), params={"pageSize": 100, "page": 0, "textSearch": name}, timeout=30
    )
    if response.status_code == 200:
        candidates = response.json().get("data", [])
        if candidates:
            print(f"⚠️  Kein exakter Name-Treffer, verwende '{candidates[0].get('title')}'")
            return candidates[0]
    return None


def get_customer_info(customer_id: str) -> Optional[Dict[str, Any]]:
    url = f"{THINGSBOARD_BASE_URL}/api/customer/{customer_id}"
    response = SESSION.get(url, headers=_auth_headers(), timeout=30)
    if response.status_code == 200:
        return response.json()
    print(f"⚠️  Customer-Info nicht abrufbar: {response.status_code}")
    return None


def _get_all_pages(url: str) -> List[Dict[str, Any]]:
    """Lädt alle Seiten einer paginierten ThingsBoard-Liste."""
    items: List[Dict[str, Any]] = []
    page = 0
    while True:
        params = {"pageSize": 1000, "page": page}
        response = SESSION.get(url, headers=_auth_headers(), params=params, timeout=30)
        if response.status_code != 200:
            print(f"❌ Fehler beim Laden von {url}: {response.status_code} - {response.text}")
            break
        data = response.json()
        items.extend(data.get("data", []))
        if not data.get("hasNext", False):
            break
        page += 1
    return items


def get_customer_assets(customer_id: str) -> List[Dict[str, Any]]:
    return _get_all_pages(f"{THINGSBOARD_BASE_URL}/api/customer/{customer_id}/assets")


def get_customer_devices(customer_id: str) -> List[Dict[str, Any]]:
    return _get_all_pages(f"{THINGSBOARD_BASE_URL}/api/customer/{customer_id}/devices")


def get_outgoing_relations(from_id: str) -> List[Dict[str, Any]]:
    """Holt alle ausgehenden Relationen eines Assets (Parent -> Kind)."""
    url = f"{THINGSBOARD_BASE_URL}/api/relations"
    params = {"fromId": from_id, "fromType": "ASSET"}
    try:
        response = SESSION.get(url, headers=_auth_headers(), params=params, timeout=30)
    except requests.exceptions.RequestException:
        return []
    if response.status_code != 200:
        return []
    relations = response.json()
    return relations if isinstance(relations, list) else []


def build_structure(
    customer_id: str,
) -> Tuple[
    Dict[str, Dict[str, Any]],
    Dict[str, Dict[str, Any]],
    Dict[str, List[str]],
    Dict[str, List[str]],
    List[str],
]:
    """
    Baut die Customer-Struktur auf.

    Liefert:
      assets_by_id       : asset_uuid -> asset-objekt
      devices_by_id      : device_uuid -> device-objekt
      asset_children     : asset_uuid -> [kind_asset_uuid, ...]
      asset_devices      : asset_uuid -> [device_uuid, ...]
      root_asset_ids     : [asset_uuid, ...] (Assets ohne Eltern-Asset im Customer)
    """
    print("🔍 Lade Customer-Assets ...")
    assets = get_customer_assets(customer_id)
    print(f"   📦 {len(assets)} Assets")

    print("🔍 Lade Customer-Devices ...")
    devices = get_customer_devices(customer_id)
    print(f"   📱 {len(devices)} Devices")

    assets_by_id: Dict[str, Dict[str, Any]] = {}
    for asset in assets:
        aid = _entity_id(asset)
        if aid:
            assets_by_id[aid] = asset

    devices_by_id: Dict[str, Dict[str, Any]] = {}
    for device in devices:
        did = _entity_id(device)
        if did:
            devices_by_id[did] = device

    asset_children: Dict[str, List[str]] = {}
    asset_devices: Dict[str, List[str]] = {}
    all_child_assets: set[str] = set()

    print(f"🔍 Lade Relationen (Hierarchie) – {RELATION_WORKERS} parallele Verbindungen ...")
    total = len(assets_by_id)
    done = 0
    with ThreadPoolExecutor(max_workers=RELATION_WORKERS) as executor:
        future_to_aid = {
            executor.submit(get_outgoing_relations, aid): aid for aid in assets_by_id
        }
        for future in as_completed(future_to_aid):
            aid = future_to_aid[future]
            relations = future.result()
            child_assets: List[str] = []
            child_devices: List[str] = []
            for rel in relations:
                to_entity = rel.get("to") or {}
                etype = to_entity.get("entityType")
                cid = _entity_id(to_entity)
                if not cid:
                    continue
                if etype == "ASSET":
                    if cid in assets_by_id and cid not in child_assets:
                        child_assets.append(cid)
                        all_child_assets.add(cid)
                elif etype == "DEVICE":
                    if cid not in child_devices:
                        child_devices.append(cid)
            asset_children[aid] = child_assets
            asset_devices[aid] = child_devices
            done += 1
            if done % 100 == 0 or done == total:
                print(f"   … {done}/{total} Assets verarbeitet")

    root_asset_ids = [aid for aid in assets_by_id if aid not in all_child_assets]
    return assets_by_id, devices_by_id, asset_children, asset_devices, root_asset_ids


# Klartext-Bezeichnungen für Device Types
DEVICE_TYPE_PLAIN = {
    "dnt-LW-WSCI": "Fenstersensor",
    "dnt-LW-eTRV-C": "Thermostat",
}


def type_plain(device_type: str) -> str:
    """Liefert die Klartext-Bezeichnung zu einem Device Type (sonst leer)."""
    return DEVICE_TYPE_PLAIN.get(device_type, "")


def _device_label(device: Dict[str, Any]) -> str:
    label = device.get("label") or ""
    if not label:
        additional = device.get("additionalInfo") or {}
        label = additional.get("label") or ""
    return label or "-"


def render_structure(
    assets_by_id: Dict[str, Dict[str, Any]],
    devices_by_id: Dict[str, Dict[str, Any]],
    asset_children: Dict[str, List[str]],
    asset_devices: Dict[str, List[str]],
    root_asset_ids: List[str],
) -> Tuple[List[str], List[Dict[str, Any]]]:
    """
    Erzeugt die Baum-Textzeilen und parallel flache Datenzeilen.

    Liefert (text_lines, flat_rows). Jede flat_row enthält:
      path_parts : List[str]  (Hierarchie-Ebenen bis zum zugeordneten Asset)
      name       : Device Name / DevEUI
      type       : Device Type
      label      : Device Label
    """
    text_lines: List[str] = []
    flat_rows: List[Dict[str, Any]] = []
    assigned_devices: set[str] = set()

    def asset_caption(asset: Dict[str, Any]) -> str:
        name = asset.get("name", "?")
        label = asset.get("label") or ""
        atype = asset.get("type") or ""
        caption = f"{label} ({name})" if label else name
        if atype:
            caption += f"  [{atype}]"
        return caption

    def walk(asset_id: str, depth: int, path: List[str], stack: set[str]) -> None:
        asset = assets_by_id.get(asset_id)
        if not asset:
            return
        indent = "    " * depth
        text_lines.append(f"{indent}📁 {asset_caption(asset)}")

        current_path = path + [asset.get("label") or asset.get("name", "?")]

        device_ids = asset_devices.get(asset_id, [])
        child_ids = asset_children.get(asset_id, [])

        for did in device_ids:
            assigned_devices.add(did)
            device = devices_by_id.get(did)
            if device:
                dtype = device.get("type", "?")
                dname = device.get("name", "?")
                dlabel = _device_label(device)
            else:
                # Device nicht in Customer-Device-Liste (z. B. anderem Customer zugeordnet)
                dtype, dname, dlabel = "?", did, "-"
            dplain = type_plain(dtype)
            klartext_suffix = f"  | Klartext: {dplain}" if dplain else ""
            text_lines.append(
                f"{indent}    📱 {dname}  | Type: {dtype}{klartext_suffix}  | Label: {dlabel}"
            )
            flat_rows.append(
                {
                    "path_parts": list(current_path),
                    "name": dname,
                    "type": dtype,
                    "type_plain": dplain,
                    "label": dlabel,
                }
            )

        # Struktur ohne Devices ebenfalls erfassen: Blatt-Asset ohne Geräte
        # bekommt eine Zeile mit leeren Device-Spalten, damit der Pfad in
        # CSV/Excel sichtbar bleibt.
        if not device_ids and not child_ids:
            flat_rows.append(
                {
                    "path_parts": list(current_path),
                    "name": "",
                    "type": "",
                    "type_plain": "",
                    "label": "",
                }
            )

        for child_id in sorted(
            child_ids,
            key=lambda c: (assets_by_id.get(c, {}).get("label") or assets_by_id.get(c, {}).get("name") or ""),
        ):
            if child_id in stack:
                text_lines.append(f"{indent}    ⚠️  Zyklus übersprungen: {child_id}")
                continue
            walk(child_id, depth + 1, current_path, stack | {child_id})

    text_lines.append("📂 STRUKTUR:")
    for root_id in sorted(
        root_asset_ids,
        key=lambda c: (assets_by_id.get(c, {}).get("label") or assets_by_id.get(c, {}).get("name") or ""),
    ):
        walk(root_id, 0, [], {root_id})

    # Devices ohne Asset-Zuordnung
    orphan_ids = [did for did in devices_by_id if did not in assigned_devices]
    if orphan_ids:
        text_lines.append("")
        text_lines.append(f"📦 DEVICES OHNE ASSET-ZUORDNUNG ({len(orphan_ids)}):")
        for did in orphan_ids:
            device = devices_by_id[did]
            dtype = device.get("type", "?")
            dname = device.get("name", "?")
            dlabel = _device_label(device)
            dplain = type_plain(dtype)
            klartext_suffix = f"  | Klartext: {dplain}" if dplain else ""
            text_lines.append(f"    📱 {dname}  | Type: {dtype}{klartext_suffix}  | Label: {dlabel}")
            flat_rows.append(
                {
                    "path_parts": ["(ohne Zuordnung)"],
                    "name": dname,
                    "type": dtype,
                    "type_plain": dplain,
                    "label": dlabel,
                }
            )

    return text_lines, flat_rows


def write_csv(flat_rows: List[Dict[str, Any]], path: str) -> None:
    fieldnames = [
        "Pfad",
        "Device Name (DevEUI)",
        "Device Type",
        "Typ (Klartext)",
        "Device Label",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in flat_rows:
            writer.writerow(
                {
                    "Pfad": " / ".join(row["path_parts"]),
                    "Device Name (DevEUI)": row["name"],
                    "Device Type": row["type"],
                    "Typ (Klartext)": row.get("type_plain", ""),
                    "Device Label": row["label"],
                }
            )
    print(f"💾 CSV gespeichert: {path}")


def write_excel(flat_rows: List[Dict[str, Any]], path: str) -> None:
    """Schreibt die Struktur nach Excel: je Hierarchie-Ebene eine Spalte."""
    try:
        from openpyxl import Workbook
        from openpyxl.styles import Alignment, Font, PatternFill
        from openpyxl.utils import get_column_letter
    except ImportError:
        print("❌ Excel-Export nicht möglich – 'openpyxl' ist nicht installiert.")
        return

    max_levels = max((len(row["path_parts"]) for row in flat_rows), default=0)

    wb = Workbook()
    ws = wb.active
    ws.title = "Struktur"

    headers = [f"Ebene {i}" for i in range(1, max_levels + 1)]
    headers += ["Device Name (DevEUI)", "Device Type", "Typ (Klartext)", "Device Label"]

    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="366092", end_color="366092", fill_type="solid")
    header_alignment = Alignment(horizontal="center", vertical="center")
    for col, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=header)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = header_alignment

    for row_idx, row in enumerate(flat_rows, 2):
        parts = row["path_parts"]
        for level in range(max_levels):
            value = parts[level] if level < len(parts) else ""
            ws.cell(row=row_idx, column=level + 1, value=value)
        base = max_levels
        ws.cell(row=row_idx, column=base + 1, value=row["name"])
        ws.cell(row=row_idx, column=base + 2, value=row["type"])
        ws.cell(row=row_idx, column=base + 3, value=row.get("type_plain", ""))
        ws.cell(row=row_idx, column=base + 4, value=row["label"])

    ws.freeze_panes = "A2"
    for col in range(1, len(headers) + 1):
        letter = get_column_letter(col)
        max_length = len(str(headers[col - 1]))
        for row_idx in range(2, ws.max_row + 1):
            value = ws.cell(row=row_idx, column=col).value
            if value:
                max_length = max(max_length, len(str(value)))
        ws.column_dimensions[letter].width = min(max(max_length + 2, 12), 45)

    wb.save(path)
    print(f"💾 Excel gespeichert: {path} ({len(flat_rows)} Devices, {max_levels} Ebenen-Spalten)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gibt die komplette Customer-Struktur (Assets + Devices) aus ThingsBoard aus."
    )
    parser.add_argument("customer_id", nargs="?", help="Customer ID (UUID)")
    parser.add_argument("--name", help="Customer über den Titel suchen statt per UUID")
    parser.add_argument("--output", help="Baum-Ausgabe zusätzlich in Textdatei schreiben")
    parser.add_argument("--csv", help="Flache Ausgabe (Pfad|Name|Type|Label) als CSV schreiben")
    parser.add_argument(
        "--excel",
        help="Excel-Ausgabe (.xlsx) – je Hierarchie-Ebene eine Spalte plus Device-Spalten",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not THINGSBOARD_USERNAME or not THINGSBOARD_PASSWORD:
        print("❌ THINGBOARD_USERNAME / THINGBOARD_PASSWORD in .env setzen.")
        return 1

    print("=" * 70)
    print("CUSTOMER STRUCTURE LISTER (ThingsBoard)")
    print("=" * 70)
    print(f"🌐 {THINGSBOARD_BASE_URL}")
    print(f"⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    if not login_to_thingsboard():
        return 1

    customer_id = args.customer_id
    if args.name:
        customer = resolve_customer_by_name(args.name)
        if not customer:
            print(f"❌ Kein Customer mit Namen '{args.name}' gefunden.")
            return 1
        customer_id = _entity_id(customer)
        print(f"✅ Customer gefunden: {customer.get('title')} ({customer_id})")

    if not customer_id:
        print("❌ Bitte Customer ID angeben oder --name verwenden.")
        return 1

    info = get_customer_info(customer_id)
    if info:
        print(f"🏢 Customer: {info.get('title', '?')} ({customer_id})")

    (
        assets_by_id,
        devices_by_id,
        asset_children,
        asset_devices,
        root_asset_ids,
    ) = build_structure(customer_id)

    if not assets_by_id and not devices_by_id:
        print("❌ Keine Assets oder Devices für diesen Customer gefunden.")
        return 1

    text_lines, flat_rows = render_structure(
        assets_by_id, devices_by_id, asset_children, asset_devices, root_asset_ids
    )

    print()
    print("=" * 70)
    for line in text_lines:
        print(line)
    print("=" * 70)
    print(
        f"📊 Zusammenfassung: {len(assets_by_id)} Assets, "
        f"{len(devices_by_id)} Devices, {len(root_asset_ids)} Wurzel-Assets"
    )

    if args.output:
        with open(args.output, "w", encoding="utf-8") as f:
            f.write("\n".join(text_lines) + "\n")
        print(f"💾 Struktur gespeichert: {args.output}")

    if args.csv:
        write_csv(flat_rows, args.csv)

    if args.excel:
        write_excel(flat_rows, args.excel)

    return 0


if __name__ == "__main__":
    sys.exit(main())
