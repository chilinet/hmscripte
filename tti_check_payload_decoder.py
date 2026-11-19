#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TTI Payload-Decoder Checker
===========================
Prüft für alle Devices einer TTI/TTS (The Things Stack / Industries) Application,
ob ein Payload-Decoder (Uplink-Formatter) eingetragen ist.

Ablauf:
- TTS: Alle Devices der Application laden.
- Pro Device die Formatter-Einstellungen abrufen (Uplink-/Downlink-Formatter).
- Auswerten, ob ein Decoder konfiguriert ist (javascript / repository / none).
- Übersicht ausgeben; optional nur fehlende anzeigen und/oder als CSV exportieren.

Konfiguration (.env):
- TTS_BASE_URL, TTS_API_KEY, TTS_APPLICATION_ID
"""

import os
import sys
import csv
import argparse
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

from heatmanager_common.config import REQUEST_TIMEOUT


# Formatter-Typen, die als "kein Decoder" gelten (nach Entfernen des "formatter_"-Präfix)
NO_DECODER_VALUES = {"", "none", "unknown"}


def get_headers(api_key: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {api_key}", "Accept": "application/json"}


def list_devices(session: requests.Session, base_url: str, app_id: str, headers: Dict[str, str], limit: int = 100) -> List[Dict[str, Any]]:
    """Lädt alle Devices einer TTS-Application (paginiert)."""
    devices: List[Dict[str, Any]] = []
    page = 1
    url = f"{base_url}/api/v3/applications/{app_id}/devices"
    while True:
        params = {"limit": limit, "page": page, "field_mask": "ids,name,version_ids"}
        r = session.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            raise RuntimeError(f"TTS Device-Liste fehlgeschlagen: HTTP {r.status_code} - {r.text}")
        data = r.json() or {}
        items = data.get("end_devices") or data.get("devices") or []
        if not items:
            break
        devices.extend(items)
        if len(items) < limit:
            break
        page += 1
    return devices


def get_device_version(session: requests.Session, base_url: str, app_id: str, device_id: str, headers: Dict[str, str]) -> Dict[str, Any]:
    """Holt die Modell-/Version-Infos (version_ids) eines Devices aus dem Identity Server."""
    url = f"{base_url}/api/v3/applications/{app_id}/devices/{device_id}"
    try:
        r = session.get(url, headers=headers, params={"field_mask": "version_ids"}, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            result = r.json() or {}
            if "end_device" in result:
                result = result["end_device"]
            return result.get("version_ids") or {}
    except requests.exceptions.RequestException:
        pass
    return {}


def format_model(version_ids: Optional[Dict[str, Any]]) -> str:
    """Formatiert Brand/Model aus version_ids zu einem lesbaren String."""
    if not version_ids:
        return "—"
    brand = (version_ids.get("brand_id") or "").strip()
    model = (version_ids.get("model_id") or "").strip()
    if brand and model:
        return f"{brand}/{model}"
    return model or brand or "—"


def get_device_formatters(session: requests.Session, base_url: str, app_id: str, device_id: str, headers: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """
    Holt die Formatter-Einstellungen eines Devices.

    Die Formatter (Payload-Decoder/Encoder) liegen im Application Server (as).
    Endpoint: /api/v3/as/applications/{app}/devices/{device_id}?field_mask=formatters
    Gibt das formatters-Dict zurück, {} wenn keine gesetzt sind, None bei Fehler.
    """
    url = f"{base_url}/api/v3/as/applications/{app_id}/devices/{device_id}"
    try:
        r = session.get(url, headers=headers, params={"field_mask": "formatters"}, timeout=REQUEST_TIMEOUT)
        if r.status_code == 200:
            result = r.json() or {}
            if "end_device" in result:
                result = result["end_device"]
            return result.get("formatters") or {}
        # 404: Device existiert im Application Server (noch) nicht -> keine Formatter
        if r.status_code == 404:
            return {}
        return None
    except requests.exceptions.RequestException:
        return None


def extract_formatter_values(formatters: Optional[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    """Extrahiert Uplink-/Downlink-Formatter-Typ und Parameter (unterstützt Feldnamen-Varianten)."""
    if not formatters:
        return {"uplink": None, "downlink": None, "uplink_param": None, "downlink_param": None}

    uplink = formatters.get("uplink_formatter") or formatters.get("up_formatter")
    downlink = formatters.get("downlink_formatter") or formatters.get("down_formatter")
    uplink_param = (
        formatters.get("uplink_formatter_parameter")
        or formatters.get("up_formatter_parameter")
        or formatters.get("formatter_parameter")
    )
    downlink_param = formatters.get("downlink_formatter_parameter") or formatters.get("down_formatter_parameter")
    return {
        "uplink": uplink,
        "downlink": downlink,
        "uplink_param": uplink_param,
        "downlink_param": downlink_param,
    }


def _normalize_formatter_type(formatter_type: Optional[str]) -> str:
    """Normalisiert z.B. 'FORMATTER_JAVASCRIPT' -> 'javascript'."""
    if not formatter_type:
        return ""
    ftype = str(formatter_type).strip().lower()
    if ftype.startswith("formatter_"):
        ftype = ftype[len("formatter_"):]
    return ftype


def has_decoder(formatter_type: Optional[str], formatter_param: Optional[str]) -> bool:
    """True, wenn ein echter Decoder eingetragen ist."""
    ftype = _normalize_formatter_type(formatter_type)
    if ftype in NO_DECODER_VALUES:
        return False
    # Bei javascript zusätzlich prüfen, ob auch tatsächlich Code hinterlegt ist
    if ftype == "javascript":
        return bool(formatter_param and str(formatter_param).strip())
    # repository / grpc / cayenne_lpp o.ä. gelten als konfiguriert
    return True


def pretty_type(formatter_type: Optional[str]) -> str:
    ftype = _normalize_formatter_type(formatter_type)
    if not ftype:
        return "—"
    mapping = {
        "javascript": "Custom Javascript",
        "repository": "Repository",
        "grpc": "gRPC",
        "cayenne_lpp": "Cayenne LPP",
        "none": "None",
    }
    return mapping.get(ftype, ftype)


def main():
    parser = argparse.ArgumentParser(
        description="Prüft für alle Devices einer TTI/TTS Application, ob ein Payload-Decoder (Uplink-Formatter) eingetragen ist.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Beispiele:
  # Alle Devices der Default-Application prüfen
  python3 tti_check_payload_decoder.py

  # Nur Devices OHNE Decoder anzeigen
  python3 tti_check_payload_decoder.py --only-missing

  # Ein einzelnes Device prüfen
  python3 tti_check_payload_decoder.py --device-id eui-7066e1fffe010c08

  # Ergebnis als CSV exportieren
  python3 tti_check_payload_decoder.py --export output/decoder_check.csv
""",
    )
    parser.add_argument("--tts-base-url", default=os.getenv("TTS_BASE_URL"), help="TTS Base-URL (Standard: TTS_BASE_URL aus .env)")
    parser.add_argument("--tts-api-key", default=os.getenv("TTS_API_KEY"), help="TTS API Key (Standard: TTS_API_KEY aus .env)")
    parser.add_argument("--tts-application-id", default=os.getenv("TTS_APPLICATION_ID"), help="TTS Application ID (Standard: TTS_APPLICATION_ID aus .env)")
    parser.add_argument("--device-id", help="Nur ein bestimmtes Device prüfen")
    parser.add_argument("--only-missing", action="store_true", help="Nur Devices OHNE Decoder anzeigen")
    parser.add_argument("--export", metavar="CSV_DATEI", help="Ergebnis als CSV-Datei exportieren")
    parser.add_argument("--limit", type=int, default=100, help="Seitengröße für das Listing (Standard: 100)")
    args = parser.parse_args()

    base_url = (args.tts_base_url or "").rstrip("/")
    api_key = args.tts_api_key
    app_id = args.tts_application_id
    missing = [n for n, v in (("TTS_BASE_URL", base_url), ("TTS_API_KEY", api_key), ("TTS_APPLICATION_ID", app_id)) if not v]
    if missing:
        print(f"❌ Fehlende TTS-Konfiguration: {', '.join(missing)} (in .env oder per Argument setzen)", file=sys.stderr)
        sys.exit(1)

    print("=" * 80)
    print("TTI/TTS PAYLOAD-DECODER CHECK")
    print("=" * 80)
    print(f"  TTS Base-URL:    {base_url}")
    print(f"  TTS Application: {app_id}")
    print(f"  Startzeit:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    session = requests.Session()
    headers = get_headers(api_key)

    # Devices ermitteln
    if args.device_id:
        devices = [{"ids": {"device_id": args.device_id}, "name": args.device_id}]
        print(f"🔍 Prüfe einzelnes Device: {args.device_id}\n")
    else:
        print(f"📋 Lade Devices der Application '{app_id}' ...")
        try:
            devices = list_devices(session, base_url, app_id, headers, limit=args.limit)
        except Exception as e:
            print(f"❌ {e}", file=sys.stderr)
            sys.exit(1)
        print(f"✅ {len(devices)} Devices geladen\n")

    if not devices:
        print("❌ Keine Devices gefunden.")
        sys.exit(1)

    rows: List[Dict[str, str]] = []
    with_decoder = 0
    without_decoder = 0
    errors = 0

    print(f"{'='*132}")
    print(f"{'Device ID':<40} {'Model':<22} {'Decoder?':<12} {'Uplink-Formatter':<22} {'Downlink-Formatter':<22}")
    print(f"{'='*132}")

    for d in devices:
        device_id = (d.get("ids") or {}).get("device_id") or d.get("name") or "N/A"

        # Modell aus der Liste (version_ids); im Einzel-Device-Modus nachladen
        version_ids = d.get("version_ids")
        if version_ids is None and args.device_id:
            version_ids = get_device_version(session, base_url, app_id, device_id, headers)
        model_disp = format_model(version_ids)

        formatters = get_device_formatters(session, base_url, app_id, device_id, headers)

        if formatters is None:
            errors += 1
            status = "❓ Fehler"
            up_disp = dn_disp = "—"
            decoder_ok = False
        else:
            vals = extract_formatter_values(formatters)
            decoder_ok = has_decoder(vals["uplink"], vals["uplink_param"])
            up_disp = pretty_type(vals["uplink"])
            dn_disp = pretty_type(vals["downlink"])
            if decoder_ok:
                with_decoder += 1
                status = "✅ ja"
            else:
                without_decoder += 1
                status = "❌ nein"

        rows.append({
            "device_id": device_id,
            "model": model_disp,
            "has_decoder": "ja" if decoder_ok else ("Fehler" if formatters is None else "nein"),
            "uplink_formatter": up_disp,
            "downlink_formatter": dn_disp,
        })

        if args.only_missing and decoder_ok:
            continue
        print(f"{device_id[:39]:<40} {model_disp[:21]:<22} {status:<12} {up_disp[:21]:<22} {dn_disp[:21]:<22}")

    print(f"{'='*132}\n")

    # CSV-Export
    if args.export:
        out_dir = os.path.dirname(args.export)
        if out_dir and not os.path.exists(out_dir):
            os.makedirs(out_dir)
        try:
            with open(args.export, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=["device_id", "model", "has_decoder", "uplink_formatter", "downlink_formatter"])
                writer.writeheader()
                writer.writerows(rows)
            print(f"✅ CSV exportiert: {args.export} ({len(rows)} Zeilen)\n")
        except Exception as e:
            print(f"❌ Fehler beim CSV-Export: {e}", file=sys.stderr)

    # Zusammenfassung
    print(f"{'='*80}")
    print("ZUSAMMENFASSUNG")
    print(f"{'='*80}")
    print(f"Geprüfte Devices:        {len(devices)}")
    print(f"Mit Payload-Decoder:     {with_decoder}")
    print(f"Ohne Payload-Decoder:    {without_decoder}")
    if errors:
        print(f"Fehler beim Abruf:       {errors}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()
