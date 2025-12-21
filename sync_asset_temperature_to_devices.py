#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Asset Temperature Sync to Devices
Überträgt die min/max Temperatur von Thingsboard Assets auf die zugehörigen Devices
über Agility Thingspark Downlinks.

Verwendung:
  python sync_asset_temperature_to_devices.py <customer_id> [--dry-run]
  python sync_asset_temperature_to_devices.py "customer-id" --dry-run
"""

import requests
import argparse
import sys
import os
from datetime import datetime, timezone
import json
from pathlib import Path
from heatmanager_common.config import (
    THINGSBOARD_BASE_URL,
    THINGSBOARD_USERNAME,
    THINGSBOARD_PASSWORD,
    AGILITY_URL
)

# Globale Variablen
HEADERS = {}
TOKEN = ""
LOG_FILE = None

# Standardwerte
DEFAULT_FPORT = 10

# Device-Typen die unterstützt werden
SUPPORTED_DEVICE_TYPES = ['dnt-LW-eTRV-C', 'dnt-LW-eTRV']


def setup_log_file(customer_id):
    """
    Erstellt eine Log-Datei mit Timestamp
    
    Args:
        customer_id: Customer ID für den Dateinamen
    
    Returns:
        str: Pfad zur Log-Datei
    """
    # Erstelle output-Verzeichnis falls nicht vorhanden
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    
    # Erstelle Dateiname mit Timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"sync_temp_{customer_id[:8]}_{timestamp}.log"
    log_path = output_dir / log_filename
    
    return str(log_path)


def log_print(*args, **kwargs):
    """
    Druckt sowohl auf stdout als auch in die Log-Datei
    
    Args:
        *args: Argumente für print()
        **kwargs: Keyword-Argumente für print()
    """
    # Drucke auf stdout
    print(*args, **kwargs)
    
    # Drucke auch in Log-Datei falls vorhanden
    if LOG_FILE:
        try:
            # Konvertiere alle Argumente zu Strings
            message = ' '.join(str(arg) for arg in args)
            # Füge Newline hinzu falls nicht vorhanden
            if not message.endswith('\n'):
                message += '\n'
            LOG_FILE.write(message)
            LOG_FILE.flush()  # Stelle sicher, dass sofort geschrieben wird
        except Exception as e:
            # Bei Fehler einfach auf stdout ausgeben
            print(f"⚠️  Fehler beim Schreiben in Log-Datei: {e}", file=sys.stderr)


def login_to_thingsboard():
    """Loggt sich bei ThingsBoard ein und holt den JWT Token"""
    global HEADERS, TOKEN
    
    url = f"{THINGSBOARD_BASE_URL}/api/auth/login"
    login_data = {
        "username": THINGSBOARD_USERNAME,
        "password": THINGSBOARD_PASSWORD
    }
    
    try:
        response = requests.post(url, json=login_data, headers={"Content-Type": "application/json"})
        
        if response.status_code == 200:
            data = response.json()
            TOKEN = data.get('token', '')
            HEADERS = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {TOKEN}"
            }
            log_print(f"✅ Login erfolgreich")
            return True
        else:
            log_print(f"❌ Login fehlgeschlagen: {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        log_print(f"❌ Fehler beim Login: {e}")
        return False


def get_customer_assets(customer_id, page_size=1000):
    """
    Holt alle Assets eines Customers
    
    Args:
        customer_id: Customer ID
        page_size: Seitengröße für API-Abfragen
    
    Returns:
        list: Liste der Assets
    """
    url = f"{THINGSBOARD_BASE_URL}/api/customer/{customer_id}/assets"
    params = {
        "pageSize": page_size,
        "page": 0
    }
    
    all_assets = []
    page = 0
    
    try:
        while True:
            params["page"] = page
            response = requests.get(url, headers=HEADERS, params=params)
            
            if response.status_code == 200:
                assets_data = response.json()
                assets = assets_data.get('data', [])
                total_pages = assets_data.get('totalPages', 0)
                
                all_assets.extend(assets)
                
                if page >= total_pages - 1:
                    break
                    
                page += 1
            else:
                log_print(f"❌ Fehler beim Abrufen der Customer-Assets: {response.status_code} - {response.text}")
                break
                
    except requests.exceptions.RequestException as e:
        log_print(f"❌ Fehler beim Abrufen der Customer-Assets: {e}")
        return []
    
    return all_assets


def get_asset_attributes(asset_id):
    """
    Holt Server-Attribute eines Assets (minTemp und maxTemp)
    
    Args:
        asset_id: Asset ID
    
    Returns:
        dict: Dictionary mit minTemp und maxTemp
    """
    url = f"{THINGSBOARD_BASE_URL}/api/plugins/telemetry/ASSET/{asset_id}/values/attributes"
    params = {
        "keys": "minTemp,maxTemp"
    }
    
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        
        if response.status_code == 200:
            attributes_data = response.json()
            attributes_dict = {}
            
            if isinstance(attributes_data, list):
                for attr in attributes_data:
                    if isinstance(attr, dict) and 'key' in attr and 'value' in attr:
                        attributes_dict[attr['key']] = attr['value']
            elif isinstance(attributes_data, dict):
                attributes_dict = attributes_data
            
            return {
                'minTemp': attributes_dict.get('minTemp'),
                'maxTemp': attributes_dict.get('maxTemp')
            }
        else:
            return {'minTemp': None, 'maxTemp': None}
            
    except requests.exceptions.RequestException as e:
        log_print(f"   ⚠️  Fehler beim Abrufen der Asset-Attribute: {e}")
        return {'minTemp': None, 'maxTemp': None}


def get_asset_devices(asset_id):
    """
    Holt alle Devices die mit einem Asset verbunden sind
    
    Args:
        asset_id: Asset ID
    
    Returns:
        list: Liste der Devices
    """
    url = f"{THINGSBOARD_BASE_URL}/api/relations"
    params = {
        "fromId": asset_id,
        "fromType": "ASSET",
        "toType": "DEVICE"
    }
    
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        
        if response.status_code == 200:
            relations = response.json()
            device_ids = []
            
            if isinstance(relations, list):
                for rel in relations:
                    if isinstance(rel, dict):
                        to_entity = rel.get('to', {})
                        if isinstance(to_entity, dict):
                            device_id = to_entity.get('id')
                            if device_id:
                                device_ids.append(device_id)
            
            # Hole Device-Details für alle Device-IDs
            devices = []
            for device_id in device_ids:
                device = get_device_by_id(device_id)
                if device:
                    # Filtere nach unterstützten Device-Typen
                    device_type = device.get('type', '')
                    if device_type in SUPPORTED_DEVICE_TYPES:
                        devices.append(device)
            
            return devices
        else:
            return []
            
    except requests.exceptions.RequestException as e:
        log_print(f"   ⚠️  Fehler beim Abrufen der Asset-Devices: {e}")
        return []


def get_device_by_id(device_id):
    """
    Holt ein Device nach ID
    
    Args:
        device_id: Device ID
    
    Returns:
        dict: Device-Dictionary oder None
    """
    url = f"{THINGSBOARD_BASE_URL}/api/device/{device_id}"
    
    try:
        response = requests.get(url, headers=HEADERS)
        
        if response.status_code == 200:
            return response.json()
        else:
            return None
            
    except requests.exceptions.RequestException as e:
        return None


def get_device_attributes(device_id):
    """
    Holt Client-Attribute eines Devices (manu_temp_min und manu_temp_max)
    
    Args:
        device_id: Device ID
    
    Returns:
        dict: Dictionary mit manu_temp_min und manu_temp_max
    """
    # Verwende direkt den CLIENT_SCOPE Endpunkt
    url = f"{THINGSBOARD_BASE_URL}/api/plugins/telemetry/DEVICE/{device_id}/values/attributes/CLIENT_SCOPE"
    params = {
        "keys": "manu_temp_min,manu_temp_max"
    }
    
    try:
        response = requests.get(url, headers=HEADERS, params=params)
        
        if response.status_code == 200:
            attributes_data = response.json()
            attributes_dict = {}
            
            if isinstance(attributes_data, list):
                for attr in attributes_data:
                    if isinstance(attr, dict) and 'key' in attr and 'value' in attr:
                        attributes_dict[attr['key']] = attr['value']
            elif isinstance(attributes_data, dict):
                attributes_dict = attributes_data
            
            return {
                'manu_temp_min': attributes_dict.get('manu_temp_min'),
                'manu_temp_max': attributes_dict.get('manu_temp_max')
            }
        else:
            return {'manu_temp_min': None, 'manu_temp_max': None}
            
    except requests.exceptions.RequestException as e:
        log_print(f"   ⚠️  Fehler beim Abrufen der Device-Attribute: {e}")
        return {'manu_temp_min': None, 'manu_temp_max': None}


def normalize_deveui(deveui):
    """
    Normalisiert eine DevEUI (entfernt Präfixe, Leerzeichen, Bindestriche, etc.)
    
    Args:
        deveui: DevEUI String (kann verschiedene Formate haben)
    
    Returns:
        str: Normalisierte DevEUI (16 hexadezimale Zeichen) oder None wenn ungültig
    """
    if not deveui:
        return None
    
    deveui = str(deveui).strip().upper()
    # Entferne mögliche Präfixe wie "eui-" oder "EUI-"
    deveui = deveui.replace('EUI-', '').replace('eui-', '').replace('EUI_', '').replace('eui_', '')
    # Entferne Leerzeichen, Bindestriche, Doppelpunkte
    deveui = deveui.replace(' ', '').replace('-', '').replace(':', '')
    
    # Prüfe ob es ein gültiger DevEUI ist (16 hexadezimale Zeichen)
    if len(deveui) == 16 and all(c in '0123456789ABCDEF' for c in deveui):
        return deveui
    
    return None


def extract_deveui(device):
    """
    Extrahiert DevEUI aus einem Device-Objekt
    
    Args:
        device: Device-Dictionary von Thingsboard
    
    Returns:
        str: DevEUI oder None
    """
    device_id = device.get('id', {}).get('id', '')
    
    # 1. Versuche DevEUI aus Attributen zu holen
    url = f"{THINGSBOARD_BASE_URL}/api/plugins/telemetry/DEVICE/{device_id}/values/attributes"
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            all_attrs = response.json()
            if isinstance(all_attrs, list):
                for attr in all_attrs:
                    if isinstance(attr, dict) and 'key' in attr:
                        key = attr['key'].lower()
                        if key in ['deveui', 'eui']:
                            deveui = normalize_deveui(attr.get('value'))
                            if deveui:
                                return deveui
    except:
        pass
    
    # 2. Versuche DevEUI aus dem Device-Namen zu extrahieren
    device_name = device.get('name', '').strip()
    if device_name:
        deveui = normalize_deveui(device_name)
        if deveui:
            return deveui
    
    # 3. Versuche DevEUI aus dem Label zu extrahieren
    device_label = device.get('label', '').strip()
    if device_label:
        deveui = normalize_deveui(device_label)
        if deveui:
            return deveui
    
    # 4. Prüfe additionalInfo
    additional_info = device.get('additionalInfo', {})
    if isinstance(additional_info, dict):
        for key in ['devEUI', 'devEui', 'DevEUI', 'DevEui', 'deveui', 'eui']:
            if key in additional_info:
                deveui = normalize_deveui(additional_info[key])
                if deveui:
                    return deveui
    
    return None


def temperature_to_hex_payload(temperature, is_min=True):
    """
    Konvertiert eine Temperatur zu einem Hex-Payload
    
    Args:
        temperature: Temperatur als Zahl (z.B. 20 für 20°C)
        is_min: True für minTemp (3E), False für maxTemp (40)
    
    Returns:
        str: Hex-Payload String (z.B. "3E28" für 20°C min)
    """
    if temperature is None:
        return None
    
    try:
        temp_value = float(temperature)
        # Temperatur * 2
        temp_hex = int(temp_value * 2)
        
        # Konvertiere zu Hex (1 Byte = 2 hex Zeichen)
        temp_hex_str = format(temp_hex, '02X')
        
        # Füge Präfix hinzu: 3E für min, 40 für max
        prefix = "3E" if is_min else "40"
        
        return f"{prefix}{temp_hex_str}"
    except (ValueError, TypeError):
        return None


def get_query_payload(is_min=True):
    """
    Gibt den Query-Payload zurück (BD für min, BF für max)
    
    Args:
        is_min: True für minTemp Query (BD), False für maxTemp Query (BF)
    
    Returns:
        str: Hex-Payload String ("BD" oder "BF")
    """
    return "BD" if is_min else "BF"


def combine_query_payloads():
    """
    Kombiniert beide Query-Payloads in einem Request
    
    Returns:
        str: Kombinierter Hex-Payload String ("BDBF")
    """
    return "BDBF"


def combine_temperature_payloads(min_temp, max_temp):
    """
    Kombiniert min und max Temperatur in einem Payload
    
    Args:
        min_temp: Min Temperatur als Zahl (z.B. 20 für 20°C)
        max_temp: Max Temperatur als Zahl (z.B. 25 für 25°C)
    
    Returns:
        str: Kombinierter Hex-Payload String (z.B. "3E284032" für 20°C min, 25°C max)
    """
    min_payload = temperature_to_hex_payload(min_temp, is_min=True)
    max_payload = temperature_to_hex_payload(max_temp, is_min=False)
    
    if min_payload and max_payload:
        return f"{min_payload}{max_payload}"
    
    return None


def send_downlink_to_agility(deveui, fport, payload_hex, dry_run=False):
    """
    Sendet eine Downlink-Nachricht an Agility Thingspark
    
    Args:
        deveui: DevEUI des Devices
        fport: FPort (Standard: 10)
        payload_hex: Hexadezimales Payload (z.B. "3E28")
        dry_run: Wenn True, wird nur simuliert ohne tatsächlich zu senden
    
    Returns:
        bool: True wenn erfolgreich, False bei Fehler
    """
    if not AGILITY_URL:
        log_print(f"   ❌ AGILITY_URL nicht in .env konfiguriert")
        return False
    
    # Erstelle JSON-Payload
    current_time = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S+00:00')
    
    payload = {
        "DevEUI_downlink": {
            "Time": current_time,
            "DevEUI": deveui,
            "FPort": fport,
            "payload_hex": payload_hex
        }
    }
    
    if dry_run:
        log_print(f"   📤 DRY-RUN: Würde senden an {AGILITY_URL}")
        log_print(f"      Payload: {json.dumps(payload, indent=6)}")
        return True
    
    try:
        response = requests.post(
            AGILITY_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        if response.status_code in [200, 201, 202]:
            log_print(f"   ✅ Downlink erfolgreich gesendet")
            return True
        else:
            log_print(f"   ❌ Fehler beim Senden: {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        log_print(f"   ❌ Fehler beim Senden: {e}")
        return False


def main():
    """Hauptfunktion"""
    parser = argparse.ArgumentParser(
        description="Überträgt min/max Temperatur von Thingsboard Assets auf Devices über Agility",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Beispiele:
  # Synchronisiere Temperaturen für einen Customer
  python sync_asset_temperature_to_devices.py "customer-id"
  
  # Dry-Run: Simuliere das Senden ohne tatsächliche Nachrichten
  python sync_asset_temperature_to_devices.py "customer-id" --dry-run
        """
    )
    
    parser.add_argument(
        "customer_id",
        help="Customer ID, dessen Assets synchronisiert werden sollen"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simuliert das Senden ohne tatsächliche Nachrichten zu senden"
    )
    
    parser.add_argument(
        "--fport",
        type=int,
        default=DEFAULT_FPORT,
        help=f"FPort für die Downlink-Nachricht (Standard: {DEFAULT_FPORT})"
    )
    
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Begrenzt die Anzahl der zu verarbeitenden Devices (nützlich zum Testen)"
    )
    
    args = parser.parse_args()
    
    # Erstelle Log-Datei
    global LOG_FILE
    log_path = setup_log_file(args.customer_id)
    try:
        LOG_FILE = open(log_path, 'w', encoding='utf-8')
        log_print(f"📝 Log-Datei: {log_path}")
    except Exception as e:
        print(f"⚠️  Warnung: Konnte Log-Datei nicht erstellen: {e}", file=sys.stderr)
        LOG_FILE = None
    
    # Überprüfe ob Credentials aus .env geladen wurden
    if not AGILITY_URL:
        log_print("❌ Fehler: AGILITY_URL nicht in .env gefunden")
        log_print("   Bitte fügen Sie AGILITY_URL=<url> zur .env-Datei hinzu.")
        if LOG_FILE:
            LOG_FILE.close()
        sys.exit(1)
    
    if not THINGSBOARD_USERNAME or not THINGSBOARD_PASSWORD:
        log_print("❌ Fehler: THINGBOARD_USERNAME oder THINGBOARD_PASSWORD nicht in .env gefunden")
        log_print("   Bitte stellen Sie sicher, dass die .env-Datei existiert und die Variablen enthält.")
        if LOG_FILE:
            LOG_FILE.close()
        sys.exit(1)
    
    log_print(f"🌡️  ASSET TEMPERATURE SYNC TO DEVICES")
    log_print(f"{'='*80}")
    log_print(f"👤 Customer ID: {args.customer_id}")
    log_print(f"🌐 ThingsBoard URL: {THINGSBOARD_BASE_URL}")
    log_print(f"📡 Agility URL: {AGILITY_URL}")
    log_print(f"🔌 FPort: {args.fport}")
    if args.dry_run:
        log_print(f"⚠️  DRY-RUN Modus aktiviert")
    if args.limit:
        log_print(f"🔢 Limit: {args.limit} Devices")
    log_print(f"⏰ Startzeit: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_print(f"{'='*80}\n")
    
    # Login
    if not login_to_thingsboard():
        log_print("❌ Login fehlgeschlagen. Beende Programm.")
        if LOG_FILE:
            LOG_FILE.close()
        sys.exit(1)
    
    # Customer-Assets abrufen
    log_print(f"🔍 Hole Assets für Customer '{args.customer_id}'...")
    assets = get_customer_assets(args.customer_id)
    
    if not assets:
        log_print(f"❌ Keine Assets für diesen Customer gefunden.")
        if LOG_FILE:
            LOG_FILE.close()
        sys.exit(1)
    
    log_print(f"✅ {len(assets)} Assets gefunden\n")
    
    # Statistik
    stats = {
        'assets_processed': 0,
        'devices_processed': 0,
        'min_temp_sent': 0,
        'max_temp_sent': 0,
        'combined_sent': 0,
        'min_query_sent': 0,
        'max_query_sent': 0,
        'skipped_no_deveui': 0,
        'skipped_no_asset_temp': 0,
        'skipped_empty_min_temp': 0,
        'skipped_empty_max_temp': 0,
        'errors': 0
    }
    
    # Verarbeite jedes Asset
    log_print(f"{'='*120}")
    log_print(f"{'Asset Name':<30} {'Device Name':<30} {'DevEUI':<20} {'Aktion':<30} {'Details':<10}")
    log_print(f"{'='*120}")
    
    # Zähler für Device-Limit
    devices_processed_count = 0
    limit_reached = False
    
    for asset in assets:
        # Prüfe ob Limit erreicht wurde (vor der Asset-Verarbeitung)
        if limit_reached:
            break
            
        asset_id = asset.get('id', {}).get('id', '')
        asset_name = asset.get('name', 'Unbekannt')
        
        # Hole Asset-Attribute (minTemp, maxTemp)
        asset_attrs = get_asset_attributes(asset_id)
        asset_min_temp = asset_attrs.get('minTemp')
        asset_max_temp = asset_attrs.get('maxTemp')
        
        # Überspringe Assets ohne Temperatur-Einstellungen
        if asset_min_temp is None and asset_max_temp is None:
            stats['skipped_no_asset_temp'] += 1
            continue
        
        stats['assets_processed'] += 1
        
        # Hole zugehörige Devices
        devices = get_asset_devices(asset_id)
        
        if not devices:
            continue
        
        # Verarbeite jedes Device
        for device in devices:
            # Prüfe ob Limit erreicht wurde
            if args.limit and devices_processed_count >= args.limit:
                log_print(f"\n⚠️  Limit von {args.limit} Devices erreicht. Stoppe Verarbeitung.")
                limit_reached = True
                break
            
            device_id = device.get('id', {}).get('id', '')
            device_name = device.get('name', 'Unbekannt')
            
            devices_processed_count += 1
            stats['devices_processed'] += 1
            
            # Extrahiere DevEUI
            deveui = extract_deveui(device)
            
            if not deveui:
                log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {'N/A':<20} {'❌ Kein DevEUI':<30} {'':<10}")
                stats['skipped_no_deveui'] += 1
                continue
            
            # Hole Device-Attribute (manu_temp_min, manu_temp_max)
            device_attrs = get_device_attributes(device_id)
            device_min_temp = device_attrs.get('manu_temp_min')
            device_max_temp = device_attrs.get('manu_temp_max')
            
            # Normalisiere: Behandle leere Strings, 0, etc. als None
            if device_min_temp == "" or device_min_temp == 0:
                device_min_temp = None
            if device_max_temp == "" or device_max_temp == 0:
                device_max_temp = None
            
            # Prüfe ob Asset-Temperaturen vorhanden sind und gib Meldung aus wenn leer
            if asset_min_temp is None:
                log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {deveui:<20} {'⚠️  Asset minTemp leer':<30} {'':<10}")
                stats['skipped_empty_min_temp'] += 1
            
            if asset_max_temp is None:
                log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {deveui:<20} {'⚠️  Asset maxTemp leer':<30} {'':<10}")
                stats['skipped_empty_max_temp'] += 1
            
            # Wenn beide Asset-Temperaturen leer sind, überspringe dieses Device
            if asset_min_temp is None and asset_max_temp is None:
                continue
            
            # Prüfe welche Temperaturen gesendet werden müssen (nur wenn Asset-Temp vorhanden)
            needs_min = asset_min_temp is not None and (device_min_temp is None or device_min_temp != asset_min_temp)
            needs_max = asset_max_temp is not None and (device_max_temp is None or device_max_temp != asset_max_temp)
            
            # Prüfe ob Queries nötig sind (nur wenn Device-Temp leer ist)
            # WICHTIG: Explizit prüfen ob beide None sind für kombinierte Query
            both_device_temps_none = device_min_temp is None and device_max_temp is None
            min_is_query = needs_min and device_min_temp is None
            max_is_query = needs_max and device_max_temp is None
            
            # Wenn beide gesendet werden müssen, prüfe ob kombiniert werden kann
            if needs_min and needs_max:
                # Prüfe zuerst: Beide Device-Temperaturen sind leer → Query senden (BDBF)
                # WICHTIG: Explizit prüfen mit is None
                device_min_is_none = (device_min_temp is None)
                device_max_is_none = (device_max_temp is None)
                
                # Prüfe zuerst: Beide Device-Temperaturen sind leer → Query senden (BDBF)
                if device_min_is_none and device_max_is_none:
                    payload = combine_query_payloads()  # Gibt "BDBF" zurück
                    action = "📤 Query Min/Max Temp"
                    # Ausgabe VOR dem Senden
                    log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {deveui:<20} {action:<30} {'':<10}")
                    success = send_downlink_to_agility(deveui, args.fport, payload, args.dry_run)
                    if success:
                        stats['min_query_sent'] += 1
                        stats['max_query_sent'] += 1
                        stats['combined_sent'] += 1
                    else:
                        log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {deveui:<20} {'❌ Fehler Query Min/Max':<30} {'':<10}")
                        stats['errors'] += 1
                # Beide Device-Temperaturen sind vorhanden → Set senden
                elif not device_min_is_none and not device_max_is_none:
                    payload = combine_temperature_payloads(asset_min_temp, asset_max_temp)
                    if payload:
                        # Füge Query-Hex-Werte hinten an (BDBF)
                        query_payload = combine_query_payloads()  # Gibt "BDBF" zurück
                        payload = payload + query_payload  # z.B. "3E144030" + "BDBF" = "3E144030BDBF"
                        action = f"📤 Set Min/Max ({asset_min_temp}°C/{asset_max_temp}°C)"
                        # Ausgabe VOR dem Senden
                        log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {deveui:<20} {action:<30} {'':<10}")
                        success = send_downlink_to_agility(deveui, args.fport, payload, args.dry_run)
                        if success:
                            stats['min_temp_sent'] += 1
                            stats['max_temp_sent'] += 1
                            stats['combined_sent'] += 1
                        else:
                            log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {deveui:<20} {'❌ Fehler Min/Max':<30} {'':<10}")
                            stats['errors'] += 1
                else:
                    # Eine Query, eine Temperatur → einzeln senden
                    # Prüfe und sende minTemp
                    if needs_min:
                        if min_is_query:
                            # Query senden (BD)
                            payload = get_query_payload(is_min=True)
                            action = "📤 Query Min Temp"
                            stats['min_query_sent'] += 1
                        else:
                            # Temperatur senden (3E + temp*2)
                            payload = temperature_to_hex_payload(asset_min_temp, is_min=True)
                            action = f"📤 Set Min Temp ({asset_min_temp}°C)"
                            stats['min_temp_sent'] += 1
                        
                        if payload:
                            success = send_downlink_to_agility(deveui, args.fport, payload, args.dry_run)
                            if success:
                                log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {deveui:<20} {action:<30} {'':<10}")
                            else:
                                log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {deveui:<20} {'❌ Fehler Min':<30} {'':<10}")
                                stats['errors'] += 1
                    
                    # Prüfe und sende maxTemp
                    if needs_max:
                        if max_is_query:
                            # Query senden (BF)
                            payload = get_query_payload(is_min=False)
                            action = "📤 Query Max Temp"
                            stats['max_query_sent'] += 1
                        else:
                            # Temperatur senden (40 + temp*2)
                            payload = temperature_to_hex_payload(asset_max_temp, is_min=False)
                            action = f"📤 Set Max Temp ({asset_max_temp}°C)"
                            stats['max_temp_sent'] += 1
                        
                        if payload:
                            success = send_downlink_to_agility(deveui, args.fport, payload, args.dry_run)
                            if success:
                                log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {deveui:<20} {action:<30} {'':<10}")
                            else:
                                log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {deveui:<20} {'❌ Fehler Max':<30} {'':<10}")
                                stats['errors'] += 1
            else:
                # Nur eine Temperatur → einzeln senden
                # Prüfe und sende minTemp
                if needs_min:
                    if min_is_query:
                        # Query senden (BD)
                        payload = get_query_payload(is_min=True)
                        action = "📤 Query Min Temp"
                        stats['min_query_sent'] += 1
                    else:
                        # Temperatur senden (3E + temp*2)
                        payload = temperature_to_hex_payload(asset_min_temp, is_min=True)
                        action = f"📤 Set Min Temp ({asset_min_temp}°C)"
                        stats['min_temp_sent'] += 1
                    
                    if payload:
                        success = send_downlink_to_agility(deveui, args.fport, payload, args.dry_run)
                        if success:
                            log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {deveui:<20} {action:<30} {'':<10}")
                        else:
                            log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {deveui:<20} {'❌ Fehler Min':<30} {'':<10}")
                            stats['errors'] += 1
                
                # Prüfe und sende maxTemp
                if needs_max:
                    if max_is_query:
                        # Query senden (BF)
                        payload = get_query_payload(is_min=False)
                        action = "📤 Query Max Temp"
                        stats['max_query_sent'] += 1
                    else:
                        # Temperatur senden (40 + temp*2)
                        payload = temperature_to_hex_payload(asset_max_temp, is_min=False)
                        action = f"📤 Set Max Temp ({asset_max_temp}°C)"
                        stats['max_temp_sent'] += 1
                    
                    if payload:
                        success = send_downlink_to_agility(deveui, args.fport, payload, args.dry_run)
                        if success:
                            log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {deveui:<20} {action:<30} {'':<10}")
                        else:
                            log_print(f"{asset_name[:29]:<30} {device_name[:29]:<30} {deveui:<20} {'❌ Fehler Max':<30} {'':<10}")
                            stats['errors'] += 1
    
    log_print(f"{'='*120}\n")
    
    # Zusammenfassung
    log_print(f"📊 ZUSAMMENFASSUNG")
    log_print(f"{'='*80}")
    log_print(f"Assets verarbeitet: {stats['assets_processed']}")
    log_print(f"Devices verarbeitet: {stats['devices_processed']}")
    log_print(f"Min Temp gesendet: {stats['min_temp_sent']}")
    log_print(f"Max Temp gesendet: {stats['max_temp_sent']}")
    log_print(f"Kombinierte Requests: {stats['combined_sent']}")
    log_print(f"Min Query gesendet: {stats['min_query_sent']}")
    log_print(f"Max Query gesendet: {stats['max_query_sent']}")
    log_print(f"Übersprungen (kein DevEUI): {stats['skipped_no_deveui']}")
    log_print(f"Übersprungen (keine Asset-Temp): {stats['skipped_no_asset_temp']}")
    log_print(f"Übersprungen (Asset minTemp leer): {stats['skipped_empty_min_temp']}")
    log_print(f"Übersprungen (Asset maxTemp leer): {stats['skipped_empty_max_temp']}")
    log_print(f"Fehler: {stats['errors']}")
    log_print(f"FPort: {args.fport}")
    if args.dry_run:
        log_print(f"Modus: DRY-RUN (keine Nachrichten gesendet)")
    log_print(f"{'='*80}\n")
    
    log_print(f"✅ Verarbeitung abgeschlossen!")
    
    # Schließe Log-Datei
    if LOG_FILE:
        LOG_FILE.close()
        log_print(f"📝 Log-Datei gespeichert: {log_path}")


if __name__ == "__main__":
    main()

