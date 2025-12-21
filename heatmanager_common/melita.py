#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zentrale Melita.io Verbindungsfunktionen
Kann von allen Python-Skripten genutzt werden für:
- Token-Generierung
- API-Aufrufe
- Queue-Nachrichten
- Device-Queue-Verwaltung
"""

import os
import json
import requests
import time
import base64
from datetime import datetime
from dotenv import load_dotenv

# .env Datei laden
load_dotenv()

# Melita.io Konfiguration
MELITA_BASE_URL = "https://www.melita.io"
MELITA_API_KEY = os.getenv('MELITA_API_KEY')

# Globaler Melita Bearer Token
melita_bearer_token = None

def check_melita_connection():
    """Testet die Verbindung zu Melita.io"""
    if not MELITA_API_KEY:
        print("❌ MELITA_API_KEY nicht in .env gesetzt")
        return False
    
    try:
        # Einfacher GET-Request zum Testen der Verbindung
        response = requests.get(f"{MELITA_BASE_URL}/api/iot-gateway/auth/generate", 
                              headers={"ApiKey": MELITA_API_KEY}, timeout=10)
        print(f"✅ Melita.io Verbindung erfolgreich - Status: {response.status_code}")
        return True
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Verbindungsfehler zu Melita.io: {e}")
        return False
    except requests.exceptions.Timeout as e:
        print(f"❌ Timeout-Fehler bei Melita.io: {e}")
        return False
    except Exception as e:
        print(f"❌ Unbekannter Verbindungsfehler zu Melita.io: {e}")
        return False

def generate_melita_bearer_token():
    """Generiert einen Bearer Token für Melita.io über den Auth-Endpunkt"""
    global melita_bearer_token
    
    if not MELITA_API_KEY:
        print("❌ MELITA_API_KEY nicht in .env gesetzt")
        return None
    
    try:
        print(f"🔑 Generiere Melita.io Bearer Token...")
        
        # Melita Auth-Endpunkt
        auth_url = f"{MELITA_BASE_URL}/api/iot-gateway/auth/generate"
        headers = {"ApiKey": MELITA_API_KEY}
        
        response = requests.post(auth_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            try:
                data = response.json()
                if 'authToken' in data:
                    melita_bearer_token = data['authToken']
                    print(f"✅ Melita.io Bearer Token erfolgreich generiert")
                    print(f"   Token: {melita_bearer_token[:20]}...{melita_bearer_token[-20:]}")
                    
                    if 'expiry' in data:
                        expiry_timestamp = data['expiry']
                        expiry_date = datetime.fromtimestamp(expiry_timestamp).strftime('%Y-%m-%d %H:%M:%S')
                        print(f"   ⏰ Token läuft ab: {expiry_date}")
                    
                    return melita_bearer_token
                else:
                    print(f"⚠️  Token nicht in der API-Antwort gefunden")
                    print(f"   Verfügbare Schlüssel: {list(data.keys())}")
                    return None
            except json.JSONDecodeError as e:
                print(f"❌ Fehler beim Parsen der JSON-Antwort: {e}")
                return None
        else:
            print(f"❌ HTTP-Fehler {response.status_code}: {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Fehler beim Generieren des Melita.io Bearer Tokens: {e}")
        return None

def get_melita_headers():
    """Gibt die HTTP-Header mit dem aktuellen Melita.io Bearer Token zurück"""
    global melita_bearer_token
    
    if not melita_bearer_token:
        print("❌ Kein Melita.io Bearer Token verfügbar!")
        return None
    
    return {
        "Authorization": f"Bearer {melita_bearer_token}",
        "Content-Type": "application/json",
        "accept": "application/json"
    }

def refresh_melita_token_if_needed():
    """Erneuert den Token falls er abgelaufen ist oder fehlt"""
    global melita_bearer_token
    
    if not melita_bearer_token:
        return generate_melita_bearer_token()
    
    # Hier könnte man eine Token-Validitätsprüfung hinzufügen
    # Für jetzt: Token erneuern wenn er fehlt
    return melita_bearer_token

def flush_melita_device_queue(device_eui):
    """Leert die Queue eines Melita.io Devices vor dem Senden neuer Nachrichten"""
    global melita_bearer_token
    
    if not melita_bearer_token:
        print("❌ Kein Melita.io Bearer Token verfügbar")
        return False
    
    headers = get_melita_headers()
    url = f"{MELITA_BASE_URL}/api/iot-gateway/lorawan/{device_eui}/queue"
    
    try:
        print(f"🧹 Leere Queue für Device {device_eui}...")
        response = requests.delete(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            print(f"✅ Queue erfolgreich geleert für {device_eui}")
            return True
        elif response.status_code == 204:
            print(f"✅ Queue erfolgreich geleert für {device_eui} (keine Inhalte)")
            return True
        elif response.status_code == 403:
            print(f"⚠️  Token abgelaufen (403) - Versuche Token zu erneuern...")
            # Token erneuern und erneut versuchen
            if generate_melita_bearer_token():
                print(f"🔄 Token erneuert - Versuche Queue-Leerung erneut...")
                headers = get_melita_headers()
                response = requests.delete(url, headers=headers, timeout=30)
                if response.status_code in [200, 204]:
                    print(f"✅ Queue erfolgreich geleert für {device_eui} (nach Token-Erneuerung)")
                    return True
                else:
                    print(f"❌ Fehler beim erneuten Versuch: {response.status_code}")
                    return False
            else:
                print(f"❌ Token-Erneuerung fehlgeschlagen")
                return False
        else:
            print(f"❌ Fehler beim Leeren der Queue: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Fehler beim Leeren der Queue: {e}")
        return False

def send_melita_queue_message(device_eui, data="FRg=", fport=2, confirmed=False):
    """Sendet eine Queue-Nachricht an ein Melita.io Device"""
    global melita_bearer_token
    
    if not melita_bearer_token:
        print("❌ Kein Melita.io Bearer Token verfügbar")
        return False
    
    # Zuerst die Queue leeren (gemäß Melita.io API-Dokumentation)
    if not flush_melita_device_queue(device_eui):
        print(f"⚠️  Queue konnte nicht geleert werden - überspringe Device {device_eui}")
        return False
    
    # Kurze Pause nach dem Leeren der Queue
    time.sleep(1)
    
    headers = get_melita_headers()
    url = f"{MELITA_BASE_URL}/api/iot-gateway/lorawan/{device_eui}/queue"
    
    # Queue-Nachricht mit den spezifizierten Parametern
    queue_data = {
        "confirmed": confirmed,
        "data": data,
        "devEUI": device_eui,
        "fPort": fport
    }
    
    try:
        print(f"📤 Sende Queue-Nachricht an Device {device_eui}...")
        response = requests.post(url, headers=headers, json=queue_data, timeout=30)
        
        if response.status_code == 200:
            print(f"✅ Queue-Nachricht erfolgreich gesendet an {device_eui}")
            return True
        elif response.status_code == 403:
            print(f"⚠️  Token abgelaufen (403) - Versuche Token zu erneuern...")
            # Token erneuern und erneut versuchen
            if generate_melita_bearer_token():
                print(f"🔄 Token erneuert - Versuche erneut...")
                headers = get_melita_headers()
                response = requests.post(url, headers=headers, json=queue_data, timeout=30)
                if response.status_code == 200:
                    print(f"✅ Queue-Nachricht erfolgreich gesendet an {device_eui} (nach Token-Erneuerung)")
                    return True
                else:
                    print(f"❌ Fehler beim erneuten Versuch: {response.status_code}")
                    return False
            else:
                print(f"❌ Token-Erneuerung fehlgeschlagen")
                return False
        else:
            print(f"❌ Fehler beim Senden der Queue-Nachricht: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Fehler beim Senden der Queue-Nachricht: {e}")
        return False

def create_temperature_hex_payload(min_temp, max_temp, operational_mode=None):
    """
    Erstellt einen Hex-Payload für Temperaturdaten
    Format: 08 + minTemp (1 Byte) + maxTemp (1 Byte) + operationalMode (2 Bytes) + 15 + 18
    """
    try:
        # Temperaturwerte in Integer umwandeln
        min_temp_int = int(min_temp)
        max_temp_int = int(max_temp)
        
        # Prüfe ob Temperaturen im gültigen Bereich liegen (0-255°C)
        if min_temp_int < 0 or min_temp_int > 255:
            print(f"⚠️  minTemp {min_temp_int}°C außerhalb des gültigen Bereichs (0-255°C)")
            return None
        if max_temp_int < 0 or max_temp_int > 255:
            print(f"⚠️  maxTemp {max_temp_int}°C außerhalb des gültigen Bereichs (0-255°C)")
            return None
        
        # Operational Mode bestimmen
        if operational_mode in [2, 10]:
            op_mode_hex = "02"
            print(f"   🔧 Operational Mode: {operational_mode} → 02 (aktiviert)")
        else:
            op_mode_hex = "00"
            print(f"   🔧 Operational Mode: {operational_mode} → 00 (deaktiviert)")
        
        # Hex-String erstellen: 08 + minTemp (1 Byte) + maxTemp (1 Byte) + operationalMode (2 Bytes) + 15 + 18
        hex_payload = f"08{min_temp_int:02x}{max_temp_int:02x}0d{op_mode_hex}1518"
        
        print(f"🌡️  Temperatur-Payload erstellt:")
        print(f"   minTemp: {min_temp_int}°C -> {min_temp_int:02x}")
        print(f"   maxTemp: {max_temp_int}°C -> {max_temp_int:02x}")
        print(f"   Hex-Payload: {hex_payload} (7 Bytes)")
        print(f"   Zusätzliche Hex-Werte: 15 + 18")
        
        return hex_payload
        
    except (ValueError, TypeError) as e:
        print(f"❌ Fehler beim Erstellen des Temperatur-Payloads: {e}")
        return None

def hex_to_base64(hex_string):
    """Wandelt einen Hex-String in Base64 um"""
    try:
        # Hex-String zu Bytes
        hex_bytes = bytes.fromhex(hex_string)
        # Bytes zu Base64
        base64_string = base64.b64encode(hex_bytes).decode('utf-8')
        
        print(f"🔄 Hex zu Base64 konvertiert:")
        print(f"   Hex: {hex_string}")
        print(f"   Base64: {base64_string}")
        
        return base64_string
        
    except Exception as e:
        print(f"❌ Fehler bei der Base64-Konvertierung: {e}")
        return None

def send_temperature_to_vicki_device(device_eui, min_temp, max_temp, operational_mode=None, fport=2):
    """
    Sendet Temperaturdaten an ein vicki-Device
    - Erstellt Hex-Payload: 08 + minTemp + maxTemp + operationalMode
    - Konvertiert zu Base64
    - Sendet an Melita.io
    """
    print(f"🌡️  Sende Temperaturdaten an vicki-Device {device_eui}")
    print(f"   minTemp: {min_temp}°C, maxTemp: {max_temp}°C")
    if operational_mode is not None:
        print(f"   Operational Mode: {operational_mode}")
    
    # Hex-Payload erstellen
    hex_payload = create_temperature_hex_payload(min_temp, max_temp, operational_mode)
    if not hex_payload:
        print(f"❌ Konnte Hex-Payload nicht erstellen für Device {device_eui}")
        return False
    
    # Hex zu Base64 konvertieren
    base64_payload = hex_to_base64(hex_payload)
    if not base64_payload:
        print(f"❌ Konnte Base64-Payload nicht erstellen für Device {device_eui}")
        return False
    
    # Nachricht an Melita.io senden
    success = send_melita_queue_message(device_eui, data=base64_payload, fport=fport)
    
    if success:
        print(f"✅ Temperaturdaten erfolgreich an {device_eui} gesendet")
        print(f"   Payload: {base64_payload}")
    else:
        print(f"❌ Fehler beim Senden der Temperaturdaten an {device_eui}")
    
    return success

def send_temperature_to_all_vicki_devices(devices_data, fport=2):
    """
    Sendet Temperaturdaten an alle vicki-Devices
    devices_data: Liste von Dictionaries mit device_eui, min_temp, max_temp, operational_mode (optional)
    """
    if not devices_data:
        print("⚠️  Keine Devices-Daten übergeben")
        return False
    
    print(f"🚀 Starte Temperatur-Synchronisation für {len(devices_data)} vicki-Devices")
    
    # Melita.io Token generieren
    if not generate_melita_bearer_token():
        print("❌ Konnte Melita.io Token nicht generieren")
        return False
    
    success_count = 0
    error_count = 0
    
    for i, device in enumerate(devices_data, 1):
        device_eui = device.get('device_eui')
        min_temp = device.get('min_temp')
        max_temp = device.get('max_temp')
        operational_mode = device.get('operational_mode')  # Optional
        
        if not all([device_eui, min_temp is not None, max_temp is not None]):
            print(f"⚠️  Unvollständige Daten für Device {i}: {device}")
            error_count += 1
            continue
        
        print(f"\n📱 Device {i}/{len(devices_data)}: {device_eui}")
        
        try:
            if send_temperature_to_vicki_device(device_eui, min_temp, max_temp, operational_mode, fport):
                success_count += 1
            else:
                error_count += 1
        except Exception as e:
            print(f"❌ Unerwarteter Fehler bei Device {device_eui}: {e}")
            error_count += 1
        
        # Kurze Pause zwischen den Devices
        if i < len(devices_data):
            time.sleep(2)
    
    print(f"\n🎯 Temperatur-Synchronisation abgeschlossen:")
    print(f"   ✅ Erfolgreich: {success_count}")
    print(f"   ❌ Fehler: {error_count}")
    print(f"   📊 Gesamt: {len(devices_data)}")
    
    return success_count > 0

def get_melita_devices(contract_id=None):
    """Holt alle verfügbaren Devices von Melita.io"""
    global melita_bearer_token
    
    if not melita_bearer_token:
        print("❌ Kein Melita.io Bearer Token verfügbar")
        return None
    
    headers = get_melita_headers()
    
    if contract_id:
        print(f"🔍 Hole alle Devices für Contract ID: {contract_id}")
        url = f"{MELITA_BASE_URL}/api/iot-gateway/lorawan/devices"
        params = {'contractId': contract_id, 'pageSize': 1000, 'page': 0}
    else:
        print(f"🔍 Hole alle verfügbaren Devices")
        url = f"{MELITA_BASE_URL}/api/iot-gateway/lorawan/devices"
        params = {'pageSize': 1000, 'page': 0}
    
    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            
            # Verschiedene Response-Strukturen unterstützen
            devices = None
            if 'content' in data:
                devices = data['content']
            elif 'devices' in data:
                devices = data['devices']
            elif 'data' in data:
                devices = data['data']
            elif 'results' in data:
                devices = data['results']
            elif 'items' in data:
                devices = data['items']
            
            if devices:
                print(f"✅ {len(devices)} Devices erfolgreich geladen")
                return devices
            else:
                print("⚠️  Keine Devices in der Antwort gefunden")
                return []
        else:
            print(f"❌ Fehler beim Abrufen der Devices: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Fehler beim Abrufen der Devices: {e}")
        return None

def get_melita_contracts():
    """Holt alle verfügbaren Contracts von Melita.io"""
    global melita_bearer_token
    
    if not melita_bearer_token:
        print("❌ Kein Melita.io Bearer Token verfügbar")
        return None
    
    headers = get_melita_headers()
    url = f"{MELITA_BASE_URL}/api/iot-gateway/contracts"
    
    print(f"📋 Hole alle verfügbaren Contracts von Melita.io...")
    
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            
            if 'contracts' in data and isinstance(data['contracts'], list):
                contracts = data['contracts']
                print(f"✅ {len(contracts)} Contracts erfolgreich geladen")
                return contracts
            else:
                print(f"⚠️  Keine Contracts in der Antwort gefunden")
                return None
        else:
            print(f"❌ Fehler beim Abrufen der Contracts: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Fehler beim Abrufen der Contracts: {e}")
        return None

# Hilfsfunktionen
def is_melita_connected():
    """Prüft ob eine Verbindung zu Melita.io besteht"""
    return melita_bearer_token is not None

def get_melita_token_info():
    """Gibt Informationen über den aktuellen Token zurück"""
    if melita_bearer_token:
        return {
            'has_token': True,
            'token_preview': f"{melita_bearer_token[:20]}...{melita_bearer_token[-20:]}"
        }
    else:
        return {
            'has_token': False,
            'token_preview': None
        }
