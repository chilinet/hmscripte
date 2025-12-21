# Heatmanager Common - Zentrale Melita.io Funktionen

Dieses Paket enthält zentrale Funktionen für die Verbindung mit Melita.io, die von allen Python-Skripten genutzt werden können.

## Installation

1. Stellen Sie sicher, dass das `heatmanager_common` Verzeichnis im Python-Pfad liegt
2. Installieren Sie die erforderlichen Abhängigkeiten:
   ```bash
   pip install requests python-dotenv
   ```

## Konfiguration

Erstellen Sie eine `.env` Datei im Hauptverzeichnis:
```bash
MELITA_API_KEY=ihr_melita_api_key_hier
MSSQL_SERVER=ihr_server
MSSQL_DATABASE=ihre_datenbank
MSSQL_USER=ihr_benutzer
MSSQL_PASSWORD=ihr_passwort
```

## Verwendung

### 1. Einfache Verwendung

```python
from heatmanager_common import generate_melita_bearer_token, send_melita_queue_message

# Token generieren
if generate_melita_bearer_token():
    # Queue-Nachricht senden
    success = send_melita_queue_message("device_eui_here")
    if success:
        print("Nachricht erfolgreich gesendet!")
```

### 2. Temperatur-Synchronisation für vicki-Devices

```python
from heatmanager_common import (
    check_melita_connection,
    send_temperature_to_vicki_device,
    send_temperature_to_all_vicki_devices
)

def main():
    # Verbindung testen
    if not check_melita_connection():
        print("Keine Verbindung zu Melita.io möglich")
        return
    
    # Einzelnes Device
    success = send_temperature_to_vicki_device(
        device_eui="70b3d52dd3007c11",
        min_temp=18,
        max_temp=24
    )
    
    # Mehrere Devices
    devices_data = [
        {'device_eui': '70b3d52dd3007c11', 'min_temp': 18, 'max_temp': 24},
        {'device_eui': '70b3d52dd3007d2a', 'min_temp': 20, 'max_temp': 26}
    ]
    
    send_temperature_to_all_vicki_devices(devices_data)
```

### 3. Vollständiges Beispiel

```python
from heatmanager_common import (
    check_melita_connection,
    generate_melita_bearer_token,
    send_melita_queue_message,
    get_melita_devices,
    get_melita_contracts
)

def main():
    # Verbindung testen
    if not check_melita_connection():
        print("Keine Verbindung zu Melita.io möglich")
        return
    
    # Token generieren
    if not generate_melita_bearer_token():
        print("Token-Generierung fehlgeschlagen")
        return
    
    # Alle Contracts abrufen
    contracts = get_melita_contracts()
    if contracts:
        print(f"Gefunden: {len(contracts)} Contracts")
        
        # Für jeden Contract Devices abrufen
        for contract in contracts:
            contract_id = contract.get('contractId')
            devices = get_melita_devices(contract_id)
            
            if devices:
                print(f"Contract {contract_id}: {len(devices)} Devices")
                
                # Queue-Nachricht an jedes Device senden
                for device in devices:
                    device_eui = device.get('deviceEUI')
                    if device_eui:
                        success = send_melita_queue_message(device_eui)
                        if success:
                            print(f"Nachricht an {device_eui} gesendet")

if __name__ == "__main__":
    main()
```

## Verfügbare Funktionen

### Verbindung & Authentifizierung
- `check_melita_connection()` - Testet die Verbindung zu Melita.io
- `generate_melita_bearer_token()` - Generiert einen Bearer Token
- `get_melita_headers()` - Gibt HTTP-Header mit Token zurück
- `refresh_melita_token_if_needed()` - Erneuert Token bei Bedarf

### Queue-Verwaltung
- `flush_melita_device_queue(device_eui)` - Leert die Device-Queue
- `send_melita_queue_message(device_eui, data, fport, confirmed)` - Sendet Queue-Nachricht

### Temperatur-Synchronisation für vicki-Devices
- `create_temperature_hex_payload(min_temp, max_temp)` - Erstellt Hex-Payload für Temperaturdaten
- `hex_to_base64(hex_string)` - Konvertiert Hex zu Base64
- `send_temperature_to_vicki_device(device_eui, min_temp, max_temp, fport)` - Sendet Temperaturdaten an ein Device
- `send_temperature_to_all_vicki_devices(devices_data, fport)` - Sendet Temperaturdaten an alle Devices

### Daten abrufen
- `get_melita_devices(contract_id)` - Holt alle Devices (optional gefiltert nach Contract)
- `get_melita_contracts()` - Holt alle verfügbaren Contracts

### Hilfsfunktionen
- `is_melita_connected()` - Prüft ob Verbindung besteht
- `get_melita_token_info()` - Gibt Token-Informationen zurück

## Parameter

### `send_melita_queue_message()`
- `device_eui` (erforderlich): EUI des Ziel-Devices
- `data` (optional): Nachrichtendaten (Standard: "FRg=")
- `fport` (optional): FPort (Standard: 2)
- `confirmed` (optional): Bestätigte Nachricht (Standard: False)

### `send_temperature_to_vicki_device()`
- `device_eui` (erforderlich): EUI des vicki-Devices
- `min_temp` (erforderlich): Minimale Temperatur in °C
- `max_temp` (erforderlich): Maximale Temperatur in °C
- `operational_mode` (optional): Operational Mode (2/10 → aktiviert, sonst → deaktiviert)
- `fport` (optional): FPort (Standard: 2)

### `send_temperature_to_all_vicki_devices()`
- `devices_data` (erforderlich): Liste von Dictionaries mit `device_eui`, `min_temp`, `max_temp`, `operational_mode` (optional)
- `fport` (optional): FPort (Standard: 2)

### `get_melita_devices()`
- `contract_id` (optional): Contract ID für Filterung

## Temperatur-Payload Format

Die vicki-Devices erhalten einen speziellen Hex-Payload:

```
08 + minTemp (1 Byte) + maxTemp (1 Byte) + 0d + operationalMode (1 Byte) + 15 + 18
```

**Operational Mode:**
- **Mode 2 oder 10:** → `02` (aktiviert)
- **Alle anderen Modes:** → `00` (deaktiviert)

**Zusätzliche Hex-Werte:** `15` + `18` werden immer angehängt

**Beispiel:**
- minTemp: 15°C → 15 (dezimal) → 0f (hex)
- maxTemp: 30°C → 30 (dezimal) → 1e (hex)
- operationalMode: 2 → 02 (hex)
- Zusätzliche Werte: 15 + 18
- **Hex-Payload:** `080f1e0d021518`
- **Base64:** `CA8eDQoVFg==`

**Gültiger Temperaturbereich:** 0-255°C (1 Byte pro Temperatur)
**Payload-Größe:** 7 Bytes (08 + minTemp + maxTemp + 0d + operationalMode + 15 + 18)

## Fehlerbehandlung

Alle Funktionen haben integrierte Fehlerbehandlung:
- Automatische Token-Erneuerung bei 403-Fehlern
- Timeout-Behandlung (30 Sekunden)
- Detaillierte Fehlermeldungen
- Graceful Fallbacks

## Beispiel-Ausgabe

```
🔑 Generiere Melita.io Bearer Token...
✅ Melita.io Bearer Token erfolgreich generiert
   Token: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...abc123
   ⏰ Token läuft ab: 2025-01-15 14:30:00

🌡️  Sende Temperaturdaten an vicki-Device 70b3d52dd3007c11
   minTemp: 15°C, maxTemp: 30°C
   Operational Mode: 2

🌡️  Temperatur-Payload erstellt:
   minTemp: 15°C -> 0f
   maxTemp: 30°C -> 1e
   🔧 Operational Mode: 2 → 02 (aktiviert)
   Hex-Payload: 080f1e0d021518 (7 Bytes)
   Zusätzliche Hex-Werte: 15 + 18

🔄 Hex zu Base64 konvertiert:
   Hex: 080f1e0d021518
   Base64: CA8eDQoVFg==

🧹 Leere Queue für Device 70b3d52dd3007c11...
✅ Queue erfolgreich geleert für 70b3d52dd3007c11

📤 Sende Queue-Nachricht an Device 70b3d52dd3007c11...
✅ Queue-Nachricht erfolgreich gesendet an 70b3d52dd3007c11

✅ Temperaturdaten erfolgreich an 70b3d52dd3007c11 gesendet
   Payload: CAASABg=
```

## Integration in bestehende Skripte

Ersetzen Sie die bestehenden Melita.io Funktionen in Ihren Skripten:

**Vorher:**
```python
# Lokale Melita-Funktionen
def generate_melita_bearer_token():
    # Implementation...
    pass
```

**Nachher:**
```python
# Zentrale Melita-Funktionen importieren
from heatmanager_common import generate_melita_bearer_token
```

## Beispielskript

Ein vollständiges Beispielskript finden Sie in `example_vicki_temperature_sync.py` im Hauptverzeichnis.

## Support

Bei Problemen oder Fragen wenden Sie sich an das Entwicklungsteam.
