#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Zentrale Konfigurationsdatei für Heatmanager Python-Skripte
Enthält alle gemeinsamen Einstellungen und Konfigurationen
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Bestimme das Projektverzeichnis (Verzeichnis mit .env Datei)
# Gehe vom aktuellen Modul-Verzeichnis aus nach oben zum Projektroot
current_dir = Path(__file__).parent
project_root = current_dir.parent  # heatmanager_common -> pythonscripts

# .env Datei laden - versuche zuerst im Projektroot, dann im aktuellen Verzeichnis
env_path = project_root / '.env'
if not env_path.exists():
    env_path = current_dir / '.env'
if not env_path.exists():
    env_path = Path('.env')  # Fallback: aktuelles Arbeitsverzeichnis

load_dotenv(dotenv_path=env_path)

# ThingsBoard Konfiguration
THINGSBOARD_BASE_URL = "https://webapp02.heatmanager.cloud"
THINGSBOARD_USERNAME = os.getenv('THINGBOARD_USERNAME')
THINGSBOARD_PASSWORD = os.getenv('THINGBOARD_PASSWORD')

# Entferne Whitespace falls vorhanden (häufiges Problem bei .env Dateien)
if THINGSBOARD_USERNAME:
    THINGSBOARD_USERNAME = THINGSBOARD_USERNAME.strip()
if THINGSBOARD_PASSWORD:
    THINGSBOARD_PASSWORD = THINGSBOARD_PASSWORD.strip()

# Melita.io Konfiguration
MELITA_BASE_URL = "https://www.melita.io"
MELITA_API_KEY = os.getenv('MELITA_API_KEY')

# Agility Thingspark Konfiguration
AGILITY_URL = os.getenv('AGILITY_URL')

# Database Konfiguration
DB_SERVER = os.getenv('MSSQL_SERVER')
DB_DATABASE = os.getenv('MSSQL_DATABASE')
DB_USERNAME = os.getenv('MSSQL_USER')
DB_PASSWORD = os.getenv('MSSQL_PASSWORD')

# HTTP Konfiguration
REQUEST_TIMEOUT = 30
REQUEST_RETRIES = 3

# Melita.io Standard-Parameter
MELITA_DEFAULT_DATA = "FRg="
MELITA_DEFAULT_FPORT = 2
MELITA_DEFAULT_CONFIRMED = False

# Logging Konfiguration
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# API Endpunkte
THINGSBOARD_ENDPOINTS = {
    'login': '/api/auth/login',
    'assets': '/api/tenant/assets',
    'devices': '/api/tenant/devices',
    'asset_attributes': '/api/plugins/telemetry/ASSET/{asset_id}/values/attributes',
    'device_rpc': '/api/device/{device_id}/rpc'
}

MELITA_ENDPOINTS = {
    'auth': '/api/iot-gateway/auth/generate',
    'contracts': '/api/iot-gateway/contracts',
    'devices': '/api/iot-gateway/lorawan/devices',
    'device_queue': '/api/iot-gateway/lorawan/{device_eui}/queue'
}
