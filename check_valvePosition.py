#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Valve Position Checker
======================
Prüft Geräte mit veralteten oder fehlenden Ventilpositionen und sendet Downlink-Nachrichten.

Das Skript führt eine SQL-Abfrage aus, um Geräte zu finden, deren Ventilposition
älter als 2 Stunden ist oder fehlt, und sendet für diese Geräte eine Downlink-Nachricht
über die LNS API.
"""

import os
import sys
import argparse
import json
import requests
from dotenv import load_dotenv

# Lade Umgebungsvariablen aus .env
load_dotenv()

# PostgreSQL Verbindungsdaten aus .env
PG_HOST = os.getenv('PG_HOST', 'localhost')
PG_PORT = os.getenv('PG_PORT', '5432')
PG_DATABASE = os.getenv('PG_DATABASE', 'hmreporting')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')

# LNS API Konfiguration
LNS_API_URL = os.getenv('LNS_API_URL', 'http://localhost:3000/api/lns/downlink')
LNS_API_KEY = os.getenv('LNS_API_KEY')

# SQL Query Parameter
TENANT_ID = '7e3d1eb0-5318-11ef-a22a-49a76fa570ac'
BRAND = 'dnt'
DEVICE_TYPE = 'dnt-lw-etrv-c'


def parse_arguments():
    """Parst die Kommandozeilenargumente"""
    parser = argparse.ArgumentParser(
        description='Prüft Geräte mit veralteten Ventilpositionen und sendet Downlink-Nachrichten'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Nur anzeigen, keine API-Aufrufe durchführen'
    )
    parser.add_argument(
        '--pg-host',
        default=PG_HOST,
        help=f'PostgreSQL Host (Standard: {PG_HOST})'
    )
    parser.add_argument(
        '--pg-port',
        default=PG_PORT,
        help=f'PostgreSQL Port (Standard: {PG_PORT})'
    )
    parser.add_argument(
        '--pg-database',
        default=PG_DATABASE,
        help=f'PostgreSQL Datenbank (Standard: {PG_DATABASE})'
    )
    parser.add_argument(
        '--pg-user',
        default=PG_USER,
        help='PostgreSQL Benutzer (Standard: aus .env)'
    )
    parser.add_argument(
        '--pg-password',
        default=PG_PASSWORD,
        help='PostgreSQL Passwort (Standard: aus .env)'
    )
    parser.add_argument(
        '--lns-api-url',
        default=LNS_API_URL,
        help=f'LNS API URL (Standard: {LNS_API_URL})'
    )
    parser.add_argument(
        '--lns-api-key',
        default=LNS_API_KEY,
        help='LNS API Key (Standard: aus .env)'
    )
    
    return parser.parse_args()


def get_database_connection(host, port, database, user, password):
    """
    Erstellt eine Verbindung zur PostgreSQL-Datenbank
    
    Args:
        host: PostgreSQL Host
        port: PostgreSQL Port
        database: Datenbankname
        user: Benutzername
        password: Passwort
    
    Returns:
        psycopg2.Connection: Datenbankverbindung oder None bei Fehler
    """
    try:
        import psycopg2
        
        if not all([host, database, user, password]):
            print("❌ Fehlende PostgreSQL-Verbindungsdaten", file=sys.stderr)
            print("   Bitte PG_HOST, PG_DATABASE, PG_USER, PG_PASSWORD in .env setzen", file=sys.stderr)
            return None
        
        conn_str = f"host={host} port={port} dbname={database} user={user} password={password}"
        conn = psycopg2.connect(conn_str)
        print(f"✅ PostgreSQL-Verbindung erfolgreich: {host}:{port}/{database}")
        return conn
        
    except ImportError:
        print("❌ psycopg2 nicht installiert. Bitte installieren Sie es mit:", file=sys.stderr)
        print("   pip install psycopg2-binary", file=sys.stderr)
        return None
    except Exception as e:
        print(f"❌ Fehler bei der PostgreSQL-Verbindung: {e}", file=sys.stderr)
        return None


def execute_query(conn):
    """
    Führt die SQL-Abfrage aus, um Geräte mit veralteten Ventilpositionen zu finden
    
    Args:
        conn: PostgreSQL-Verbindung
    
    Returns:
        list: Liste von Dictionaries mit den Ergebnissen
    """
    query = """
    SELECT *
    FROM hmreporting.v_device_valve_last
    WHERE (percent_valve_open_ts_utc IS NULL
       OR percent_valve_open_ts_utc < now() - interval '2 hour' )
    AND tenant_id = %s
    AND brand = %s
    AND devicetype IN %s
    """
    
    try:
        cursor = conn.cursor()
        # Konvertiere DEVICE_TYPE zu einem Tuple für IN-Klausel
        device_types = tuple([DEVICE_TYPE] if isinstance(DEVICE_TYPE, str) else DEVICE_TYPE)
        cursor.execute(query, (TENANT_ID, BRAND, device_types))
        
        # Hole Spaltennamen
        columns = [desc[0] for desc in cursor.description]
        
        # Konvertiere Ergebnisse zu Dictionaries
        results = []
        for row in cursor.fetchall():
            row_dict = dict(zip(columns, row))
            results.append(row_dict)
        
        cursor.close()
        return results
        
    except Exception as e:
        print(f"❌ Fehler beim Ausführen der SQL-Abfrage: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        return []


def send_downlink(device_id, api_url, api_key, dry_run=False):
    """
    Sendet eine Downlink-Nachricht über die LNS API
    
    Args:
        device_id: Device ID
        api_url: LNS API URL
        api_key: LNS API Key
        dry_run: Wenn True, wird nur simuliert
    
    Returns:
        bool: True wenn erfolgreich, False sonst
    """
    if not api_key:
        print(f"❌ LNS_API_KEY fehlt in .env", file=sys.stderr)
        return False
    
    payload = {
        "deviceId": device_id,
        "frm_payload": "03F4",
        "confirmed": False,
        "priority": "NORMAL"
    }
    
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": api_key
    }
    
    if dry_run:
        print(f"🔍 DRY-RUN: Würde Downlink senden für Device {device_id}")
        print(f"   Payload: {json.dumps(payload, indent=2)}")
        return True
    
    try:
        response = requests.post(
            api_url,
            json=payload,
            headers=headers,
            timeout=30
        )
        
        if response.status_code in [200, 201, 202]:
            print(f"✅ Downlink erfolgreich gesendet für Device {device_id}")
            return True
        else:
            print(f"❌ Fehler beim Senden für Device {device_id}: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Fehler beim API-Aufruf für Device {device_id}: {e}", file=sys.stderr)
        return False


def main():
    """Hauptfunktion"""
    print("=" * 80)
    print("VALVE POSITION CHECKER")
    print("=" * 80)
    
    # Argumente parsen
    args = parse_arguments()
    
    # Validierung
    if not args.lns_api_key:
        print("❌ LNS_API_KEY fehlt. Bitte in .env setzen oder --lns-api-key angeben", file=sys.stderr)
        sys.exit(1)
    
    if not args.pg_user or not args.pg_password:
        print("❌ PostgreSQL-Benutzer oder Passwort fehlt", file=sys.stderr)
        print("   Bitte PG_USER und PG_PASSWORD in .env setzen", file=sys.stderr)
        sys.exit(1)
    
    # Datenbankverbindung herstellen
    print(f"\n1. Verbinde mit PostgreSQL-Datenbank...")
    conn = get_database_connection(
        args.pg_host,
        args.pg_port,
        args.pg_database,
        args.pg_user,
        args.pg_password
    )
    
    if not conn:
        print("❌ Keine Datenbankverbindung - Programm wird beendet", file=sys.stderr)
        sys.exit(1)
    
    try:
        # SQL-Abfrage ausführen
        print(f"\n2. Führe SQL-Abfrage aus...")
        print(f"   Tenant ID: {TENANT_ID}")
        print(f"   Brand: {BRAND}")
        print(f"   Device Type: {DEVICE_TYPE}")
        
        devices = execute_query(conn)
        
        if not devices:
            print(f"\n✅ Keine Geräte gefunden, die eine Downlink-Nachricht benötigen")
            return
        
        print(f"\n✅ {len(devices)} Gerät(e) gefunden, die eine Downlink-Nachricht benötigen")
        
        # Zeige gefundene Geräte an
        print(f"\n3. Gefundene Geräte:")
        for i, device in enumerate(devices, 1):
            device_id = device.get('device_id') or device.get('deviceId') or device.get('id')
            print(f"   {i}. Device ID: {device_id}")
            if 'percent_valve_open_ts_utc' in device:
                print(f"      Letzte Ventilposition: {device['percent_valve_open_ts_utc']}")
        
        # Sende Downlink-Nachrichten
        print(f"\n4. Sende Downlink-Nachrichten...")
        success_count = 0
        error_count = 0
        
        for device in devices:
            device_id = device.get('device_id') or device.get('deviceId') or device.get('id')
            
            if not device_id:
                print(f"⚠️  Gerät ohne Device ID übersprungen: {device}")
                error_count += 1
                continue
            
            if send_downlink(device_id, args.lns_api_url, args.lns_api_key, args.dry_run):
                success_count += 1
            else:
                error_count += 1
        
        # Zusammenfassung
        print(f"\n{'='*80}")
        print("ZUSAMMENFASSUNG")
        print(f"{'='*80}")
        print(f"Gefundene Geräte: {len(devices)}")
        print(f"Erfolgreich gesendet: {success_count}")
        print(f"Fehler: {error_count}")
        
        if args.dry_run:
            print(f"\n🔍 DRY-RUN Modus - Keine tatsächlichen API-Aufrufe durchgeführt")
        
    finally:
        # Datenbankverbindung schließen
        conn.close()
        print(f"\n✅ Datenbankverbindung geschlossen")


if __name__ == "__main__":
    main()

