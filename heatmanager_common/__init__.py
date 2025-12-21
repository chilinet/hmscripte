# heatmanager_common package
# Zentrale Funktionen für Heatmanager Python-Skripte

from .melita import (
    generate_melita_bearer_token,
    get_melita_headers,
    send_melita_queue_message,
    flush_melita_device_queue,
    check_melita_connection,
    create_temperature_hex_payload,
    hex_to_base64,
    send_temperature_to_vicki_device,
    send_temperature_to_all_vicki_devices
)

__all__ = [
    'generate_melita_bearer_token',
    'get_melita_headers', 
    'send_melita_queue_message',
    'flush_melita_device_queue',
    'check_melita_connection',
    'create_temperature_hex_payload',
    'hex_to_base64',
    'send_temperature_to_vicki_device',
    'send_temperature_to_all_vicki_devices'
]
