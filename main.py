from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import datetime
import threading
import time
import math
import json
import os

try:
    import paho.mqtt.client as mqtt_client
    MQTT_AVAILABLE = True
except ImportError:
    MQTT_AVAILABLE = False
    print("WARNING: paho-mqtt not installed. Zigbee control will not work.")
    print("Install with: pip install paho-mqtt")

try:
    import requests as http_requests
except ImportError:
    http_requests = None
    print("WARNING: requests not installed. ESP32 polling will not work.")
    print("Install with: pip install requests")

app = Flask(__name__)
CORS(app)

# ---------- Конфигурация ----------
MQTT_BROKER_HOST = os.environ.get('MQTT_BROKER_HOST', 'localhost')
MQTT_BROKER_PORT = int(os.environ.get('MQTT_BROKER_PORT', '1883'))
MQTT_USERNAME = os.environ.get('MQTT_USERNAME', '')
MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD', '')
SERVER_PORT = int(os.environ.get('SERVER_PORT', '8080'))

# Файл для сохранения IP ESP32
ESP32_IP_FILE = 'esp32_ip.txt'

# Таймауты для сканирования ESP32
ESP32_SCAN_TCP_TIMEOUT = 0.8
ESP32_SCAN_HTTP_TIMEOUT = 1.0

# Статус инициализации
initialization_status = {
    "mqtt_connected": False,
    "esp32_polling": False,
    "esp32_reachable": False,
    "devices_loaded": False,
    "ready": False
}


def load_esp32_ip():
    """Загрузить сохранённый IP ESP32"""
    env_ip = os.environ.get('ESP32_IP', '')
    if env_ip:
        return env_ip
    try:
        with open(ESP32_IP_FILE, 'r') as f:
            return f.read().strip()
    except Exception:
        return ''


def save_esp32_ip(ip):
    """Сохранить IP ESP32 в файл"""
    try:
        with open(ESP32_IP_FILE, 'w') as f:
            f.write(ip)
    except Exception:
        pass


ESP32_IP = load_esp32_ip()

# ---------- BLE Client для подключения к ESP32 ----------
try:
    from bleak import BleakScanner, BleakClient
    from bleak.backends.characteristic import BleakGATTCharacteristic
    BLE_AVAILABLE = True
except ImportError:
    BLE_AVAILABLE = False
    print("WARNING: bleak not installed. BLE config will not work.")
    print("Install with: pip install bleak")

BLE_SERVICE_UUID = "12345678-1234-5678-1234-56789abcdef0"
BLE_SSID_UUID = "12345678-1234-5678-1234-56789abcdef1"
BLE_PASSWORD_UUID = "12345678-1234-5678-1234-56789abcdef2"
BLE_STATUS_UUID = "12345678-1234-5678-1234-56789abcdef3"

ble_wifi_ssid = ""
ble_wifi_password = ""
zigbee_devices = {}
zigbee_devices_lock = threading.Lock()
polling_running = False
last_poll_success = None
last_poll_error = None
esp32_error_count = 0  # Счётчик ошибок подряд
ESP32_MAX_ERRORS_BEFORE_FLAG = 3
esp32_polling_waiting = False  # True если polling запущен в режиме ожидания ESP32


# ---------- MQTT клиент ----------
class ZigbeeMqttClient:
    def __init__(self):
        self.client = None
        self.connected = False
        self._lock = threading.Lock()

    def connect(self, host=MQTT_BROKER_HOST, port=MQTT_BROKER_PORT,
                username=MQTT_USERNAME, password=MQTT_PASSWORD):
        if not MQTT_AVAILABLE:
            print("MQTT not available - paho-mqtt not installed")
            return False
        try:
            client_id = f"airquality_server_{int(time.time())}"
            self.client = mqtt_client.Client(client_id=client_id)

            if username:
                self.client.username_pw_set(username, password)

            self.client.on_connect = self._on_connect
            self.client.on_message = self._on_message
            self.client.on_disconnect = self._on_disconnect

            self.client.connect(host, port, keepalive=60)
            self.client.loop_start()
            print(f"Connecting to MQTT broker at {host}:{port}")
            return True
        except Exception as e:
            print(f"Failed to connect to MQTT: {e}")
            return False

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            initialization_status["mqtt_connected"] = True
            print("Connected to MQTT broker")
            client.subscribe("zigbee2mqtt/bridge/devices", 1)
            client.subscribe("zigbee2mqtt/bridge/state", 1)
            client.subscribe("zigbee2mqtt/bridge/log", 1)
            client.subscribe("zigbee2mqtt/+", 1)
            client.publish("zigbee2mqtt/bridge/config/devices/get", "")
        else:
            print(f"MQTT connection failed with code {rc}")

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        initialization_status["mqtt_connected"] = False
        print(f"Disconnected from MQTT broker (rc={rc})")

    def _on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            payload = msg.payload.decode('utf-8', errors='ignore')
            if topic == "zigbee2mqtt/bridge/devices":
                self._handle_devices_list(payload)
            elif topic.startswith("zigbee2mqtt/") and "bridge" not in topic:
                device_name = topic.replace("zigbee2mqtt/", "")
                self._handle_device_state(device_name, payload)
        except Exception as e:
            print(f"Error handling MQTT message: {e}")

    def _handle_devices_list(self, payload):
        try:
            devices = json.loads(payload)
            with zigbee_devices_lock:
                for dev in devices:
                    name = dev.get("friendly_name", "")
                    if name and name != "Coordinator":
                        zigbee_devices[name] = {
                            "friendly_name": name,
                            "ieee_address": dev.get("ieee_address", ""),
                            "model_id": dev.get("model_id", ""),
                            "type": dev.get("type", ""),
                            "is_online": dev.get("disabled", False) == False,
                            "state": "OFF",
                            "power": None, "voltage": None,
                            "current": None, "energy": None
                        }
                        save_zigbee_device_to_db(zigbee_devices[name])
            print(f"Updated {len(zigbee_devices)} devices")
            initialization_status["devices_loaded"] = True
        except json.JSONDecodeError as e:
            print(f"Error parsing devices list: {e}")

    def _handle_device_state(self, device_name, payload):
        try:
            data = json.loads(payload)
            with zigbee_devices_lock:
                if device_name not in zigbee_devices:
                    zigbee_devices[device_name] = {
                        "friendly_name": device_name,
                        "ieee_address": "", "model_id": "", "type": "",
                        "is_online": True, "state": "OFF",
                        "power": None, "voltage": None,
                        "current": None, "energy": None
                    }
                device = zigbee_devices[device_name]
                for key in ("state", "power", "voltage", "current", "energy"):
                    if key in data:
                        device[key] = data[key]
                device["is_online"] = True
                save_zigbee_device_to_db(device)
        except json.JSONDecodeError:
            pass

    def set_device_state(self, device_name, state):
        if not self.connected or not self.client:
            return False
        try:
            self.client.publish(
                f"zigbee2mqtt/{device_name}/set",
                json.dumps({"state": state}), qos=1
            )
            return True
        except Exception as e:
            print(f"Error setting device state: {e}")
            return False

    def request_devices_list(self):
        if not self.connected or not self.client:
            return False
        try:
            self.client.publish("zigbee2mqtt/bridge/config/devices/get", "")
            return True
        except Exception:
            return False

    def disconnect(self):
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
        self.connected = False


mqtt_zigbee = ZigbeeMqttClient()


# ---------- База данных ----------
def init_db():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS readings
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                  temperature REAL, humidity REAL,
                  tvoc INTEGER, co2 INTEGER, pm25 INTEGER,
                  overallQuality REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS automation_rules
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                   device_name TEXT NOT NULL,
                   sensor TEXT NOT NULL,
                   operator TEXT NOT NULL,
                   value REAL NOT NULL,
                   action TEXT NOT NULL,
                   enabled INTEGER DEFAULT 1,
                   last_triggered TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS zigbee_devices
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  ieee_address TEXT UNIQUE,
                  friendly_name TEXT NOT NULL,
                  model_id TEXT,
                  device_type TEXT,
                  is_online INTEGER DEFAULT 1,
                  state TEXT DEFAULT 'OFF',
                  power REAL, voltage REAL, current REAL, energy REAL,
                  last_updated DATETIME DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()
    conn.close()


def save_zigbee_device_to_db(device):
    """Сохранить устройство в БД"""
    try:
        conn = sqlite3.connect('data.db')
        c = conn.cursor()
        c.execute('''INSERT OR REPLACE INTO zigbee_devices 
                     (ieee_address, friendly_name, model_id, device_type, is_online, 
                      state, power, voltage, current, energy, last_updated)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)''',
                  (device.get('ieee_address', ''), device.get('friendly_name', ''),
                   device.get('model_id', ''), device.get('type', ''),
                   1 if device.get('is_online', True) else 0,
                   device.get('state', 'OFF'),
                   device.get('power'), device.get('voltage'),
                   device.get('current'), device.get('energy')))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] Error saving zigbee device: {e}")


def load_zigbee_devices_from_db():
    """Загрузить устройства из БД при старте"""
    global zigbee_devices
    try:
        conn = sqlite3.connect('data.db')
        c = conn.cursor()
        c.execute('SELECT ieee_address, friendly_name, model_id, device_type, is_online, state, power, voltage, current, energy FROM zigbee_devices')
        rows = c.fetchall()
        conn.close()
        
        with zigbee_devices_lock:
            zigbee_devices = {}
            for row in rows:
                ieee_addr, name, model, dtype, online, state, power, voltage, current, energy = row
                if name:
                    zigbee_devices[name] = {
                        "friendly_name": name,
                        "ieee_address": ieee_addr or "",
                        "model_id": model or "",
                        "type": dtype or "",
                        "is_online": bool(online),
                        "state": state or "OFF",
                        "power": power,
                        "voltage": voltage,
                        "current": current,
                        "energy": energy
                    }
        
        if zigbee_devices:
            print(f"[DB] Loaded {len(zigbee_devices)} zigbee devices from database")
            initialization_status["devices_loaded"] = True
    except Exception as e:
        print(f"[DB] Error loading zigbee devices: {e}")


init_db()

# Кеш для отслеживания срабатываний и ручных переключений
rule_trigger_cache = {}  # {(rule_id, action): timestamp}
manual_override_cache = {}  # {device_name: timestamp} — время последнего ручного переключения
MANUAL_OVERRIDE_COOLDOWN = 60  # секунд блокировки автоматики после ручного переключения


def save_reading(temperature, humidity, tvoc, co2, pm25, overallQuality):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''INSERT INTO readings
                 (temperature, humidity, tvoc, co2, pm25, overallQuality)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (temperature, humidity, tvoc, co2, pm25, overallQuality))
    conn.commit()
    conn.close()


def safe_int(value, default=0):
    try:
        if value is None:
            return default
        result = int(value)
        if math.isnan(result) or math.isinf(result):
            return default
        return max(0, min(result, 999999))
    except (ValueError, TypeError):
        return default


def safe_float(value, default=0.0):
    try:
        if value is None:
            return default
        result = float(value)
        if math.isnan(result) or math.isinf(result):
            return default
        return max(-100.0, min(result, 999.0))
    except (ValueError, TypeError):
        return default


def calculate_quality(co2, tvoc):
    try:
        co2 = float(co2)
        tvoc = float(tvoc)
        if tvoc == 0 and co2 <= 400:
            return 95.0
        quality = 100.0 - (co2 / 40.0 + tvoc / 10.0) / 2.0
        if math.isnan(quality) or math.isinf(quality):
            return 95.0
        return max(0.0, min(100.0, quality))
    except Exception:
        return 95.0


def get_latest_reading():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''SELECT temperature, humidity, tvoc, co2, pm25, overallQuality
                 FROM readings ORDER BY timestamp DESC LIMIT 1''')
    row = c.fetchone()
    conn.close()
    if row:
        temp = safe_float(row[0])
        hum = safe_float(row[1])
        tv = safe_int(row[2])
        c2 = safe_int(row[3])
        pm = safe_int(row[4])
        qual = safe_float(row[5])
        if qual == 0.0 and temp > 0:
            qual = calculate_quality(c2, tv)
        return {
            'temperature': temp, 'humidity': hum,
            'tvoc': tv, 'co2': c2, 'pm25': pm,
            'overallQuality': qual
        }
    return None


# ---------- Опрос ESP32 ----------
def poll_esp32():
    """Фоновый поток: опрашивает ESP32 и сохраняет данные"""
    global polling_running, last_poll_success, last_poll_error, esp32_error_count
    polling_running = True
    poll_count = 0
    esp32_error_count = 0
    initialization_status["esp32_polling"] = True
    print(f"[POLL] ESP32 polling started, target: http://{ESP32_IP}:8080/data")

    while polling_running:
        try:
            if http_requests and ESP32_IP:
                url = f"http://{ESP32_IP}:8080/data"
                try:
                    resp = http_requests.get(url, timeout=5)
                    if resp.status_code == 200:
                        data = resp.json()
                        temp = safe_float(data.get('temperature'), 20.0)
                        hum = safe_float(data.get('humidity'), 50.0)
                        tvoc = safe_int(data.get('tvoc'), 0)
                        co2 = safe_int(data.get('co2'), 400)
                        pm25 = safe_int(data.get('pm25'), 0)
                        quality = calculate_quality(co2, tvoc)
                        save_reading(temp, hum, tvoc, co2, pm25, quality)
                        poll_count += 1
                        esp32_error_count = 0
                        initialization_status["esp32_reachable"] = True
                        # IP уже валидный - подтверждаем его
                        print(f"[POLL] Confirmed ESP32 at {ESP32_IP}")
                        last_poll_success = {
                            "count": poll_count,
                            "data": {"temp": temp, "hum": hum, "co2": co2, "tvoc": tvoc}
                        }
                        if poll_count % 12 == 0:
                            print(f"[POLL] OK #{poll_count}: T={temp} H={hum} CO2={co2} TVOC={tvoc}")
                    else:
                        last_poll_error = f"HTTP {resp.status_code}"
                        esp32_error_count += 1
                        print(f"[POLL] ESP32 returned status {resp.status_code}")
                except http_requests.exceptions.ConnectionError:
                    last_poll_error = "Connection refused"
                    esp32_error_count += 1
                    if poll_count == 0:
                        print(f"[POLL] Cannot connect to ESP32 at {ESP32_IP}:8080")
                except http_requests.exceptions.Timeout:
                    last_poll_error = "Timeout"
                    esp32_error_count += 1
                except Exception as e:
                    last_poll_error = str(e)
                    esp32_error_count += 1
                    print(f"[POLL] Error: {e}")
            else:
                last_poll_error = "requests not available or ESP32_IP not set"
                esp32_error_count += 1
                time.sleep(10)
        except Exception as e:
            print(f"[POLL] Outer error: {e}")

        time.sleep(5)


# ---------- Эндпоинты ----------
@app.route('/ping', methods=['GET'])
def ping():
    return jsonify({"status": "ok"}), 200


@app.route('/debug/poll', methods=['GET'])
def debug_poll():
    """Диагностика опроса ESP32"""
    reading = get_latest_reading()
    return jsonify({
        "esp32_ip": ESP32_IP,
        "polling_running": polling_running,
        "requests_available": http_requests is not None,
        "last_poll_success": last_poll_success,
        "last_poll_error": last_poll_error,
        "last_reading": reading
    }), 200


@app.route('/esp32/ip', methods=['POST'])
def set_esp32_ip():
    """Обновить IP ESP32 без перезапуска сервера"""
    global ESP32_IP, polling_running, last_poll_error
    data = request.get_json()
    if not data or 'ip' not in data:
        return jsonify({"error": "Provide 'ip' in JSON body"}), 400
    
    new_ip = data['ip']
    ESP32_IP = new_ip
    save_esp32_ip(new_ip)
    last_poll_error = None
    print(f"[POLL] ESP32 IP updated to: {ESP32_IP}")
    
    if not polling_running and http_requests:
        t = threading.Thread(target=poll_esp32, daemon=True)
        t.start()
    
    return jsonify({"status": "ok", "esp32_ip": ESP32_IP}), 200


@app.route('/esp32/ping', methods=['GET'])
def ping_esp32():
    """Проверить доступность ESP32"""
    if not http_requests or not ESP32_IP:
        return jsonify({"error": "requests not available or ESP32_IP not set"}), 500
    
    try:
        resp = http_requests.get(f"http://{ESP32_IP}:8080/ping", timeout=5)
        return jsonify({"status": "ok", "esp32_ip": ESP32_IP, "reachable": True}), 200
    except Exception as e:
        return jsonify({"status": "error", "esp32_ip": ESP32_IP, "reachable": False, "error": str(e)}), 503


@app.route('/esp32/configure', methods=['POST'])
def configure_esp32():
    """Настроить ESP32 с новыми параметрами WiFi - асинхронно"""
    global ESP32_IP, polling_running
    
    print("[CONFIG] Received configure request")
    
    data = request.get_json()
    if not data:
        print("[CONFIG] No JSON data")
        return jsonify({"error": "No JSON data"}), 400
    
    wifi_ssid = data.get('wifi_ssid', '')
    wifi_password = data.get('wifi_password', '')
    
    print(f"[CONFIG] SSID: {wifi_ssid}, method is async")
    
    if not wifi_ssid:
        return jsonify({"error": "wifi_ssid is required"}), 400
    
    # Сразу возвращаем успех - настройка пойдёт асинхронно
    # Запускаем настройку в фоновом потоке
    def do_config():
        try:
            time.sleep(2)  # Даём время ESP32 переключиться в режим если нужно
            send_wifi_via_ble_thread(wifi_ssid, wifi_password)
        except Exception as e:
            print(f"[CONFIG] Async error: {e}")
    
    threading.Thread(target=do_config, daemon=True).start()
    
    return jsonify({
        "status": "configuring", 
        "message": "WiFi config is being sent to ESP32 via BLE. This takes 20-40 seconds."
    }), 200
    
    print(f"[CONFIG] Target ESP32 IP: {target_ip}")
    
    # Проверяем работоспособность текущего IP
    current_ip_works = False
    if target_ip:
        try:
            resp = http_requests.get(f"http://{target_ip}:8080/ping", timeout=2)
            if resp.status_code == 200:
                current_ip_works = True
                print(f"[CONFIG] Current IP {target_ip} works")
        except Exception as e:
            print(f"[CONFIG] Current IP {target_ip} not reachable: {e}")
    
    # Если IP не работает - пробуем BLE
    if not current_ip_works:
        print("[CONFIG] WiFi not works, trying BLE...")
        
        # Пробуем отправить по BLE
        ble_result = False
        try:
            ble_result = send_wifi_via_ble_thread(wifi_ssid, wifi_password)
        except Exception as e:
            print(f"[CONFIG] BLE failed: {e}")
        
        if ble_result:
            return jsonify({
                "status": "configured", 
                "method": "ble",
                "message": "WiFi config sent via BLE. ESP32 will connect to WiFi."
            }), 200
        
        # BLE тоже не работает - пробуем найти по WiFi
        print("[CONFIG] BLE failed, starting WiFi scan...")
        threading.Thread(target=auto_scan_esp32, daemon=True).start()
        
        # Ждём немного и пробуем получить новый IP
        time.sleep(3)
        if ESP32_IP:
            target_ip = ESP32_IP
            print(f"[CONFIG] Using new ESP32 IP: {target_ip}")
        else:
            return jsonify({
                "status": "error", 
                "message": "Cannot reach ESP32. Try: 1)blespress BTN to reset WiFi, 2) send WiFi via BLE from phone"
            }), 503
    
    try:
        config_data = {
            "wifi_ssid": wifi_ssid,
            "wifi_password": wifi_password
        }
        
        print(f"[CONFIG] Sending WiFi config to ESP32 at {target_ip}")
        resp = http_requests.post(
            f"http://{target_ip}:8080/configure",
            json=config_data,
            timeout=15
        )
        
        if resp.status_code == 200:
            # Обновляем IP и перезапускаем polling
            ESP32_IP = target_ip
            save_esp32_ip(ESP32_IP)
            last_poll_error = None
            polling_running = False
            time.sleep(0.5)
            polling_running = True
            t = threading.Thread(target=poll_esp32, daemon=True)
            t.start()
            
            return jsonify({"status": "configured", "esp32_ip": target_ip}), 200
        else:
            return jsonify({"error": f"ESP32 returned {resp.status_code}"}), 500
            
    except http_requests.exceptions.ConnectionError:
        return jsonify({"error": "Cannot connect to ESP32. Make sure it's reachable."}), 503
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------- BLE функция для отправки WiFi на ESP32 ----------
async def send_wifi_via_ble(ssid: str, password: str) -> bool:
    """Отправить WiFi данные на ESP32 по BLE"""
    if not BLE_AVAILABLE:
        print("[BLE] Bleak not available")
        return False
    
    try:
        from bleak import BleakScanner
        print("[BLE] Scanning for ESP32...")
        
        # Сканируем устройства (новый API)
        scanner = BleakScanner()
        devices = await scanner.discover(timeout=10)
        
        esp32_device = None
        for d in devices:
            name = d.name or ""
            print(f"[BLE] Found: {name} ({d.address})")
            if "AirQuality" in name or "ESP32" in name:
                esp32_device = d
                break
        
        if not esp32_device:
            print("[BLE] ESP32 not found via BLE")
            return False
        
        print(f"[BLE] Connecting to {esp32_device.name}...")
        
        async with BleakClient(esp32_device.address) as client:
            # Получаем сервис
            service = client.services.get_service(BLE_SERVICE_UUID)
            if not service:
                print("[BLE] Service not found")
                return False
            
            # Записываем SSID
            ssid_char = service.get_characteristic(BLE_SSID_UUID)
            if ssid_char:
                await client.write_gatt_char(ssid_char, ssid.encode())
                print(f"[BLE] SSID sent: {ssid}")
            
            # Записываем пароль
            pwd_char = service.get_characteristic(BLE_PASSWORD_UUID)
            if pwd_char:
                await client.write_gatt_char(pwd_char, password.encode())
                print("[BLE] Password sent")
            
            print("[BLE] WiFi config sent successfully!")
            return True
            
    except Exception as e:
        print(f"[BLE] Error: {e}")
        return False


def send_wifi_via_ble_thread(ssid: str, password: str) -> bool:
    """Запустить BLE отправку в отдельном потоке"""
    try:
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        result = loop.run_until_complete(send_wifi_via_ble(ssid, password))
        loop.close()
        return result
    except Exception as e:
        print(f"[BLE] Thread error: {e}")
        return False


@app.route('/esp32/scan', methods=['POST'])
def scan_esp32():
    """Автопоиск ESP32 в локальной сети"""
    global ESP32_IP, last_poll_error, polling_running

    import socket as sock
    import struct

    # Получаем IP сервера для определения подсети
    try:
        s = sock.socket(sock.AF_INET, sock.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        return jsonify({"error": "Cannot determine local IP"}), 500

    parts = local_ip.split(".")
    subnet = f"{parts[0]}.{parts[1]}.{parts[2]}"

    found_devices = []

    # Метод 1: TCP discovery (порт 8082) - быстрый
    def check_tcp_discovery(ip):
        if ip == local_ip:
            return None  # Пропускаем свой IP
        try:
            s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
            s.settimeout(0.8)
            s.connect((ip, 8082))
            s.send(b"DISCOVER")
            data = s.recv(1024).decode()
            s.close()
            if "AIR_QUALITY_SENSOR" in data:
                return {"ip": ip, "method": "tcp_discovery", "info": data}
        except Exception:
            pass
        return None

    # Метод 2: HTTP /data (порт 8080) - запасной
    def check_http(ip):
        if not http_requests:
            return None
        if ip == local_ip:
            return None  # Пропускаем свой IP
        try:
            resp = http_requests.get(f"http://{ip}:8080/data", timeout=1.0)
            if resp.status_code == 200:
                data = resp.json()
                if "co2" in data and "tvoc" in data:
                    return {"ip": ip, "method": "http", "data": data}
        except Exception:
            pass
        return None

    # Сканируем подсеть
    import concurrent.futures
    ips = [f"{subnet}.{i}" for i in range(1, 255)]

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        # Сначала пробуем TCP discovery
        futures = {executor.submit(check_tcp_discovery, ip): ip for ip in ips}
        try:
            for future in concurrent.futures.as_completed(futures, timeout=3):
                try:
                    result = future.result()
                    if result:
                        found_devices.append(result)
                        break
                except Exception:
                    pass
        except concurrent.futures.TimeoutError:
            pass

    # Если не нашли через TCP, пробуем HTTP
    if not found_devices:
        with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
            futures = {executor.submit(check_http, ip): ip for ip in ips}
            for future in concurrent.futures.as_completed(futures, timeout=5):
                try:
                    result = future.result()
                    if result:
                        found_devices.append(result)
                        break
                except Exception:
                    pass

    if found_devices:
        device = found_devices[0]
        new_ip = device["ip"]
        old_ip = ESP32_IP
        
        # Если IP тот же - ничего не делаем
        if new_ip == old_ip:
            print(f"[SCAN] ESP32 unchanged at {old_ip}")
            return jsonify({
                "status": "found",
                "devices": found_devices,
                "selected_ip": ESP32_IP
            }), 200
        
        # Если IP изменился - проверяем что старый не работает перед переключением
        if old_ip and http_requests:
            try:
                resp = http_requests.get(f"http://{old_ip}:8080/ping", timeout=2)
                if resp.status_code == 200:
                    print(f"[SCAN] Current ESP32 {old_ip} still works, keeping it")
                    ESP32_IP = old_ip  # Восстанавливаем старый IP
                    return jsonify({
                        "status": "found",
                        "devices": found_devices,
                        "selected_ip": ESP32_IP
                    }), 200
            except Exception:
                pass  # Старый не работает, переключаемся
        
        # Новый IP - используем его
        ESP32_IP = new_ip
        save_esp32_ip(ESP32_IP)
        last_poll_error = None
        print(f"[SCAN] Found ESP32 at {ESP32_IP}")

        # Перезапускаем polling
        if http_requests:
            polling_running = False
            time.sleep(0.5)
            polling_running = True
            t = threading.Thread(target=poll_esp32, daemon=True)
            t.start()

        return jsonify({
            "status": "found",
            "devices": found_devices,
            "selected_ip": ESP32_IP
        }), 200

    return jsonify({
        "status": "not_found",
        "subnet": subnet,
        "message": "ESP32 not found. Check WiFi connection."
    }), 404


@app.route('/data/clear', methods=['POST'])
def clear_data():
    """Очистить все данные из БД"""
    try:
        conn = sqlite3.connect('data.db')
        c = conn.cursor()
        c.execute('DELETE FROM readings')
        conn.commit()
        conn.close()
        return jsonify({"status": "ok", "message": "Data cleared"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ---------- Automation API ----------
@app.route('/api/automation/rules', methods=['GET'])
def get_rules():
    """Получить все правила автоматизации"""
    device_name = request.args.get('device', '')
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    if device_name:
        c.execute('SELECT * FROM automation_rules WHERE device_name = ?', (device_name,))
    else:
        c.execute('SELECT * FROM automation_rules')
    rows = c.fetchall()
    conn.close()
    
    rules = []
    for row in rows:
        rules.append({
            "id": row[0],
            "device_name": row[1],
            "sensor": row[2],
            "operator": row[3],
            "value": row[4],
            "action": row[5],
            "enabled": bool(row[6]),
            "last_triggered": row[7]
        })
    return jsonify({"rules": rules}), 200


@app.route('/api/automation/rules', methods=['POST'])
def create_rule():
    """Создать правило автоматизации"""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON data"}), 400
    
    required = ['device_name', 'sensor', 'operator', 'value', 'action']
    for field in required:
        if field not in data:
            return jsonify({"error": f"Missing field: {field}"}), 400
    
    valid_sensors = ['co2', 'tvoc', 'temperature', 'humidity']
    if data['sensor'] not in valid_sensors:
        return jsonify({"error": f"Invalid sensor. Use: {valid_sensors}"}), 400
    
    valid_operators = ['gt', 'lt', 'gte', 'lte']
    if data['operator'] not in valid_operators:
        return jsonify({"error": f"Invalid operator. Use: {valid_operators}"}), 400
    
    valid_actions = ['ON', 'OFF']
    if data['action'] not in valid_actions:
        return jsonify({"error": f"Invalid action. Use: {valid_actions}"}), 400
    
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''INSERT INTO automation_rules (device_name, sensor, operator, value, action, enabled)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (data['device_name'], data['sensor'], data['operator'],
               float(data['value']), data['action'], 1 if data.get('enabled', True) else 0))
    rule_id = c.lastrowid
    conn.commit()
    conn.close()
    
    return jsonify({"status": "ok", "id": rule_id}), 201


@app.route('/api/automation/rules/<int:rule_id>', methods=['DELETE'])
def delete_rule(rule_id):
    """Удалить правило"""
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('DELETE FROM automation_rules WHERE id = ?', (rule_id,))
    conn.commit()
    conn.close()
    return jsonify({"status": "ok"}), 200


@app.route('/api/automation/rules/<int:rule_id>', methods=['PUT'])
def update_rule(rule_id):
    """Обновить правило"""
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON data"}), 400
    
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    
    fields = []
    values = []
    for key in ['sensor', 'operator', 'value', 'action', 'enabled']:
        if key in data:
            fields.append(f'{key} = ?')
            values.append(data[key])
    
    if fields:
        values.append(rule_id)
        c.execute(f'UPDATE automation_rules SET {", ".join(fields)} WHERE id = ?', values)
        conn.commit()
    
    conn.close()
    return jsonify({"status": "ok"}), 200


@app.route('/api/automation/status/<device_name>', methods=['GET'])
def automation_status(device_name):
    """Статус автоматики для устройства"""
    now = time.time()
    last_manual = manual_override_cache.get(device_name, 0)
    manual_active = (now - last_manual) < MANUAL_OVERRIDE_COOLDOWN
    remaining = max(0, int(MANUAL_OVERRIDE_COOLDOWN - (now - last_manual))) if manual_active else 0
    
    return jsonify({
        "device_name": device_name,
        "manual_override_active": manual_active,
        "cooldown_remaining": remaining
    }), 200


def check_automation_rules():
    """Фоновая проверка правил автоматизации"""
    global rule_trigger_cache, manual_override_cache
    
    reading = get_latest_reading()
    if not reading:
        return
    
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('SELECT id, device_name, sensor, operator, value, action, enabled FROM automation_rules WHERE enabled = 1')
    rules = c.fetchall()
    conn.close()
    
    now = time.time()
    
    for rule in rules:
        rule_id, device_name, sensor, operator, value, action, enabled = rule
        
        # Проверяем — не было ли ручного переключения
        last_manual = manual_override_cache.get(device_name, 0)
        if now - last_manual < MANUAL_OVERRIDE_COOLDOWN:
            continue  # Пропускаем — пользователь недавно управлял вручную
        
        sensor_value = reading.get(sensor)
        if sensor_value is None:
            continue
        
        should_trigger = False
        if operator == 'gt' and sensor_value > value:
            should_trigger = True
        elif operator == 'lt' and sensor_value < value:
            should_trigger = True
        elif operator == 'gte' and sensor_value >= value:
            should_trigger = True
        elif operator == 'lte' and sensor_value <= value:
            should_trigger = True
        
        if should_trigger:
            cache_key = (rule_id, action)
            last_trigger = rule_trigger_cache.get(cache_key, 0)
            
            # Не чаще раза в 30 секунд
            if now - last_trigger > 30:
                success = mqtt_zigbee.set_device_state(device_name, action)
                if success:
                    rule_trigger_cache[cache_key] = now
                    conn = sqlite3.connect('data.db')
                    c = conn.cursor()
                    c.execute('UPDATE automation_rules SET last_triggered = ? WHERE id = ?',
                              (datetime.datetime.now().isoformat(), rule_id))
                    conn.commit()
                    conn.close()
                    print(f"[AUTO] Rule {rule_id}: {sensor} {operator} {value} -> {device_name} {action}")


@app.route('/data', methods=['POST'])
def receive_data():
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data'}), 400
        temperature = safe_float(data.get('temperature'), 20.0)
        humidity = safe_float(data.get('humidity'), 50.0)
        tvoc = safe_int(data.get('tvoc'), 0)
        co2 = safe_int(data.get('co2'), 400)
        pm25 = safe_int(data.get('pm25'), 0)
        overallQuality = calculate_quality(co2, tvoc)
        save_reading(temperature, humidity, tvoc, co2, pm25, overallQuality)
        return jsonify({'status': 'ok'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/data', methods=['GET'])
def get_data():
    reading = get_latest_reading()
    if reading:
        return jsonify(reading), 200
    return jsonify({
        'temperature': 0.0, 'humidity': 0.0,
        'tvoc': 0, 'co2': 0, 'pm25': 0, 'overallQuality': 0.0
    }), 200


@app.route('/latest', methods=['GET'])
def latest():
    reading = get_latest_reading()
    if reading:
        return jsonify(reading), 200
    return jsonify({'error': 'No data yet'}), 404


@app.route('/history', methods=['GET'])
def history():
    limit = request.args.get('limit', default=100, type=int)
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''SELECT timestamp, temperature, humidity, tvoc, co2, pm25, overallQuality
                 FROM readings ORDER BY timestamp DESC LIMIT ?''', (limit,))
    rows = c.fetchall()
    conn.close()
    return jsonify([{
        'timestamp': r[0], 'temperature': r[1], 'humidity': r[2],
        'tvoc': r[3], 'co2': r[4], 'pm25': r[5], 'overallQuality': r[6]
    } for r in rows]), 200


# ---------- Zigbee API ----------
@app.route('/api/zigbee/devices', methods=['GET'])
def get_zigbee_devices():
    with zigbee_devices_lock:
        devices_list = list(zigbee_devices.values())
    return jsonify({
        "count": len(devices_list),
        "devices": devices_list,
        "mqtt_connected": mqtt_zigbee.connected if mqtt_zigbee else False,
        "initialization_complete": initialization_status["ready"]
    }), 200


@app.route('/api/zigbee/devices/<device_name>', methods=['GET'])
def get_zigbee_device(device_name):
    with zigbee_devices_lock:
        device = zigbee_devices.get(device_name)
    if device:
        return jsonify(device), 200
    return jsonify({"error": "Device not found"}), 404


@app.route('/api/zigbee/devices/<device_name>/control', methods=['POST'])
def control_zigbee_device(device_name):
    global manual_override_cache
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON data"}), 400
    action = data.get("action", "").upper()
    if action not in ("ON", "OFF", "TOGGLE"):
        return jsonify({"error": "Invalid action"}), 400
    if not mqtt_zigbee.connected:
        return jsonify({"error": "MQTT not connected"}), 503
    
    # Конвертируем ieee_address в friendly_name если нужно
    mqtt_device_name = get_friendly_name(device_name)
    if not mqtt_device_name:
        mqtt_device_name = device_name  # Пробуем как есть
    
    print(f"[CONTROL] Sending {action} to {device_name} -> {mqtt_device_name}")
    
    # Проверяем — это ручное управление или от автоматики?
    from_automation = data.get("from_automation", False)
    if not from_automation:
        # Ручное управление — блокируем автоматику на 60 секунд
        manual_override_cache[device_name] = time.time()
        print(f"[MANUAL] {mqtt_device_name} {action} — automation blocked for {MANUAL_OVERRIDE_COOLDOWN}s")
    
    success = mqtt_zigbee.set_device_state(mqtt_device_name, action)
    if success:
        return jsonify({"status": "ok", "device": mqtt_device_name, "action": action}), 200
    return jsonify({"error": "Failed to send command"}), 500


def get_friendly_name(ieee_address):
    """Получить friendly_name по ieee_address"""
    try:
        conn = sqlite3.connect('data.db')
        c = conn.cursor()
        c.execute('SELECT friendly_name FROM zigbee_devices WHERE ieee_address = ?', (ieee_address,))
        row = c.fetchone()
        conn.close()
        if row:
            return row[0]
    except Exception:
        pass
    # Если не найден - пробуем использовать как friendly_name
    return ieee_address


@app.route('/api/zigbee/scan', methods=['POST'])
def scan_zigbee_devices():
    if not mqtt_zigbee.connected:
        return jsonify({"error": "MQTT not connected"}), 503
    if mqtt_zigbee.request_devices_list():
        return jsonify({"status": "scan_started"}), 200
    return jsonify({"error": "Failed"}), 500


@app.route('/api/zigbee/mqtt/status', methods=['GET'])
def mqtt_status():
    return jsonify({
        "connected": mqtt_zigbee.connected if mqtt_zigbee else False,
        "broker_host": MQTT_BROKER_HOST,
        "broker_port": MQTT_BROKER_PORT,
        "mqtt_available": MQTT_AVAILABLE
    }), 200


@app.route('/api/zigbee/mqtt/connect', methods=['POST'])
def mqtt_connect():
    data = request.get_json() or {}
    host = data.get("host", MQTT_BROKER_HOST)
    port = data.get("port", MQTT_BROKER_PORT)
    username = data.get("username", MQTT_USERNAME)
    password = data.get("password", MQTT_PASSWORD)
    mqtt_zigbee.disconnect()
    success = mqtt_zigbee.connect(host, port, username, password)
    if success:
        time.sleep(1)
        return jsonify({"status": "connecting", "broker": f"{host}:{port}"}), 200
    return jsonify({"error": "Failed to connect"}), 500


@app.route('/api/server/info', methods=['GET'])
def server_info():
    import socket
    try:
        local_ip = socket.gethostbyname(socket.gethostname())
    except Exception:
        local_ip = "unknown"
    
    esp32_status = "not_configured"
    if last_poll_success:
        esp32_status = "polling"
    elif ESP32_IP and last_poll_error:
        esp32_status = "error"
    elif ESP32_IP:
        esp32_status = "configured"
    
    return jsonify({
        "server_ip": local_ip,
        "server_port": SERVER_PORT,
        "mqtt_host": MQTT_BROKER_HOST,
        "mqtt_port": MQTT_BROKER_PORT,
        "mqtt_connected": mqtt_zigbee.connected if mqtt_zigbee else False,
        "zigbee_device_count": len(zigbee_devices),
        "esp32": {
            "status": esp32_status,
            "ip": ESP32_IP,
            "last_error": last_poll_error,
            "last_success": last_poll_success
        },
        "api_version": "1.0"
    }), 200


@app.route('/api/status/startup', methods=['GET'])
def startup_status():
    """Статус инициализации сервера"""
    global initialization_status
    
    # Обновить статус
    initialization_status["mqtt_connected"] = mqtt_zigbee.connected if mqtt_zigbee else False
    initialization_status["esp32_polling"] = polling_running
    initialization_status["esp32_reachable"] = last_poll_success is not None
    
    # Готов если MQTT подключен или есть устройства в БД
    ready = initialization_status["mqtt_connected"] or initialization_status["devices_loaded"]
    initialization_status["ready"] = ready
    
    return jsonify({
        "mqtt_connected": initialization_status["mqtt_connected"],
        "esp32_polling": initialization_status["esp32_polling"],
        "esp32_reachable": initialization_status["esp32_reachable"],
        "devices_loaded": initialization_status["devices_loaded"],
        "devices_count": len(zigbee_devices),
        "ready": ready
    }), 200


# ---------- Запуск ----------
def auto_scan_esp32():
    """Автопоиск ESP32 при запуске"""
    global ESP32_IP, polling_running, last_poll_error

    if not http_requests:
        return

    print("[SCAN] Auto-scanning for ESP32...")

    import socket as sock
    import concurrent.futures

    try:
        s = sock.socket(sock.AF_INET, sock.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
    except Exception:
        print("[SCAN] Cannot determine local IP")
        return

    parts = local_ip.split(".")
    subnet = f"{parts[0]}.{parts[1]}.{parts[2]}"
    ips = [f"{subnet}.{i}" for i in range(1, 255) if f"{subnet}.{i}" != local_ip]

    # Метод 1: TCP discovery (параллельно)
    def check_tcp(ip):
        try:
            s = sock.socket(sock.AF_INET, sock.SOCK_STREAM)
            s.settimeout(0.8)
            s.connect((ip, 8082))
            s.send(b"DISCOVER")
            data = s.recv(1024).decode()
            s.close()
            if "AIR_QUALITY_SENSOR" in data:
                return ip
        except Exception:
            pass
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(check_tcp, ip): ip for ip in ips}
        try:
            for future in concurrent.futures.as_completed(futures, timeout=5):
                result = future.result()
                if result:
                    ESP32_IP = result
                    save_esp32_ip(result)
                    last_poll_error = None
                    print(f"[SCAN] Found ESP32 at {ESP32_IP} (tcp_discovery)")
                    executor.shutdown(wait=False)
                    return
        except Exception:
            pass

    # Метод 2: HTTP (параллельно)
    def check_http(ip):
        try:
            resp = http_requests.get(f"http://{ip}:8080/data", timeout=ESP32_SCAN_HTTP_TIMEOUT)
            if resp.status_code == 200:
                data = resp.json()
                if "co2" in data and "tvoc" in data:
                    return ip
        except Exception:
            pass
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        futures = {executor.submit(check_http, ip): ip for ip in ips}
        try:
            for future in concurrent.futures.as_completed(futures, timeout=8):
                result = future.result()
                if result:
                    # Проверяем что это тот же IP который уже работает
                    if result == ESP32_IP:
                        print(f"[SCAN] ESP32 unchanged: {ESP32_IP}")
                        return
                    # Это новый IP - переключаемся
                    ESP32_IP = result
                    save_esp32_ip(result)
                    last_poll_error = None
                    print(f"[SCAN] Found ESP32 at {ESP32_IP} (http)")
                    return
        except Exception:
            pass

    print("[SCAN] ESP32 not found or unchanged")


def periodic_esp32_rescan():
    """Периодический повторный поиск ESP32 каждые 60 секунд"""
    global ESP32_IP, polling_running, last_poll_error
    print("[SCAN] Periodic ESP32 re-scan thread started")
    
    while True:
        time.sleep(60)
        
        # Если ESP32 уже найден и polling работает - пропускаем
        if ESP32_IP and last_poll_error is None:
            continue
        
        # Если есть сохранённый IP - пробуем пинговать
        if ESP32_IP:
            try:
                if http_requests:
                    resp = http_requests.get(f"http://{ESP32_IP}:8080/ping", timeout=2)
                    if resp.status_code == 200:
                        print(f"[SCAN] Periodic: ESP32 {ESP32_IP} is reachable")
                        if not polling_running:
                            last_poll_error = None
                            t = threading.Thread(target=poll_esp32, daemon=True)
                            t.start()
                        continue
                    else:
                        print(f"[SCAN] Periodic: ESP32 {ESP32_IP} returned {resp.status_code}, re-scanning...")
            except Exception as e:
                print(f"[SCAN] Periodic: ESP32 {ESP32_IP} unreachable ({e}), re-scanning...")
        
        # Если не найден или недоступен - запускаем auto-scan
        print("[SCAN] Periodic: re-scanning for ESP32...")
        try:
            auto_scan_esp32()
        except Exception as e:
            print(f"[SCAN] Periodic scan error: {e}")


if __name__ == '__main__':
    load_zigbee_devices_from_db()
    
    # Запустить BLE сервер для настройки WiFi
    if BLE_AVAILABLE:
        try:
            import asyncio
            asyncio.get_event_loop().run_until_complete(ble_server.start())
            print("[BLE] BLE server running")
        except Exception as e:
            print(f"[BLE] Could not start: {e}")
    
    if MQTT_AVAILABLE:
        mqtt_zigbee.connect()

    print(f"[START] Loaded ESP32_IP: {ESP32_IP}")
    
    if ESP32_IP:
        t = threading.Thread(target=poll_esp32, daemon=True)
        t.start()
    else:
        print("[START] ESP32_IP not set, running auto-scan...")
        auto_scan_esp32()
        print(f"[START] After scan, ESP32_IP: {ESP32_IP}")
        if ESP32_IP:
            t = threading.Thread(target=poll_esp32, daemon=True)
            t.start()
        else:
            print("[START] ESP32 not found. Use POST /esp32/scan to search later.")

    # Фоновый поток проверки автоматизации
    def automation_loop():
        print("[AUTO] Automation checker started")
        while True:
            try:
                check_automation_rules()
            except Exception as e:
                print(f"[AUTO] Error: {e}")
            time.sleep(10)
    
    t_auto = threading.Thread(target=automation_loop, daemon=True)
    t_auto.start()

    t_rescan = threading.Thread(target=periodic_esp32_rescan, daemon=True)
    t_rescan.start()

    print(f"Server starting on port {SERVER_PORT}")
    app.run(host='0.0.0.0', port=SERVER_PORT, debug=False)
