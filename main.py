from flask import Flask, request, jsonify
from flask_cors import CORS
import tinytuya
import sqlite3
import threading
import time
import datetime

app = Flask(__name__)
CORS(app)

# ---------- Конфигурация Tuya ----------
# Замените на свои данные из Tuya IoT Platform
API_KEY = "4u5mvguw4xugvj9kvmyp"
API_SECRET = "a93b4f5cbf174f61919c0e041a47f980"
DEFAULT_DEVICE_ID = "bf939cadb80711fb2dh7jl"   # ID розетки по умолчанию

cloud = tinytuya.Cloud(apiRegion="eu", apiKey=API_KEY, apiSecret=API_SECRET)

# ---------- Настройки управления ----------
current_device_id = DEFAULT_DEVICE_ID
humidity_low = 45      # влажность ниже этого → включить розетку
humidity_high = 60     # влажность выше этого → выключить розетку
current_state = False  # текущее состояние розетки (будет обновляться при командах)

sensor_data = {
    'temperature': None,
    'humidity': None,
    'tvoc': None,
    'co2': None,
    'last_update': None
}

# ---------- База данных (SQLite) ----------
def init_db():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS readings
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                  temperature REAL,
                  humidity REAL,
                  tvoc INTEGER,
                  co2 INTEGER,
                  pm25 INTEGER,
                  overallQuality REAL)''')
    conn.commit()
    conn.close()

def save_reading(temperature, humidity, tvoc, co2, pm25, overallQuality):
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''INSERT INTO readings 
                 (temperature, humidity, tvoc, co2, pm25, overallQuality)
                 VALUES (?, ?, ?, ?, ?, ?)''',
              (temperature, humidity, tvoc, co2, pm25, overallQuality))
    conn.commit()
    conn.close()

def get_latest_reading():
    conn = sqlite3.connect('data.db')
    c = conn.cursor()
    c.execute('''SELECT temperature, humidity, tvoc, co2, pm25, overallQuality 
                 FROM readings ORDER BY timestamp DESC LIMIT 1''')
    row = c.fetchone()
    conn.close()
    if row:
        return {
            'temperature': row[0],
            'humidity': row[1],
            'tvoc': row[2],
            'co2': row[3],
            'pm25': row[4],
            'overallQuality': row[5]
        }
    else:
        return None

init_db()

# ---------- Фоновый поток управления розеткой ----------
def manage_switch():
    global current_state, sensor_data
    while True:
        now = datetime.datetime.now()
        # Если есть свежие данные (не старше 10 секунд)
        if (sensor_data['humidity'] is not None and
            sensor_data['last_update'] is not None and
            (now - sensor_data['last_update']).total_seconds() <= 10):
            hum = sensor_data['humidity']

            if hum > humidity_high and current_state:
                print(f"Влажность {hum}% > {humidity_high}% → выключаем")
                result = cloud.sendcommand(current_device_id, {
                    "commands": [{"code": "switch_1", "value": False}]
                })
                if result.get('success'):
                    current_state = False
                    print("Розетка выключена")
            elif hum < humidity_low and not current_state:
                print(f"Влажность {hum}% < {humidity_low}% → включаем")
                result = cloud.sendcommand(current_device_id, {
                    "commands": [{"code": "switch_1", "value": True}]
                })
                if result.get('success'):
                    current_state = True
                    print("Розетка включена")
        time.sleep(2)

threading.Thread(target=manage_switch, daemon=True).start()

# ---------- Эндпоинты ----------
@app.route('/sensor_data', methods=['POST'])
def receive_sensor_data():
    """Принимает данные от ESP32 (температура, влажность, TVOC, CO2)"""
    global sensor_data
    data = request.get_json()
    if not data:
        return jsonify({'error': 'No JSON'}), 400

    temp = data.get('temperature')
    hum = data.get('humidity')
    tvoc = data.get('tvoc')
    co2 = data.get('co2')
    pm25 = data.get('pm25', 0)
    overall = data.get('overallQuality')

    # Вычисляем overallQuality, если не передано
    if overall is None and co2 is not None and tvoc is not None:
        overall = max(0, min(100, 100 - (co2/40 + tvoc/10) / 2))

    # Сохраняем в БД
    if temp is not None and hum is not None and tvoc is not None and co2 is not None:
        save_reading(temp, hum, tvoc, co2, pm25, overall)

    # Обновляем данные для управления
    sensor_data = {
        'temperature': temp,
        'humidity': hum,
        'tvoc': tvoc,
        'co2': co2,
        'last_update': datetime.datetime.now()
    }
    print(f"Получены данные: t={temp}, h={hum}, tvoc={tvoc}, co2={co2}")
    return jsonify({'status': 'ok'}), 200

@app.route('/latest', methods=['GET'])
def latest():
    """Возвращает последние показания для мобильного приложения"""
    reading = get_latest_reading()
    if reading:
        return jsonify(reading), 200
    else:
        return jsonify({'error': 'No data yet'}), 404

@app.route('/devices', methods=['GET'])
def get_devices():
    """Возвращает список всех устройств из облака Tuya"""
    try:
        devices = cloud.getdevices()
        # Можно отфильтровать только розетки (по типу)
        return jsonify(devices), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/set_device', methods=['POST'])
def set_device():
    """Выбирает активное устройство по его ID"""
    global current_device_id
    data = request.get_json()
    new_id = data.get('device_id')
    if new_id:
        current_device_id = new_id
        return jsonify({'status': 'ok', 'device_id': current_device_id}), 200
    return jsonify({'error': 'No device_id'}), 400

@app.route('/set_thresholds', methods=['POST'])
def set_thresholds():
    """Устанавливает пороги влажности для автоматики"""
    global humidity_low, humidity_high
    data = request.get_json()
    low = data.get('humidity_low')
    high = data.get('humidity_high')
    if low is not None:
        humidity_low = low
    if high is not None:
        humidity_high = high
    return jsonify({'status': 'ok', 'humidity_low': humidity_low, 'humidity_high': humidity_high}), 200

@app.route('/switch', methods=['POST'])
def switch():
    """Ручное управление розеткой (вкл/выкл)"""
    global current_state
    data = request.get_json()
    state = data.get('state', False)
    result = cloud.sendcommand(current_device_id, {
        "commands": [{"code": "switch_1", "value": state}]
    })
    if result.get('success'):
        current_state = state
        return jsonify({'status': 'ok'}), 200
    else:
        return jsonify({'error': 'Command failed', 'details': result}), 500

@app.route('/status', methods=['GET'])
def get_status():
    """Возвращает текущие настройки и последние данные"""
    return jsonify({
        'device_id': current_device_id,
        'humidity_low': humidity_low,
        'humidity_high': humidity_high,
        'current_state': current_state,
        'sensor_data': {
            'temperature': sensor_data['temperature'],
            'humidity': sensor_data['humidity'],
            'tvoc': sensor_data['tvoc'],
            'co2': sensor_data['co2'],
            'last_update': sensor_data['last_update'].isoformat() if sensor_data['last_update'] else None
        }
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)