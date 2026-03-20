from flask import Flask, request, jsonify
from flask_cors import CORS
import sqlite3
import datetime
import threading
import time

app = Flask(__name__)
CORS(app)  # разрешаем кросс-доменные запросы от Android-приложения

# ---------- Инициализация базы данных ----------
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

# Создаём таблицу при старте приложения (вызывается при импорте модуля)
init_db()

# ---------- Работа с базой данных ----------
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

# ---------- Эндпоинты ----------
@app.route('/data', methods=['POST'])
def receive_data():
    """Принимает данные от ESP32 в формате JSON"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No JSON data'}), 400

        temperature = data.get('temperature')
        humidity = data.get('humidity')
        tvoc = data.get('tvoc')
        co2 = data.get('co2')
        pm25 = data.get('pm25')
        overallQuality = data.get('overallQuality')

        if pm25 is None:
            pm25 = 0

        if overallQuality is None:
            # Простейшая формула: 100 - (co2/40 + tvoc/10) / 2
            overallQuality = max(0, min(100, 100 - (co2/40 + tvoc/10) / 2))

        save_reading(temperature, humidity, tvoc, co2, pm25, overallQuality)
        return jsonify({'status': 'ok'}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/latest', methods=['GET'])
def latest():
    reading = get_latest_reading()
    if reading:
        return jsonify(reading), 200
    else:
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
    result = []
    for row in rows:
        result.append({
            'timestamp': row[0],
            'temperature': row[1],
            'humidity': row[2],
            'tvoc': row[3],
            'co2': row[4],
            'pm25': row[5],
            'overallQuality': row[6]
        })
    return jsonify(result), 200

if __name__ == '__main__':
    # Для локального тестирования
    app.run(host='0.0.0.0', port=8080, debug=True)