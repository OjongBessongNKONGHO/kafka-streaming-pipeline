from flask import Flask, jsonify
from flask_cors import CORS
import os
import psycopg2
import requests
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)
CORS(app)


def get_connection():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres_streaming"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB", "weather_streaming"),
        user=os.getenv("POSTGRES_USER", "streaming_user"),
        password=os.getenv("POSTGRES_PASSWORD", "streaming_pass")
    )


@app.route("/api/weather/latest", methods=["GET"])
def get_latest():
    """Get the latest weather record for each city."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ON (city)
                city, country, temperature, feels_like,
                humidity, pressure, weather_description,
                wind_speed, visibility, recorded_at, kafka_offset
            FROM weather_events
            ORDER BY city, recorded_at DESC
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        cities = []
        for row in rows:
            cities.append({
                "city": row[0],
                "country": row[1],
                "temperature": row[2],
                "feels_like": row[3],
                "humidity": row[4],
                "pressure": row[5],
                "description": row[6],
                "wind_speed": row[7],
                "visibility": row[8],
                "recorded_at": str(row[9]),
                "kafka_offset": row[10]
            })
        return jsonify({"status": "ok", "data": cities})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/weather/stats", methods=["GET"])
def get_stats():
    """Get pipeline statistics."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM weather_events")
        total = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM weather_events_dlq")
        dlq = cur.fetchone()[0]
        cur.execute("SELECT MAX(kafka_offset) FROM weather_events")
        max_offset = cur.fetchone()[0]
        cur.close()
        conn.close()
        return jsonify({
            "status": "ok",
            "total_records": total,
            "dlq_count": dlq,
            "max_kafka_offset": max_offset
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/weather/history/<city>", methods=["GET"])
def get_history(city):
    """Get last 20 records for a specific city."""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT temperature, humidity, wind_speed, recorded_at
            FROM weather_events
            WHERE LOWER(city) = LOWER(%s)
            ORDER BY recorded_at DESC
            LIMIT 20
        """, (city,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify({
            "status": "ok",
            "city": city,
            "data": [
                {
                    "temperature": r[0],
                    "humidity": r[1],
                    "wind_speed": r[2],
                    "recorded_at": str(r[3])
                } for r in rows
            ]
        })
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/api/weather/search/<city>", methods=["GET"])
def search_city(city):
    """
    Fetch live weather for any city directly from OpenWeatherMap.
    This data is not stored in the pipeline — it is a live lookup only.
    """
    try:
        api_key = os.getenv("OPENWEATHER_API_KEY")
        response = requests.get(
            "http://api.openweathermap.org/data/2.5/weather",
            params={"q": city, "appid": api_key, "units": "metric"},
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        return jsonify({
            "status": "ok",
            "live": True,
            "data": {
                "city": data["name"],
                "country": data["sys"]["country"],
                "temperature": data["main"]["temp"],
                "feels_like": data["main"]["feels_like"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "description": data["weather"][0]["description"],
                "wind_speed": data["wind"]["speed"],
                "visibility": data.get("visibility", 0),
                "recorded_at": datetime.utcnow().isoformat(),
                "kafka_offset": None
            }
        })
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            return jsonify({"status": "error", "message": f"City '{city}' not found"}), 404
        return jsonify({"status": "error", "message": str(e)}), 500
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)