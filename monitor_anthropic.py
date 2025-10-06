"""
Скрипт для моніторингу доступності API Anthropic.
"""

import anthropic
import time
from datetime import datetime
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import logging
from pathlib import Path

load_dotenv()

API_KEY = os.getenv("CLAUDE_API_KEY_1", "your-api-key-here")
DATABASE_URL = os.getenv("DATABASE_URL")
MODELS = [
    "claude-sonnet-4-20250514",
    "claude-opus-4-20250514",
    "claude-sonnet-4-5-20250929"
]
INTERVAL = 600
LOG_FILE = Path(__file__).parent / "monitor_anthropic.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS anthropic_monitoring (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP NOT NULL,
            model VARCHAR(100) NOT NULL,
            status_code INTEGER,
            response_time REAL,
            request_body TEXT,
            response_body TEXT,
            error_message TEXT
        )
    ''')
    conn.commit()
    conn.close()
    logger.info("✓ Таблиця anthropic_monitoring готова")


def save_to_db(timestamp, model, status_code, response_time, request_body, response_body, error_message):
    try:
        conn = get_db_connection()
        c = conn.cursor()
        c.execute('''
            INSERT INTO anthropic_monitoring 
            (timestamp, model, status_code, response_time, request_body, response_body, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        ''', (timestamp, model, status_code, response_time, request_body, response_body, error_message))
        conn.commit()
        conn.close()
        logger.info(f"💾 Збережено в БД: {model} - {status_code}")
    except Exception as e:
        logger.error(f"❌ Помилка запису в БД: {e}")


def send_message(model):
    timestamp = datetime.now()
    request_body = "привіт"
    response_body = None
    error_message = None
    status_code = None

    try:
        client = anthropic.Anthropic(api_key=API_KEY)
        start = time.time()

        response = client.messages.create(
            model=model,
            max_tokens=10,
            messages=[{"role": "user", "content": request_body}]
        )

        elapsed = time.time() - start
        status_code = 200
        response_body = response.content[0].text if response.content else ""
        logger.info(f"✓ {model}: 200 OK ({elapsed:.2f}s)")

    except anthropic.RateLimitError as e:
        elapsed = time.time() - start
        status_code = 429
        error_message = f"Rate Limit: {str(e)}"
        logger.warning(f"✗ {model}: {status_code} - {error_message} ({elapsed:.2f}s)")

    except anthropic.APIError as e:
        elapsed = time.time() - start
        status_code = getattr(e, 'status_code', 500)
        error_message = str(e)

        if "overload" in error_message.lower():
            logger.warning(f"⚠️  {model}: {status_code} - OVERLOAD ({elapsed:.2f}s)")
        else:
            logger.error(f"✗ {model}: {status_code} - {error_message} ({elapsed:.2f}s)")

    except Exception as e:
        elapsed = time.time() - start
        status_code = 0
        error_message = f"Невідома помилка: {str(e)}"
        logger.error(f"✗ {model}: ERROR - {error_message} ({elapsed:.2f}s)")

    if status_code != 200:
        save_to_db(timestamp, model, status_code, round(elapsed, 3), request_body, response_body, error_message)


def show_errors():
    try:
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)

        c.execute('SELECT COUNT(*) as count FROM anthropic_monitoring')
        total = c.fetchone()['count']

        c.execute('SELECT * FROM anthropic_monitoring ORDER BY timestamp DESC LIMIT 50')
        rows = c.fetchall()
        conn.close()

        if not rows:
            print("📊 Помилок поки немає")
            return

        print(f"\n📊 ОСТАННІ ПОМИЛКИ (всього: {total}):")
        print("=" * 100)

        for row in rows:
            print(f"\n🕒 {row['timestamp']}")
            print(f"   Модель: {row['model']}")
            print(f"   Статус: {row['status_code']} ({row['response_time']}s)")
            print(f"   Помилка: {row['error_message']}")

    except Exception as e:
        logger.error(f"❌ Помилка: {e}")


def show_stats():
    try:
        conn = get_db_connection()
        c = conn.cursor(cursor_factory=RealDictCursor)

        print("\n📈 СТАТИСТИКА:")
        print("=" * 100)

        for model in MODELS:
            c.execute('SELECT COUNT(*) as count FROM anthropic_monitoring WHERE model = %s', (model,))
            total = c.fetchone()['count']

            c.execute('SELECT COUNT(*) as count FROM anthropic_monitoring WHERE model = %s AND error_message LIKE %s',
                      (model, '%overload%'))
            overloads = c.fetchone()['count']

            c.execute('SELECT COUNT(*) as count FROM anthropic_monitoring WHERE model = %s AND status_code = 429', (model,))
            rate_limits = c.fetchone()['count']

            print(f"\n{model}:")
            print(f"  Помилок: {total} | Overload: {overloads} | Rate Limits: {rate_limits}")

        conn.close()
    except Exception as e:
        logger.error(f"❌ Помилка: {e}")


def main():
    init_db()

    logger.info("🚀 Запуск моніторингу")
    logger.info(f"📊 Моделі: {len(MODELS)}")
    logger.info(f"⏰ Інтервал: {INTERVAL // 60} хв")
    logger.info(f"💾 Логи: {LOG_FILE}")

    if API_KEY == "your-api-key-here" or not DATABASE_URL:
        logger.error("⚠️  Налаштуйте .env файл!")
        return

    try:
        iteration = 0
        while True:
            iteration += 1
            logger.info(f"\n🔄 Ітерація #{iteration}")

            for model in MODELS:
                send_message(model)
                time.sleep(2)

            logger.info(f"⏳ Сплю {INTERVAL // 60} хв...")
            time.sleep(INTERVAL)

    except KeyboardInterrupt:
        logger.info("\n🛑 Зупинено")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        cmd = sys.argv[1].lower()
        if cmd == "errors":
            show_errors()
        elif cmd == "stats":
            show_stats()
    else:
        main()