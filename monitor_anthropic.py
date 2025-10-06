"""
Скрипт для моніторингу доступності API Anthropic.
Запускається окремо від основного бота.

Використання:
    python monitor_anthropic.py           # Запустити моніторинг
    python monitor_anthropic.py errors    # Показати помилки з бази
"""

import anthropic
import time
from datetime import datetime
import os
import sqlite3
from pathlib import Path
from dotenv import load_dotenv

# Завантажуємо змінні з .env файлу
load_dotenv()

# Налаштування
API_KEY = os.getenv("ANTHROPIC_API_KEY", "your-api-key-here")
MODELS = [
    "claude-sonnet-4-20250514",      # Claude 4.0 Sonnet
    "claude-opus-4-20250514",        # Claude 4.1 Opus
    "claude-sonnet-4-5-20250929"     # Claude 4.5 Sonnet
]
INTERVAL = 600  # 10 хвилин
DB_FILE = Path(__file__).parent / "monitoring.db"


def init_db():
    """Створення бази даних для логування"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            model TEXT NOT NULL,
            status_code INTEGER,
            response_time REAL,
            request_body TEXT,
            response_body TEXT,
            error_message TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print(f"✓ База даних {DB_FILE} готова\n")


def save_to_db(timestamp, model, status_code, response_time, request_body, response_body, error_message):
    """Запис логів в базу даних"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        INSERT INTO logs (timestamp, model, status_code, response_time, request_body, response_body, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (timestamp, model, status_code, response_time, request_body, response_body, error_message))
    conn.commit()
    conn.close()


def send_message(model):
    """Відправка тестового повідомлення в модель"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
        
        print(f"✓ [{timestamp}] {model}: 200 OK ({elapsed:.2f}s)")
        
    except anthropic.RateLimitError as e:
        elapsed = time.time() - start
        status_code = 429
        error_message = f"Rate Limit: {str(e)}"
        print(f"✗ [{timestamp}] {model}: {status_code} - {error_message} ({elapsed:.2f}s)")
        
    except anthropic.APIError as e:
        elapsed = time.time() - start
        status_code = getattr(e, 'status_code', 500)
        error_message = str(e)
        
        if "overload" in error_message.lower():
            print(f"⚠️  [{timestamp}] {model}: {status_code} - OVERLOAD ({elapsed:.2f}s)")
        else:
            print(f"✗ [{timestamp}] {model}: {status_code} - {error_message} ({elapsed:.2f}s)")
    
    except Exception as e:
        elapsed = time.time() - start
        status_code = 0
        error_message = f"Невідома помилка: {str(e)}"
        print(f"✗ [{timestamp}] {model}: ERROR - {error_message} ({elapsed:.2f}s)")
    
    # Записуємо в базу тільки якщо статус != 200
    if status_code != 200:
        save_to_db(
            timestamp=timestamp,
            model=model,
            status_code=status_code,
            response_time=round(elapsed, 3),
            request_body=request_body,
            response_body=response_body,
            error_message=error_message
        )
        print(f"  💾 Збережено в БД")


def show_errors():
    """Показати всі помилки з бази даних"""
    if not DB_FILE.exists():
        print("❌ База даних ще не створена. Спочатку запустіть моніторинг.")
        return
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('SELECT COUNT(*) FROM logs')
    total = c.fetchone()[0]
    
    c.execute('SELECT * FROM logs ORDER BY timestamp DESC LIMIT 50')
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        print("📊 Помилок поки немає")
        return
    
    print(f"\n📊 ОСТАННІ ПОМИЛКИ (всього в БД: {total}):")
    print("=" * 100)
    
    for row in rows:
        print(f"\n🕒 {row[1]}")
        print(f"   Модель: {row[2]}")
        print(f"   Статус: {row[3]}")
        print(f"   Час відклику: {row[4]}s")
        print(f"   Запит: {row[5]}")
        if row[6]:
            print(f"   Відповідь: {row[6]}")
        print(f"   Помилка: {row[7]}")


def show_stats():
    """Показати статистику помилок"""
    if not DB_FILE.exists():
        print("❌ База даних ще не створена.")
        return
    
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    print("\n📈 СТАТИСТИКА ПОМИЛОК:")
    print("=" * 100)
    
    for model in MODELS:
        c.execute('SELECT COUNT(*) FROM logs WHERE model = ?', (model,))
        total = c.fetchone()[0]
        
        c.execute('SELECT COUNT(*) FROM logs WHERE model = ? AND error_message LIKE ?', 
                  (model, '%overload%'))
        overloads = c.fetchone()[0]
        
        c.execute('SELECT COUNT(*) FROM logs WHERE model = ? AND status_code = 429', (model,))
        rate_limits = c.fetchone()[0]
        
        print(f"\n{model}:")
        print(f"  Всього помилок: {total}")
        print(f"  Перевантажень (overload): {overloads}")
        print(f"  Rate Limits (429): {rate_limits}")
        
        if total > 0:
            c.execute('''
                SELECT timestamp, status_code, error_message 
                FROM logs 
                WHERE model = ? 
                ORDER BY timestamp DESC 
                LIMIT 3
            ''', (model,))
            recent = c.fetchall()
            print(f"  Останні помилки:")
            for r in recent:
                print(f"    - {r[0]}: {r[1]} - {r[2][:60]}...")
    
    conn.close()


def main():
    """Основний цикл моніторингу"""
    init_db()
    
    print("🚀 Запуск моніторингу API Anthropic")
    print(f"📊 Моделі для перевірки: {len(MODELS)} штук")
    print(f"⏰ Інтервал: кожні {INTERVAL // 60} хвилин")
    print(f"💾 Логи зберігаються в: {DB_FILE}")
    print(f"🔑 API Key: {'встановлено' if API_KEY != 'your-api-key-here' else 'НЕ ВСТАНОВЛЕНО!'}")
    print("-" * 100)
    
    if API_KEY == "your-api-key-here":
        print("\n⚠️  УВАГА! Встановіть ANTHROPIC_API_KEY у змінні середовища!")
        print("Приклад: export ANTHROPIC_API_KEY='your-key-here'\n")
        return
    
    try:
        iteration = 0
        while True:
            iteration += 1
            print(f"\n🔄 Ітерація #{iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 100)
            
            for model in MODELS:
                send_message(model)
                time.sleep(2)  # Невелика пауза між запитами
            
            print(f"\n⏳ Сплю {INTERVAL // 60} хвилин до наступної перевірки...")
            time.sleep(INTERVAL)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Моніторинг зупинено користувачем")
        print(f"📊 Перегляньте статистику: python {__file__} stats")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "errors":
            show_errors()
        elif command == "stats":
            show_stats()
        else:
            print("❌ Невідома команда. Доступні команди:")
            print("  python monitor_anthropic.py         # Запустити моніторинг")
            print("  python monitor_anthropic.py errors  # Показати помилки")
            print("  python monitor_anthropic.py stats   # Показати статистику")
    else:
        main()
