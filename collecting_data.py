import requests
import sqlite3
from datetime import datetime
import time
import schedule

# Настройки SQLite
DB_NAME = "market_pressure.db"

# Функция для подключения к SQLite
def connect_to_db():
    try:
        conn = sqlite3.connect(DB_NAME)
        return conn
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

# Функция для создания таблицы (если она не существует)
def create_table_if_not_exists(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_pressure (
                time TEXT NOT NULL,
                symbol TEXT NOT NULL,
                bid_volume REAL,
                ask_volume REAL,
                imbalance REAL,
                PRIMARY KEY (time, symbol)
            )
        """)
        conn.commit()
        cursor.close()
        print("Таблица market_pressure создана или уже существует.")
    except Exception as e:
        print(f"Ошибка при создании таблицы: {e}")

# Функция для записи данных в SQLite
def save_to_db(conn, symbol, bid_volume, ask_volume, imbalance):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO market_pressure (time, symbol, bid_volume, ask_volume, imbalance)
            VALUES (?, ?, ?, ?, ?)
        """, (datetime.utcnow().isoformat(), symbol, bid_volume, ask_volume, imbalance))
        conn.commit()
        cursor.close()
        print(f"Данные для {symbol} успешно сохранены.")
    except Exception as e:
        print(f"Ошибка при записи данных в базу: {e}")

# Функция для получения списка фьючерсных пар
def get_futures_symbols():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    response = requests.get(url)
    if response.status_code == 200:
        symbols = response.json()['symbols']
        usdt_pairs = [symbol['symbol'] for symbol in symbols if symbol['quoteAsset'] == 'USDT']
        return usdt_pairs
    else:
        print("Ошибка получения списка фьючерсных пар.")
        return None

# Функция для получения стакана ордеров
def get_order_book(symbol, limit=100):
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit={limit}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Ошибка получения стакана ордеров для {symbol}.")
        return None

# Функция для анализа дисбаланса
def analyze_order_book(order_book):
    bids = order_book['bids']
    asks = order_book['asks']

    total_bid_volume = sum([float(bid[1]) for bid in bids])
    total_ask_volume = sum([float(ask[1]) for ask in asks])

    if total_bid_volume + total_ask_volume == 0:
        return total_bid_volume, total_ask_volume, 0

    imbalance = (total_bid_volume - total_ask_volume) / (total_bid_volume + total_ask_volume) * 100
    return total_bid_volume, total_ask_volume, imbalance

# Функция для анализа и сохранения данных
def analyze_and_save_data():
    conn = connect_to_db()
    if not conn:
        return

    create_table_if_not_exists(conn)

    symbols = get_futures_symbols()
    if not symbols:
        conn.close()
        return

    for symbol in symbols:
        order_book = get_order_book(symbol)
        if order_book:
            bid_volume, ask_volume, imbalance = analyze_order_book(order_book)
            save_to_db(conn, symbol, bid_volume, ask_volume, imbalance)

    conn.close()

# Запуск задачи каждый час
schedule.every().hour.at(":00").do(analyze_and_save_data)

# Основной цикл
if __name__ == "__main__":
    print("Скрипт запущен. Ожидание следующего часа...")
    while True:
        schedule.run_pending()
        time.sleep(1)
