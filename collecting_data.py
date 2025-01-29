import requests
import sqlite3
from datetime import datetime
import time
import schedule
import pytz
import asyncio
from aiogram import Bot
from configs import BOT_TOKEN, DATABASE_NAME

# Настройки SQLite
DB_NAME = DATABASE_NAME
BOT_TOKEN = BOT_TOKEN  # Замените на ваш токен


# Функция для подключения к SQLite
def connect_to_db():
    try:
        conn = sqlite3.connect(DB_NAME)
        return conn
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

# Функция для создания таблиц (если они не существуют)
def create_tables_if_not_exist(conn):
    try:
        cursor = conn.cursor()
        # Таблица для данных по каждой паре
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_pressure (
                time TIMESTAMP NOT NULL,
                symbol TEXT NOT NULL,
                bid_volume REAL,
                ask_volume REAL,
                dizbalance REAL,
                PRIMARY KEY (time, symbol)
            );
        """)
        # Таблица для агрегированных данных по всему рынку
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS market_summary (
                time TIMESTAMP NOT NULL PRIMARY KEY,
                total_bid_volume REAL,
                total_ask_volume REAL,
                total_dizbalance REAL
            );
        """)
        conn.commit()
        cursor.close()
        print("Таблицы созданы или уже существуют.")
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")

# Функция для записи данных по каждой паре
def save_pair_data(conn, symbol, bid_volume, ask_volume, dizbalance):
    try:
        cursor = conn.cursor()
        msk_timezone = pytz.timezone("Europe/Moscow")
        current_time = datetime.now(msk_timezone).isoformat()
        cursor.execute("""
            INSERT INTO market_pressure (time, symbol, bid_volume, ask_volume, dizbalance)
            VALUES (?, ?, ?, ?, ?)
        """, (current_time, symbol, bid_volume, ask_volume, dizbalance))
        conn.commit()
        cursor.close()
        print(f"Данные для {symbol} успешно сохранены.")
    except Exception as e:
        print(f"Ошибка при записи данных для {symbol}: {e}")

# Функция для записи агрегированных данных
def save_market_summary(conn, total_bid_volume, total_ask_volume, total_dizbalance):
    try:
        cursor = conn.cursor()
        msk_timezone = pytz.timezone("Europe/Moscow")
        current_time = datetime.now(msk_timezone).isoformat()
        cursor.execute("""
            INSERT INTO market_summary (time, total_bid_volume, total_ask_volume, total_dizbalance)
            VALUES (?, ?, ?, ?)
        """, (current_time, total_bid_volume, total_ask_volume, total_dizbalance))
        conn.commit()
        cursor.close()
        print("Агрегированные данные по рынку успешно сохранены.")
    except Exception as e:
        print(f"Ошибка при записи агрегированных данных: {e}")

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
    dizbalance = (total_bid_volume - total_ask_volume) / (total_bid_volume + total_ask_volume) * 100
    return total_bid_volume, total_ask_volume, dizbalance

# Функция для отправки уведомлений
async def send_notification(bot_token, chat_id, message):
    bot = Bot(token=bot_token)
    try:
        await bot.send_message(chat_id=chat_id, text=message)
        print(f"Уведомление отправлено: {message}")
    except Exception as e:
        print(f"Ошибка при отправке уведомления: {e}")
    finally:
        await bot.session.close()

# Функция для анализа и сохранения данных
def analyze_and_save_data():
    conn = connect_to_db()
    if not conn:
        return
    create_tables_if_not_exist(conn)
    symbols = get_futures_symbols()
    if not symbols:
        conn.close()
        return
    total_bid_volume_all = 0
    total_ask_volume_all = 0
    for symbol in symbols:
        order_book = get_order_book(symbol)
        if order_book:
            bid_volume, ask_volume, dizbalance = analyze_order_book(order_book)
            save_pair_data(conn, symbol, bid_volume, ask_volume, dizbalance)
            total_bid_volume_all += bid_volume
            total_ask_volume_all += ask_volume
    # Расчет общего дисбаланса
    if total_bid_volume_all + total_ask_volume_all == 0:
        total_dizbalance = 0
    else:
        total_dizbalance = (total_bid_volume_all - total_ask_volume_all) / (total_bid_volume_all + total_ask_volume_all) * 100
    # Сохранение агрегированных данных
    save_market_summary(conn, total_bid_volume_all, total_ask_volume_all, total_dizbalance)

    # Вывод последних агрегированных данных в консоль
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT time, total_bid_volume, total_ask_volume, total_dizbalance
            FROM market_summary
            ORDER BY time DESC
            LIMIT 1
        """)
        latest_summary = cursor.fetchone()
        if latest_summary:
            time_str, total_bid_volume, total_ask_volume, total_dizbalance = latest_summary
            print("Последние агрегированные данные:")
            print(f"  Время: {time_str}")
            print(f"  Общий объем покупок: {total_bid_volume:.2f}")
            print(f"  Общий объем продаж: {total_ask_volume:.2f}")
            print(f"  Общий дисбаланс: {total_dizbalance:.2f}%")

            # Формируем сообщение для отправки
            notification_message = (
                f"Последние агрегированные данные:\n"
                f"  Время: {time_str}\n"
                f"  Общий объем покупок: {total_bid_volume:.2f}\n"
                f"  Общий объем продаж: {total_ask_volume:.2f}\n"
                f"  Общий дисбаланс: {total_dizbalance:.2f}%"
            )

            # Отправляем уведомление в Telegram
            asyncio.run(send_notification(BOT_TOKEN, CHAT_ID, notification_message))
        else:
            print("Агрегированные данные отсутствуют.")
        cursor.close()
    except Exception as e:
        print(f"Ошибка при получении последних агрегированных данных: {e}")

    conn.close()

# Запуск задачи каждые 5 минут
schedule.every(5).minutes.do(analyze_and_save_data)

# Основной цикл
if __name__ == "__main__":
    print("Скрипт запущен. Ожидание следующего интервала...")
    while True:
        schedule.run_pending()
        time.sleep(1)