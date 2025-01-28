import requests
import sqlite3
from datetime import datetime, timezone
import time
import schedule
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.utils.executor import start_polling
from configs import BOT_TOKEN

# Настройки SQLite
DB_NAME = "market_pressure.db"

# Токен вашего бота (замените на ваш токен)
TELEGRAM_BOT_TOKEN = BOT_TOKEN
# ID чата с пользователем (можно получить через /start в боте)
TELEGRAM_CHAT_ID = "ВАШ_CHAT_ID"

# Инициализация бота и диспетчера
bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(bot)

# Асинхронная функция для подключения к SQLite
async def connect_to_db():
    try:
        conn = sqlite3.connect(DB_NAME)
        return conn
    except Exception as e:
        print(f"Ошибка подключения к базе данных: {e}")
        return None

# Асинхронная функция для получения данных из базы данных
async def get_data_from_db(conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT time, symbol, bid_volume, ask_volume, imbalance
            FROM market_pressure
            ORDER BY time DESC
            LIMIT 1
        """)
        data = cursor.fetchall()
        cursor.close()
        return data
    except Exception as e:
        print(f"Ошибка при получении данных из базы: {e}")
        return None

# Асинхронная функция для отправки сообщения в Telegram
async def send_message(chat_id, message):
    await bot.send_message(chat_id=chat_id, text=message)

# Асинхронная функция для анализа и отправки данных
async def analyze_and_send_data():
    conn = await connect_to_db()
    if not conn:
        return

    data = await get_data_from_db(conn)
    if not data:
        conn.close()
        return

    message = "Общая информация по рынку:\n"
    for row in data:
        time, symbol, bid_volume, ask_volume, imbalance = row
        message += (
            f"Время: {time}\n"
            f"Символ: {symbol}\n"
            f"Объем покупок (bids): {bid_volume:.2f}\n"
            f"Объем продаж (asks): {ask_volume:.2f}\n"
            f"Дисбаланс: {imbalance:.2f}%\n"
        )

    await send_message(TELEGRAM_CHAT_ID, message)
    conn.close()

# Запуск задачи каждый час через 15 минут после ровного часа
schedule.every().hour.at(":15").do(lambda: asyncio.run(analyze_and_send_data()))

# Основной цикл
if __name__ == "__main__":
    print("Скрипт запущен. Ожидание следующего часа...")
    loop = asyncio.get_event_loop()
    loop.create_task(start_polling(dp, skip_updates=True))
    while True:
        schedule.run_pending()
        time.sleep(1)


