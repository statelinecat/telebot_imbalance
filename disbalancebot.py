from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    FSInputFile,
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from configs import BOT_TOKEN, DATABASE_NAME, PIN_CODE_HASH
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from datetime import datetime
import bcrypt
import logging
import asyncio
from logging.handlers import RotatingFileHandler
import sqlite3

# Настройка логирования
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(
    "bot.log",
    maxBytes=5 * 1024 * 1024,  # 5 MB
    backupCount=3,
    encoding="utf-8",
)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# FSM
class PinCodeState(StatesGroup):
    entering_pin = State()  # Ввод пин-кода
    incorrect_pin = State()  # Неверный пин-код
    blocked = State()  # Блокировка

# Функция для создания клавиатуры с пин-кодом и кнопкой "готово"
def get_pin_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="1", callback_data="pin_1"),
                InlineKeyboardButton(text="2", callback_data="pin_2"),
                InlineKeyboardButton(text="3", callback_data="pin_3"),
            ],
            [
                InlineKeyboardButton(text="4", callback_data="pin_4"),
                InlineKeyboardButton(text="5", callback_data="pin_5"),
                InlineKeyboardButton(text="6", callback_data="pin_6"),
            ],
            [
                InlineKeyboardButton(text="7", callback_data="pin_4"),
                InlineKeyboardButton(text="8", callback_data="pin_5"),
                InlineKeyboardButton(text="9", callback_data="pin_6"),
            ],
            [
                InlineKeyboardButton(text="0", callback_data="pin_0"),
                InlineKeyboardButton(text="Готово", callback_data="pin_done"),
            ],
        ]
    )

# Функция для создания главной клавиатуры
def get_main_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Весь рынок", callback_data="market_summary"),
                InlineKeyboardButton(text="Выбрать монету", callback_data="select_coin"),
            ],
        ]
    )

# Функция для подключения к базе данных
def connect_to_db():
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        return None

# Функция для создания таблиц, если их нет
def create_tables_if_not_exist(conn):
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS market_summary (
                time TEXT,
                total_bid_volume REAL,
                total_ask_volume REAL,
                total_dizbalance REAL
            );
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS market_pressure (
                symbol TEXT,
                time TEXT,
                bid_volume REAL,
                ask_volume REAL,
                dizbalance REAL
            );
            """
        )
        conn.commit()
        cursor.close()
        logger.info("Таблицы созданы или уже существуют.")
    except Exception as e:
        logger.error(f"Ошибка создания таблиц: {e}")

# Функция для проверки авторизации пользователя
def is_user_authorized(chat_id):
    conn = connect_to_db()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT chat_id FROM users WHERE chat_id = ?
            """,
            (chat_id,),
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result is not None
    except Exception as e:
        logger.error(f"Ошибка проверки авторизации: {e}")
        return False

# Функция для сохранения chat_id пользователя
def save_chat_id(chat_id):
    conn = connect_to_db()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO users (chat_id) VALUES (?)
            """,
            (chat_id,),
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка сохранения chat_id: {e}")
        return False

# Функция для удаления chat_id пользователя
def delete_chat_id(chat_id):
    conn = connect_to_db()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            DELETE FROM users WHERE chat_id = ?
            """,
            (chat_id,),
        )
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка удаления chat_id: {e}")
        return False

# Функция для получения последнего агрегированного отчета
def get_latest_market_summary():
    conn = connect_to_db()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT time, total_bid_volume, total_ask_volume, total_dizbalance
            FROM market_summary
            ORDER BY time DESC
            LIMIT 1
            """
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка получения агрегированного отчета: {e}")
        return None

# Функция для получения последних данных по монете
def get_latest_coin_data(symbol):
    conn = connect_to_db()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT time, bid_volume, ask_volume, dizbalance
            FROM market_pressure
            WHERE symbol = ?
            ORDER BY time DESC
            LIMIT 1
            """,
            (symbol,),
        )
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Ошибка получения данных по монете: {e}")
        return None

# Функция для создания Excel отчета
def create_excel_report(symbol=None):
    conn = connect_to_db()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        if symbol:
            cursor.execute(
                """
                SELECT time, bid_volume, ask_volume, dizbalance
                FROM market_pressure
                WHERE symbol = ?
                ORDER BY time DESC
                """,
                (symbol,),
            )
            data = cursor.fetchall()
            columns = ["Time", "Bid Volume", "Ask Volume", "Dizbalance"]
        else:
            cursor.execute(
                """
                SELECT time, total_bid_volume, total_ask_volume, total_dizbalance
                FROM market_summary
                ORDER BY time DESC
                """
            )
            data = cursor.fetchall()
            columns = ["Time", "Total Bid Volume", "Total Ask Volume", "Total Dizbalance"]
        cursor.close()
        conn.close()
        df = pd.DataFrame(data, columns=columns)
        df["Time"] = pd.to_datetime(df["Time"]).dt.strftime("%Y.%m.%d %H:%M")
        excel_file = f"{symbol}_market_summary_report.xlsx" if symbol else "market_summary_report.xlsx"
        df.to_excel(excel_file, index=False)
        return excel_file
    except Exception as e:
        logger.error(f"Ошибка создания Excel отчета: {e}")
        return None

# Функция для создания PNG отчета
def create_png_report(symbol=None):
    conn = connect_to_db()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        if symbol:
            cursor.execute(
                """
                SELECT time, dizbalance
                FROM market_pressure
                WHERE symbol = ?
                ORDER BY time DESC
                LIMIT 3000
                """,
                (symbol,),
            )
            data = cursor.fetchall()
            columns = ["Time", "Dizbalance"]
        else:
            cursor.execute(
                """
                SELECT time, total_dizbalance
                FROM market_summary
                ORDER BY time DESC
                LIMIT 3000
                """
            )
            data = cursor.fetchall()
            columns = ["Time", "Total Dizbalance"]
        cursor.close()
        conn.close()
        data = data[::-1]
        times = [
            datetime.strptime(row[0][:19], "%Y-%m-%dT%H:%M:%S") for row in data
        ]
        dizbalances = [row[1] for row in data]
        plt.figure(figsize=(10, 5))
        width = 0.8 / 48  # Уменьшаем ширину столбиков, чтобы они соответствовали каждой записи в базе данных
        plt.bar(times, dizbalances, color="#FF6B6B", width=width, edgecolor='black')  # Добавляем рамки вокруг столбиков
        plt.xlabel("")
        plt.ylabel("")
        plt.gca().xaxis.set_major_formatter(DateFormatter("%d.%m"))
        plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=24))  # Показывать только каждый день в 3 часа ночи
        plt.title("Dizbalance за последние 30 дней")
        plt.xticks(rotation=45)
        plt.tight_layout()
        png_file = f"{symbol}_market_summary_report.png" if symbol else "market_summary_report.png"
        plt.savefig(png_file)
        plt.close()
        return png_file
    except Exception as e:
        logger.error(f"Ошибка создания PNG отчета: {e}")
        return None

# Функция для создания PDF отчета
def create_pdf_report(symbol=None):
    conn = connect_to_db()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        if symbol:
            cursor.execute(
                """
                SELECT time, dizbalance
                FROM market_pressure
                WHERE symbol = ?
                ORDER by time DESC
                LIMIT 3000
                """,
                (symbol,),
            )
            data = cursor.fetchall()
            columns = ["Time", "Dizbalance"]
        else:
            cursor.execute(
                """
                SELECT time, total_dizbalance
                FROM market_summary
                ORDER by time DESC
                LIMIT 3000
                """
            )
            data = cursor.fetchall()
            columns = ["Time", "Total Dizbalance"]
        cursor.close()
        conn.close()
        data = data[::-1]
        times = [
            datetime.strptime(row[0][:19], "%Y-%m-%dT%H:%M:%S") for row in data
        ]
        dizbalances = [row[1] for row in data]
        plt.figure(figsize=(10, 5))
        width = 0.8 / 48  # Уменьшаем ширину столбиков, чтобы они соответствовали каждой записи в базе данных
        plt.bar(times, dizbalances, color="#4ECDC4", width=width, edgecolor='black')  # Добавляем рамки вокруг столбиков
        plt.xlabel("")
        plt.ylabel("")
        plt.gca().xaxis.set_major_formatter(DateFormatter("%d.%m"))
        plt.gca().xaxis.set_major_locator(mdates.HourLocator(interval=24))  # Показывать только каждый день в 3 часа ночи
        plt.title("Dizbalance за последние 30 дней")
        plt.xticks(rotation=45)
        plt.tight_layout()
        pdf_file = f"{symbol}_market_summary_report.pdf" if symbol else "market_summary_report.pdf"
        with PdfPages(pdf_file) as pdf_pages:
            pdf_pages.savefig()
        plt.close()
        return pdf_report
    except Exception as e:
        logger.error(f"Ошибка создания PDF отчета: {e}")
        return None

# Обработчик команды /start
@dp.message(Command("start"))
async def send_welcome(message: Message, state: FSMContext):
    chat_id = message.chat.id
    if is_user_authorized(chat_id):
        await message.answer(
            "✅ Вы авторизованы! Выберите действие:",
            reply_markup=get_main_keyboard(),
        )
    else:
        await message.answer(
            "⚠️ Вы не авторизованы. Введите пин-код:",
            reply_markup=get_pin_keyboard(),
        )
        await state.set_state(PinCodeState.entering_pin)
        await state.update_data(pin_buffer="", attempts=0)

# Обработчик ввода пин-кода
@dp.callback_query(PinCodeState.entering_pin)
async def handle_pin_digit(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    pin_buffer = data.get("pin_buffer", "")
    attempts = data.get("attempts", 0)
    if attempts >= 3:
        await callback.message.answer("Вы заблокированы на 5 минут.")
        await state.set_state(PinCodeState.blocked)
        await asyncio.sleep(300)  # 5 минут
        await state.clear()
        return
    if callback.data.startswith("pin_"):
        action = callback.data.split("_")[1]
        if action == "done":
            if len(pin_buffer) == 4 and bcrypt.checkpw(pin_buffer.encode(), PIN_CODE_HASH.encode()):
                chat_id = callback.message.chat.id
                if save_chat_id(chat_id):
                    await callback.message.answer(
                        "✅ Пин-код принят! Выберите действие:",
                        reply_markup=get_main_keyboard(),
                    )
                    await state.clear()
                else:
                    await callback.message.answer(
                        "❌ Ошибка авторизации. Повторите попытку.",
                        reply_markup=get_pin_keyboard(),
                    )
            else:
                attempts += 1
                await state.update_data(attempts=attempts)
                await callback.message.answer(
                    f"❌ Неверный пин-код. Осталось попыток: {3 - attempts}",
                    reply_markup=get_pin_keyboard(),
                )
                await state.update_data(pin_buffer="")  # Сброс пин-кода
        else:
            pin_buffer += action
            await state.update_data(pin_buffer=pin_buffer)
            try:
                await callback.message.edit_text(
                    f"Введите пин-код: {'*' * len(pin_buffer)}",
                    reply_markup=get_pin_keyboard(),
                )
            except Exception as e:
                logger.error(f"Ошибка редактирования текста: {e}")
    await callback.answer()

# Обработчик команды /delme для удаления пользователя
@dp.message(Command("delme"))
async def delete_user(message: Message):
    chat_id = message.chat.id
    if is_user_authorized(chat_id):
        if delete_chat_id(chat_id):
            await message.answer(
                "✅ Вы удалены из системы. Для повторной авторизации нажмите /start."
            )
        else:
            await message.answer("❌ Ошибка удаления. Обратитесь к администратору.")
    else:
        await message.answer("❌ Вы не авторизованы. Нажмите /start.")

# Обработчик нажатия на кнопку "Весь рынок"
@dp.callback_query(F.data == "market_summary")
async def show_market_summary(callback: CallbackQuery):
    if not is_user_authorized(callback.message.chat.id):
        await callback.message.answer(
            "❌ Вы не авторизованы. Нажмите /start."
        )
        return
    latest_summary = get_latest_market_summary()
    if latest_summary:
        time_str, total_bid_volume, total_ask_volume, total_dizbalance = latest_summary
        response = (
            f"✅ Последний агрегированный отчет:\n"
            f"Время: {datetime.strptime(time_str[:19], '%Y-%m-%dT%H:%M:%S').strftime('%Y.%m.%d %H:%M')}\n"
            f"Общий объем заявок на покупку: {total_bid_volume:.2f}\n"
            f"Общий объем заявок на продажу: {total_ask_volume:.2f}\n"
            f"Дизбаланс: {total_dizbalance:.2f}%"
        )
    else:
        response = "❌ Не удалось получить данные."
    await callback.message.answer(response, reply_markup=get_report_keyboard())

# Обработчик нажатия на кнопку "Выбрать монету"
@dp.callback_query(F.data == "select_coin")
async def request_coin_ticker(callback: CallbackQuery):
    if not is_user_authorized(callback.message.chat.id):
        await callback.message.answer(
            "❌ Вы не авторизованы. Нажмите /start."
        )
        return
    await callback.message.answer("Введите тикер монеты (например, BTCUSDT):")

# Обработчик ввода тикера монеты
@dp.message(lambda message: message.text.isalnum() and len(message.text) >= 6)
async def show_coin_data(message: Message):
    if not is_user_authorized(message.chat.id):
        await message.answer("❌ Вы не авторизованы. Нажмите /start.")
        return
    symbol = message.text.upper()
    if not symbol.endswith("USDT"):
        await message.answer("❌ Тикер должен заканчиваться на 'USDT'. Пожалуйста, введите корректный тикер (например, BTCUSDT):")
        return
    latest_data = get_latest_coin_data(symbol)
    if latest_data:
        time_str, bid_volume, ask_volume, dizbalance = latest_data
        response = (
            f"✅ Данные по монете {symbol}:\n"
            f"Время: {datetime.strptime(time_str[:19], '%Y-%m-%dT%H:%M:%S').strftime('%Y.%m.%d %H:%M')}\n"
            f"Объем заявок на покупку: {bid_volume:.2f}\n"
            f"Объем заявок на продажу: {ask_volume:.2f}\n"
            f"Дизбаланс: {dizbalance:.2f}%"
        )
    else:
        response = f"❌ Данные по монете {symbol} не найдены."
    await message.answer(response, reply_markup=get_report_keyboard(symbol))

# Обработчик нажатия на кнопки для генерации отчетов
@dp.callback_query()
async def generate_report(callback: CallbackQuery):
    if not is_user_authorized(callback.message.chat.id):
        await callback.message.answer("❌ Вы не авторизованы. Нажмите /start.")
        return
    if callback.data == "excel_report":
        excel_file = create_excel_report()
        if excel_file:
            await callback.message.answer_document(
                FSInputFile(excel_file),
                caption="✅ Excel отчет сформирован."
            )
        else:
            await callback.message.answer("❌ Не удалось создать Excel отчет.")
    elif callback.data == "pdf_report":
        pdf_file = create_pdf_report()
        if pdf_file:
            await callback.message.answer_document(
                FSInputFile(pdf_file),
                caption="✅ PDF отчет сформирован."
            )
        else:
            await callback.message.answer("❌ Не удалось создать PDF отчет."
            )
    elif callback.data == "png_report":
        png_file = create_png_report()
        if png_file:
            await callback.message.answer_photo(
                FSInputFile(png_file),
                caption="✅ PNG отчет сформирован."
            )
        else:
            await callback.message.answer("❌ Не удалось создать PNG отчет."
            )
    elif callback.data.startswith("excel_report_"):
        symbol = callback.data.split("_")[2]
        excel_file = create_excel_report(symbol)
        if excel_file:
            await callback.message.answer_document(
                FSInputFile(excel_file),
                caption=f"✅ Excel отчет по {symbol} сформирован."
            )
        else:
            await callback.message.answer(f"❌ Не удалось создать Excel отчет по {symbol}."
            )
    elif callback.data.startswith("pdf_report_"):
        symbol = callback.data.split("_")[2]
        pdf_file = create_pdf_report(symbol)
        if pdf_file:
            await callback.message.answer_document(
                FSInputFile(pdf_file),
                caption=f"✅ PDF отчет по {symbol} сформирован."
            )
        else:
            await callback.message.answer(f"❌ Не удалось создать PDF отчет по {symbol}."
            )
    elif callback.data.startswith("png_report_"):
        symbol = callback.data.split("_")[2]
        png_file = create_png_report(symbol)
        if png_buffer == "":
            await callback.message.answer(f"❌ Не удалось создать PNG отчет по {symbol}."
            )
        else:
            await callback.message.answer(f"✅ PNG отчет по {symbol} сформирован."
            )
            await callback.message.answer_photo(
                FSInputFile(png_file),
                caption=f"✅ PNG отчет по {symbol} сформирован."
            )

# Функция для создания клавиатуры с кнопками для генерации отчетов
def get_report_keyboard(symbol=None):
    if symbol:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="Excel", callback_data=f"excel_report_{symbol}"),
                    InlineKeyboardButton(text="pdf", callback_data=f"pdf_report_{symbol}"),
                    InlineKeyboardButton(text="png", callback_data=f"png_report_{symbol}"),
                ],
            ]
        )
    else:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="Excel", callback_data="excel_report"),
                    InlineKeyboardButton(text="pdf", callback_data="pdf_report"),
                    InlineKeyboardButton(text="png", callback_data="png_report"),
                ],
            ]
        )

# Запуск бота
if __name__ == "__main__":
    conn = connect_to_db()
    if conn:
        create_tables_if_not_exist(conn)
        conn.close()
    dp.run_polling(bot)