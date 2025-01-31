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
from matplotlib.dates import DateFormatter, HourLocator
from datetime import datetime
import bcrypt
import logging
import asyncio
from logging.handlers import RotatingFileHandler
import sqlite3
import os

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
                InlineKeyboardButton(text="7", callback_data="pin_7"),
                InlineKeyboardButton(text="8", callback_data="pin_8"),
                InlineKeyboardButton(text="9", callback_data="pin_9"),
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

# Функция для создания клавиатуры для отчетов
def get_report_keyboard(symbol=None):
    if symbol:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="Excel", callback_data=f"excel_report_{symbol}"),
                    InlineKeyboardButton(text="PDF", callback_data=f"pdf_report_{symbol}"),
                    InlineKeyboardButton(text="PNG", callback_data=f"png_report_{symbol}"),
                ],
            ]
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Excel", callback_data="excel_report"),
                InlineKeyboardButton(text="PDF", callback_data="pdf_report"),
                InlineKeyboardButton(text="PNG", callback_data="png_report"),
            ],
        ]
    )

# Функция для подключения к базе данных
def connect_to_db():
    try:
        return sqlite3.connect(DATABASE_NAME)
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
    try:
        with connect_to_db() as conn:
            return conn.execute(
                "SELECT chat_id FROM users WHERE chat_id = ?",
                (chat_id,)
            ).fetchone() is not None
    except Exception as e:
        logger.error(f"Ошибка проверки авторизации: {e}")
        return False

# Функция для сохранения chat_id пользователя
def save_chat_id(chat_id):
    try:
        with connect_to_db() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO users (chat_id) VALUES (?)",
                (chat_id,)
            )
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка сохранения chat_id: {e}")
        return False

# Функция для удаления chat_id пользователя
def delete_chat_id(chat_id):
    try:
        with connect_to_db() as conn:
            conn.execute(
                "DELETE FROM users WHERE chat_id = ?",
                (chat_id,)
            )
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Ошибка удаления chat_id: {e}")
        return False


def generate_chart(data, title, color):
    plt.figure(figsize=(20, 8))
    times = [datetime.strptime(row[0][:19], "%Y-%m-%dT%H:%M:%S") for row in data]
    values = [row[1] for row in data]
    plt.bar(
        times, values,
        color=color,
        width=0.8 / 96,
        edgecolor='black',
        linewidth=0.2
    )
    plt.gca().xaxis.set_major_formatter(DateFormatter("%d.%m"))
    plt.gca().xaxis.set_major_locator(HourLocator(interval=24))

    # Формируем название графика
    if "USDT" in title:
        chart_title = f"Дисбаланс {title.strip()} за 30 дней"
    else:
        chart_title = "Дисбаланс рынка за 30 дней"

    plt.title(chart_title, fontsize=14, pad=20)
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    return plt

# Функция для создания PNG отчета
def create_png_report(symbol=None):
    try:
        with connect_to_db() as conn:
            query = """
                SELECT time, dizbalance
                FROM market_pressure
                WHERE symbol = ?
                ORDER BY time DESC
                LIMIT 3000
            """ if symbol else """
                SELECT time, total_dizbalance
                FROM market_summary
                ORDER BY time DESC
                LIMIT 3000
            """
            data = conn.execute(query, (symbol,) if symbol else ()).fetchall()[::-1]
        plt = generate_chart(
            data,
            f" {symbol if symbol else ''} ",
            "#4ECDC4" if symbol else "#FF6B6B"
        )
        filename = f"{symbol}_report.png" if symbol else "market_report.png"
        plt.savefig(filename, dpi=100)
        plt.close()
        return filename
    except Exception as e:
        logger.error(f"Ошибка создания PNG отчета: {e}")
        return None

def create_pdf_report(symbol=None):
    try:
        # Создаем PNG отчет, чтобы использовать те же данные
        png_file = create_png_report(symbol)
        if not png_file:
            return None

        # Получаем данные из базы данных
        with connect_to_db() as conn:
            query = """
                SELECT time, dizbalance
                FROM market_pressure
                WHERE symbol = ?
                ORDER BY time DESC
                LIMIT 3000
            """ if symbol else """
                SELECT time, total_dizbalance
                FROM market_summary
                ORDER BY time DESC
                LIMIT 3000
            """
            data = conn.execute(query, (symbol,) if symbol else ()).fetchall()[::-1]

        # Создаем PDF файл с тем же именем, заменяя расширение
        pdf_file = png_file.replace('.png', '.pdf')

        # Открываем PDF файл для записи
        with PdfPages(pdf_file) as pdf:
            # Генерируем график с данными
            plt = generate_chart(
                data,
                f" {symbol if symbol else ''} ",
                "#4ECDC4" if symbol else "#FF6B6B"
            )
            # Сохраняем график в PDF
            pdf.savefig(plt.gcf())
            # Закрываем график
            plt.close()

        # Удаляем временный PNG-файл
        try:
            os.remove(png_file)
            logger.info(f"Временный файл {png_file} успешно удален.")
        except Exception as e:
            logger.error(f"Ошибка удаления временного файла {png_file}: {e}")

        return pdf_file
    except Exception as e:
        logger.error(f"Ошибка создания PDF отчета: {e}")
        return None

# Функция для создания Excel отчета
def create_excel_report(symbol=None):
    try:
        with connect_to_db() as conn:
            query, columns = (
                """
                SELECT time, bid_volume, ask_volume, dizbalance
                FROM market_pressure
                WHERE symbol = ?
                ORDER BY time DESC
                """,
                ["Time", "Bid", "Ask", "Dizbalance"]
            ) if symbol else (
                """
                SELECT time, total_bid_volume, total_ask_volume, total_dizbalance
                FROM market_summary
                ORDER BY time DESC
                """,
                ["Time", "Total Bid", "Total Ask", "Total Dizbalance"]
            )
            data = conn.execute(query, (symbol,) if symbol else ()).fetchall()
        df = pd.DataFrame(data, columns=columns)
        df['Time'] = pd.to_datetime(df['Time']).dt.strftime("%Y.%m.%d %H:%M")
        filename = f"{symbol}_report.xlsx" if symbol else "market_report.xlsx"
        df.to_excel(filename, index=False)
        return filename
    except Exception as e:
        logger.error(f"Ошибка создания Excel отчета: {e}")
        return None

# Обработчик команды /start
@dp.message(Command("start"))
async def start_handler(message: Message, state: FSMContext):
    if is_user_authorized(message.chat.id):
        await message.answer(
            "✅ Вы авторизованы! Выберите действие:",
            reply_markup=get_main_keyboard()
        )
    else:
        await message.answer(
            "⚠️ Вы не авторизованы. Введите пин-код:",
            reply_markup=get_pin_keyboard()
        )
        await state.set_state(PinCodeState.entering_pin)
        await state.update_data(pin_buffer="", attempts=0)

# Обработчик нажатия кнопки "Готово" для ввода пин-кода
@dp.callback_query(PinCodeState.entering_pin)
async def pin_handler(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    pin_buffer = data.get('pin_buffer', '')
    attempts = data.get('attempts', 0)
    if callback.data == "pin_done":
        if len(pin_buffer) == 4 and bcrypt.checkpw(pin_buffer.encode(), PIN_CODE_HASH.encode()):
            if save_chat_id(callback.message.chat.id):
                await callback.message.answer(
                    "✅ Пин-код принят!",
                    reply_markup=get_main_keyboard()
                )
                await state.clear()
            return
        else:
            attempts += 1
            if attempts >= 3:
                await callback.message.answer("Вы заблокированы на 5 минут.")
                await state.set_state(PinCodeState.blocked)
                await asyncio.sleep(300)
                await state.clear()
                return
            await callback.message.answer(
                f"❌ Неверный пин-код. Осталось попыток: {3 - attempts}",
                reply_markup=get_pin_keyboard()
            )
            await state.update_data(pin_buffer="", attempts=attempts)
    else:
        pin_buffer += callback.data.split("_")[1]
        await state.update_data(pin_buffer=pin_buffer)
        await callback.message.edit_text(
            f"⚠️ Вы не авторизованы. Введите пин-код: {'•' * len(pin_buffer)}",
            reply_markup=get_pin_keyboard()
        )
    await callback.answer()

# Обработчик команды /delme для удаления пользователя
@dp.message(Command("delme"))
async def delete_user_handler(message: Message):
    if delete_chat_id(message.chat.id):
        await message.answer("✅ Вы удалены из системы.", reply_markup=get_start_keyboard())
    else:
        await message.answer("❌ Ошибка удаления.", reply_markup=get_start_keyboard())

# Обработчик нажатия на кнопку "Весь рынок"
@dp.callback_query(F.data == "market_summary")
async def market_summary_handler(callback: CallbackQuery):
    with connect_to_db() as conn:
        data = conn.execute("""
            SELECT time, total_bid_volume, total_ask_volume, total_dizbalance
            FROM market_summary
            ORDER BY time DESC
            LIMIT 1
        """).fetchone()
    if data:
        time_str = datetime.strptime(data[0][:19], "%Y-%m-%dT%H:%M:%S").strftime("%d.%m.%Y %H:%M")
        response = (
            "✅ Последний агрегированный отчет:\n"
            f"Время: {time_str}\n"
            f"Общий объем заявок на покупку: {data[1]:.2f}\n"
            f"Общий объем заявок на продажу: {data[2]:.2f}\n"
            f"Диcбаланс: {data[3]:.2f}%"
        )
    else:
        response = "❌ Не удалось получить данные."
    await callback.message.answer(response, reply_markup=get_report_keyboard())

# Обработчик нажатия на кнопку "Выбрать монету"
@dp.callback_query(F.data == "select_coin")
async def select_coin_handler(callback: CallbackQuery):
    await callback.message.answer("Введите тикер монеты (например, BTCUSDT):")

@dp.message(lambda message: message.text.isalnum() and not message.text.startswith('/'))
async def coin_data_handler(message: Message):
    symbol = message.text.upper()
    if not symbol.endswith("USDT"):
        await message.answer("❌ Тикер должен заканчиваться на 'USDT'.", reply_markup=get_main_keyboard())
        return

    with connect_to_db() as conn:
        data = conn.execute(
            """
            SELECT time, bid_volume, ask_volume, dizbalance
            FROM market_pressure
            WHERE symbol = ?
            ORDER BY time DESC
            LIMIT 1
            """,
            (symbol,)
        ).fetchone()

    if data:
        time_str = datetime.strptime(data[0][:19], "%Y-%m-%dT%H:%M:%S").strftime("%d.%m.%Y %H:%M")
        response = (
            f"✅ Данные по монете {symbol}:\n"
            f"Время: {time_str}\n"
            f"Объем заявок на покупку: {data[1]:.2f}\n"
            f"Объем заявок на продажу: {data[2]:.2f}\n"
            f"Дисбаланс: {data[3]:.2f}%"
        )
        await message.answer(response, reply_markup=get_report_keyboard(symbol))
    else:
        await message.answer(
            f"❌ Данные по монете {symbol} не найдены.",
            reply_markup=get_main_keyboard()
        )


@dp.callback_query()
async def report_handler(callback: CallbackQuery):
    action, *params = callback.data.split('_')
    symbol = params[1] if len(params) > 1 else None
    match action:
        case 'excel':
            report = create_excel_report(symbol)
        case 'pdf':
            report = create_pdf_report(symbol)
        case 'png':
            report = create_png_report(symbol)
        case _:
            return
    if report:
        method = callback.message.answer_document if action != 'png' else callback.message.answer_photo
        await method(FSInputFile(report), caption=f"✅ {action.upper()} отчет сформирован.")

        # Удаляем файл после отправки
        try:
            os.remove(report)
            logger.info(f"Файл {report} успешно удален.")
        except Exception as e:
            logger.error(f"Ошибка удаления файла {report}: {e}")
    else:
        await callback.message.answer(f"❌ Не удалось создать {action.upper()} отчет.")

# Функция для создания клавиатуры с кнопками для генерации отчетов
def get_report_keyboard(symbol=None):
    if symbol:
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="Excel", callback_data=f"excel_report_{symbol}"),
                    InlineKeyboardButton(text="PDF", callback_data=f"pdf_report_{symbol}"),
                    InlineKeyboardButton(text="PNG", callback_data=f"png_report_{symbol}"),
                ],
            ]
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Excel", callback_data="excel_report"),
                InlineKeyboardButton(text="PDF", callback_data="pdf_report"),
                InlineKeyboardButton(text="PNG", callback_data="png_report"),
            ],
        ]
    )

# Функция для создания клавиатуры с кнопкой "Готово"
def get_start_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="/start")]],
        resize_keyboard=True,
        selective=True
    )

# Запуск бота
if __name__ == "__main__":
    with connect_to_db() as conn:
        create_tables_if_not_exist(conn)
    dp.run_polling(bot)