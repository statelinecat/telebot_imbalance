import sqlite3
from aiogram import Bot, Dispatcher, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, Message, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton, FSInputFile
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from configs import BOT_TOKEN, DATABASE_NAME, PIN_CODE  # Импортируем настройки из configs.py
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from PIL import Image
from datetime import datetime

# Настройки бота
API_TOKEN = BOT_TOKEN  # Замените на ваш токен
DB_NAME = DATABASE_NAME
PIN = str(PIN_CODE)  # Преобразуем пин-код в строку для корректного сравнения

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# Класс состояний для ввода пин-кода
class PinCodeState(StatesGroup):
    entering_pin = State()  # Состояние для ввода пин-кода
    incorrect_pin = State()  # Состояние для неверного пин-кода

# Функция для создания инлайн-клавиатуры
def get_pin_keyboard():
    """Возвращает инлайн-клавиатуру для ввода пин-кода."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1", callback_data="pin_1"),
         InlineKeyboardButton(text="2", callback_data="pin_2"),
         InlineKeyboardButton(text="3", callback_data="pin_3")],
        [InlineKeyboardButton(text="4", callback_data="pin_4"),
         InlineKeyboardButton(text="5", callback_data="pin_5"),
         InlineKeyboardButton(text="6", callback_data="pin_6")],
        [InlineKeyboardButton(text="7", callback_data="pin_7"),
         InlineKeyboardButton(text="8", callback_data="pin_8"),
         InlineKeyboardButton(text="9", callback_data="pin_9")],
        [InlineKeyboardButton(text="0", callback_data="pin_0"),
         InlineKeyboardButton(text="Готово", callback_data="pin_done")]
    ])

# Функция для создания основной клавиатуры
def get_main_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Весь рынок", callback_data="market_summary")],
        [InlineKeyboardButton(text="Выбрать монету", callback_data="select_coin")],
        [InlineKeyboardButton(text="Отчет Excel", callback_data="excel_report")],
        [InlineKeyboardButton(text="Отчет PNG", callback_data="png_report")],
        [InlineKeyboardButton(text="Отчет PDF", callback_data="pdf_report")]
    ])

# Функция для подключения к базе данных
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
        # Таблица для пользователей
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY
            );
        """)
        conn.commit()
        cursor.close()
        print("Таблицы созданы или уже существуют.")
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")

# Функция для проверки наличия CHAT_ID в базе данных
def is_user_authorized(chat_id):
    conn = connect_to_db()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT chat_id FROM users WHERE chat_id = ?
        """, (chat_id,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result is not None
    except Exception as e:
        print(f"Ошибка при проверке авторизации: {e}")
        return False

# Функция для сохранения CHAT_ID в базу данных
def save_chat_id(chat_id):
    conn = connect_to_db()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT OR IGNORE INTO users (chat_id) VALUES (?)
        """, (chat_id,))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Ошибка при сохранении CHAT_ID: {e}")
        return False

# Функция для удаления CHAT_ID из базы данных
def delete_chat_id(chat_id):
    conn = connect_to_db()
    if not conn:
        return False
    try:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM users WHERE chat_id = ?
        """, (chat_id,))
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Ошибка при удалении CHAT_ID: {e}")
        return False

# Функция для получения последних агрегированных данных из базы данных
def get_latest_market_summary():
    conn = connect_to_db()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT time, total_bid_volume, total_ask_volume, total_dizbalance
            FROM market_summary
            ORDER BY time DESC
            LIMIT 1
        """)
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        print(f"Ошибка при получении данных из базы: {e}")
        return None

# Функция для получения последних данных для конкретной монеты из базы данных
def get_latest_coin_data(symbol):
    conn = connect_to_db()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT time, bid_volume, ask_volume, dizbalance
            FROM market_pressure
            WHERE symbol = ?
            ORDER BY time DESC
            LIMIT 1
        """, (symbol,))
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        print(f"Ошибка при получении данных из базы: {e}")
        return None

# Функция для создания Excel отчета
def create_excel_report():
    conn = connect_to_db()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT time, total_bid_volume, total_ask_volume, total_dizbalance
            FROM market_summary
            ORDER BY time DESC
        """)
        data = cursor.fetchall()
        cursor.close()
        conn.close()

        df = pd.DataFrame(data, columns=["Time", "Total Bid Volume", "Total Ask Volume", "Total Dizbalance"])
        df['Time'] = pd.to_datetime(df['Time']).dt.strftime('%Y.%m.%d %H:%M')
        excel_file = "market_summary_report.xlsx"
        df.to_excel(excel_file, index=False)
        return excel_file
    except Exception as e:
        print(f"Ошибка при создании Excel отчета: {e}")
        return None

# Функция для создания PNG отчета
def create_png_report():
    conn = connect_to_db()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT time, total_dizbalance
            FROM market_summary
            ORDER BY time DESC
            LIMIT 3000
        """)
        data = cursor.fetchall()
        cursor.close()
        conn.close()

        # Изменяем порядок данных: от старых к новым
        data = data[::-1]

        times = [datetime.strptime(row[0][:19], '%Y-%m-%dT%H:%M:%S').strftime('%Y.%m.%d %H:%M') for row in data]
        dizbalances = [row[1] for row in data]

        plt.figure(figsize=(10, 5))
        plt.bar(times, dizbalances, color='skyblue')
        plt.xlabel('Время')
        plt.ylabel('Общий дисбаланс')
        plt.title('Общий дисбаланс за последние 30 дней')
        plt.xticks(rotation=45)
        plt.tight_layout()

        png_file = "market_summary_report.png"
        plt.savefig(png_file)
        plt.close()
        return png_file
    except Exception as e:
        print(f"Ошибка при создании PNG отчета: {e}")
        return None


# Функция для создания PDF отчета
def create_pdf_report():
    conn = connect_to_db()
    if not conn:
        return None
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT time, total_dizbalance
            FROM market_summary
            ORDER BY time DESC
            LIMIT 3000
        """)
        data = cursor.fetchall()
        cursor.close()
        conn.close()

        # Изменяем порядок данных: от старых к новым
        data = data[::-1]

        times = [datetime.strptime(row[0][:19], '%Y-%m-%dT%H:%M:%S').strftime('%Y.%m.%d %H:%M') for row in data]
        dizbalances = [row[1] for row in data]

        plt.figure(figsize=(10, 5))
        plt.bar(times, dizbalances, color='skyblue')
        plt.xlabel('Время')
        plt.ylabel('Общий дисбаланс')
        plt.title('Общий дисбаланс за последние 30 дней')
        plt.xticks(rotation=45)
        plt.tight_layout()

        pdf_file = "market_summary_report.pdf"
        with PdfPages(pdf_file) as pdf_pages:
            pdf_pages.savefig()
        plt.close()
        return pdf_file
    except Exception as e:
        print(f"Ошибка при создании PDF отчета: {e}")
        return None

# Обработчик команды /start
@dp.message(Command("start"))
async def send_welcome(message: Message, state: FSMContext):
    chat_id = message.chat.id
    if is_user_authorized(chat_id):
        await message.answer(
            "Привет! Я бот для анализа рынка Binance Futures.\n"
            "Что вы хотите сделать?",
            reply_markup=get_main_keyboard()
        )
    else:
        await message.answer(
            "Введите пин-код, используя кнопки ниже:",
            reply_markup=get_pin_keyboard()
        )
        await state.set_state(PinCodeState.entering_pin)  # Устанавливаем состояние для ввода пин-кода
        await state.update_data(pin_buffer="")  # Создаем буфер для пин-кода

# Обработчик нажатий на кнопки инлайн-клавиатуры
@dp.callback_query(PinCodeState.entering_pin)
async def handle_pin_digit(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    pin_buffer = data.get("pin_buffer", "")

    if callback.data.startswith("pin_"):
        action = callback.data.split("_")[1]
        if action == "done":  # Если нажата кнопка "Готово"
            if len(pin_buffer) == 4 and pin_buffer == PIN:  # Проверяем пин-код
                chat_id = callback.message.chat.id
                if save_chat_id(chat_id):
                    await callback.message.answer(
                        "Авторизация успешна! Теперь вам будут приходить уведомления.",
                        reply_markup=get_main_keyboard()
                    )
                else:
                    await callback.message.answer("Ошибка при сохранении данных. Попробуйте снова.", reply_markup=get_pin_keyboard())
            else:
                await callback.message.answer("Пин-код неверный.", reply_markup=ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text="/start")]]))
                await state.set_state(PinCodeState.incorrect_pin)  # Устанавливаем состояние для неверного пин-кода
            await state.clear()  # Очищаем состояние после завершения
        else:  # Если нажата цифра
            pin_buffer += action
            await state.update_data(pin_buffer=pin_buffer)
            try:
                await callback.message.edit_text(
                    f"Введите пин-код, используя кнопки ниже: {pin_buffer}",
                    reply_markup=get_pin_keyboard()
                )
            except aiogram.exceptions.TelegramBadRequest as e:
                if "message is not modified" in str(e):
                    print("Сообщение не было изменено.")
                else:
                    raise  # Если это другая ошибка, поднимаем исключение
    await callback.answer()  # Подтверждаем обработку callback

# Обработчик для неверного пин-кода
@dp.message(lambda message: message.text.lower() == "/start", PinCodeState.incorrect_pin)
async def handle_incorrect_pin(message: Message, state: FSMContext):
    await send_welcome(message, state)

# Команда /delme для удаления CHAT_ID из базы данных
@dp.message(Command("delme"))
async def delete_user(message: Message):
    chat_id = message.chat.id
    if is_user_authorized(chat_id):
        if delete_chat_id(chat_id):
            await message.answer("Вы успешно удалены из базы данных. Для повторной авторизации используйте команду /start.")
        else:
            await message.answer("Ошибка при удалении данных. Пожалуйста, попробуйте снова.")
    else:
        await message.answer("Вы не авторизованы. Нет данных для удаления.")

# Обработчик кнопки "Весь рынок"
@dp.callback_query(F.data == "market_summary")
async def show_market_summary(callback: CallbackQuery):
    if not is_user_authorized(callback.message.chat.id):
        await callback.message.answer("Вы не авторизованы. Для авторизации используйте команду /start.")
        return

    latest_summary = get_latest_market_summary()
    if latest_summary:
        time_str, total_bid_volume, total_ask_volume, total_dizbalance = latest_summary
        response = (
            f"Последние агрегированные данные:\n"
            f"  Время: {datetime.strptime(time_str[:19], '%Y-%m-%dT%H:%M:%S').strftime('%Y.%m.%d %H:%M')}\n"
            f"  Общий объем покупок: {total_bid_volume:.2f}\n"
            f"  Общий объем продаж: {total_ask_volume:.2f}\n"
            f"  Общий дисбаланс: {total_dizbalance:.2f}%"
        )
    else:
        response = "Агрегированные данные отсутствуют."
    await callback.message.answer(response)

# Обработчик кнопки "Выбрать монету"
@dp.callback_query(F.data == "select_coin")
async def request_coin_ticker(callback: CallbackQuery):
    if not is_user_authorized(callback.message.chat.id):
        await callback.message.answer("Вы не авторизованы. Для авторизации используйте команду /start.")
        return

    await callback.message.answer("Введите тикер монеты (например: BTCUSDT):")

# Обработчик ввода тикера монеты
@dp.message(lambda message: message.text.isalnum() and len(message.text) >= 6)
async def show_coin_data(message: Message):
    if not is_user_authorized(message.chat.id):
        await message.answer("Вы не авторизованы. Для авторизации используйте команду /start.")
        return

    symbol = message.text.upper()
    latest_data = get_latest_coin_data(symbol)
    if latest_data:
        time_str, bid_volume, ask_volume, dizbalance = latest_data
        response = (
            f"Последние данные для {symbol}:\n"
            f"  Время: {datetime.strptime(time_str[:19], '%Y-%m-%dT%H:%M:%S').strftime('%Y.%m.%d %H:%M')}\n"
            f"  Объем покупок: {bid_volume:.2f}\n"
            f"  Объем продаж: {ask_volume:.2f}\n"
            f"  Дисбаланс: {dizbalance:.2f}%"
        )
    else:
        response = f"Данные для {symbol} не найдены."
    await message.answer(response)

# Обработчик кнопки "Отчет Excel"
@dp.callback_query(F.data == "excel_report")
async def send_excel_report(callback: CallbackQuery):
    if not is_user_authorized(callback.message.chat.id):
        await callback.message.answer("Вы не авторизованы. Для авторизации используйте команду /start.")
        return

    excel_file = create_excel_report()
    if excel_file:
        await callback.message.answer_document(FSInputFile(excel_file), caption="Отчет Excel с агрегированными данными")
    else:
        await callback.message.answer("Ошибка при создании Excel отчета.")

# Обработчик кнопки "Отчет PNG"
@dp.callback_query(F.data == "png_report")
async def send_png_report(callback: CallbackQuery):
    if not is_user_authorized(callback.message.chat.id):
        await callback.message.answer("Вы не авторизованы. Для авторизации используйте команду /start.")
        return

    png_file = create_png_report()
    if png_file:
        await callback.message.answer_photo(FSInputFile(png_file), caption="Отчет PNG с гистограммой агрегированных данных")
    else:
        await callback.message.answer("Ошибка при создании PNG отчета.")

# Обработчик кнопки "Отчет PDF"
@dp.callback_query(F.data == "pdf_report")
async def send_pdf_report(callback: CallbackQuery):
    if not is_user_authorized(callback.message.chat.id):
        await callback.message.answer("Вы не авторизованы. Для авторизации используйте команду /start.")
        return

    pdf_file = create_pdf_report()
    if pdf_file:
        await callback.message.answer_document(FSInputFile(pdf_file), caption="Отчет PDF с гистограммой агрегированных данных за последние 30 дней")
    else:
        await callback.message.answer("Ошибка при создании PDF отчета.")

# Запуск бота
if __name__ == "__main__":
    # Создаем таблицы при запуске
    conn = connect_to_db()
    if conn:
        create_tables_if_not_exist(conn)
        conn.close()
    dp.run_polling(bot)

