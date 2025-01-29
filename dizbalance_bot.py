import sqlite3
from aiogram import Bot, Dispatcher, F
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, Message, ReplyKeyboardRemove
from aiogram.filters import Command
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from configs import BOT_TOKEN, DATABASE_NAME, PIN_CODE  # Импортируем настройки из configs.py

# Настройки бота
API_TOKEN = BOT_TOKEN  # Замените на ваш токен
DB_NAME = DATABASE_NAME
PIN = PIN_CODE

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# Класс состояний для ввода пин-кода
class PinCodeState(StatesGroup):
    entering_pin = State()  # Состояние для ввода пин-кода

# Клавиатура с кнопками
start_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Весь рынок"), KeyboardButton(text="Выбрать монету")]
    ],
    resize_keyboard=True
)

# Цифровая клавиатура для ввода пин-кода
pin_keyboard = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="1"), KeyboardButton(text="2"), KeyboardButton(text="3")],
        [KeyboardButton(text="4"), KeyboardButton(text="5"), KeyboardButton(text="6")],
        [KeyboardButton(text="7"), KeyboardButton(text="8"), KeyboardButton(text="9")],
        [KeyboardButton(text="0")]
    ],
    resize_keyboard=True,
    one_time_keyboard=False  # Клавиатура НЕ исчезает после использования
)

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

# Обработчик команды /start
@dp.message(Command("start"))
async def send_welcome(message: Message, state: FSMContext):
    chat_id = message.chat.id
    if is_user_authorized(chat_id):
        await message.answer(
            "Привет! Я бот для анализа рынка Binance Futures.\n"
            "Что вы хотите сделать?",
            reply_markup=start_keyboard
        )
    else:
        await message.answer(
            "Для авторизации введите пин-код:",
            reply_markup=pin_keyboard
        )
        await state.set_state(PinCodeState.entering_pin)  # Устанавливаем состояние для ввода пин-кода
        await state.update_data(pin_buffer="")  # Создаем буфер для пин-кода

# Обработчик ввода пин-кода
@dp.message(PinCodeState.entering_pin, lambda message: message.text.isdigit() and len(message.text) == 1)
async def handle_pin_digit(message: Message, state: FSMContext):
    current_digit = message.text
    data = await state.get_data()
    pin_buffer = data.get("pin_buffer", "") + current_digit  # Добавляем цифру в буфер
    await state.update_data(pin_buffer=pin_buffer)  # Обновляем буфер

    if len(pin_buffer) == 4:  # Если введено 4 цифры
        entered_pin = pin_buffer
        chat_id = message.chat.id
        if entered_pin == PIN:  # Сравниваем введенный пин-код с константой
            if save_chat_id(chat_id):
                await message.answer(
                    "Авторизация успешна! Теперь вам будут приходить уведомления.",
                    reply_markup=start_keyboard
                )
            else:
                await message.answer("Ошибка при сохранении данных. Попробуйте снова.", reply_markup=pin_keyboard)
        else:
            await message.answer("Неверный пин-код. Попробуйте снова.", reply_markup=pin_keyboard)
        await state.reset_state()  # Сбрасываем состояние после завершения
    else:
        await message.answer(f"Введите еще {4 - len(pin_buffer)} цифр.")  # Подсказка о количестве оставшихся цифр

# Обработчик кнопки "Весь рынок"
@dp.message(F.text == "Весь рынок")
async def show_market_summary(message: Message):
    if not is_user_authorized(message.chat.id):
        await message.answer("Вы не авторизованы. Для авторизации используйте команду /start.")
        return

    latest_summary = get_latest_market_summary()
    if latest_summary:
        time_str, total_bid_volume, total_ask_volume, total_dizbalance = latest_summary
        response = (
            f"Последние агрегированные данные:\n"
            f"  Время: {time_str}\n"
            f"  Общий объем покупок: {total_bid_volume:.2f}\n"
            f"  Общий объем продаж: {total_ask_volume:.2f}\n"
            f"  Общий дисбаланс: {total_dizbalance:.2f}%"
        )
    else:
        response = "Агрегированные данные отсутствуют."
    await message.answer(response)

# Обработчик кнопки "Выбрать монету"
@dp.message(F.text == "Выбрать монету")
async def request_coin_ticker(message: Message):
    if not is_user_authorized(message.chat.id):
        await message.answer("Вы не авторизованы. Для авторизации используйте команду /start.")
        return

    await message.answer("Введите тикер монеты (например: BTCUSDT):")

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
            f"  Время: {time_str}\n"
            f"  Объем покупок: {bid_volume:.2f}\n"
            f"  Объем продаж: {ask_volume:.2f}\n"
            f"  Дисбаланс: {dizbalance:.2f}%"
        )
    else:
        response = f"Данные для {symbol} не найдены."
    await message.answer(response)

# Запуск бота
if __name__ == "__main__":
    # Создаем таблицы при запуске
    conn = connect_to_db()
    if conn:
        create_tables_if_not_exist(conn)
        conn.close()
    dp.run_polling(bot)