import requests
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

# Токен вашего бота (замените на ваш токен)
TELEGRAM_BOT_TOKEN = "ВАШ_TELEGRAM_BOT_TOKEN"
# ID чата с пользователем (можно получить через /start в боте)
TELEGRAM_CHAT_ID = "ВАШ_CHAT_ID"

# Конкретные пары для анализа
SPECIFIC_PAIRS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "LINKUSDT", "TONUSDT"]

bot = Bot(token=TELEGRAM_BOT_TOKEN)
dp = Dispatcher(bot)

def get_order_book(symbol, limit=100):
    """
    Получает стакан ордеров для указанной торговой пары.
    """
    url = f"https://fapi.binance.com/fapi/v1/depth?symbol={symbol}&limit={limit}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Ошибка получения стакана ордеров для {symbol}.")
        return None

def analyze_order_book(order_book):
    """
    Анализирует дисбаланс между заявками на покупку и продажу.
    """
    bids = order_book['bids']  # Покупки
    asks = order_book['asks']  # Продажи

    # Суммируем объемы заявок
    total_bid_volume = sum([float(bid[1]) for bid in bids])  # bid[1] = объем
    total_ask_volume = sum([float(ask[1]) for ask in asks])  # ask[1] = объем

    # Проверяем, чтобы суммарный объем не был равен нулю
    if total_bid_volume + total_ask_volume == 0:
        return total_bid_volume, total_ask_volume, 0  # Дисбаланс = 0, если объемы нулевые

    imbalance = (total_bid_volume - total_ask_volume) / (total_bid_volume + total_ask_volume) * 100

    return total_bid_volume, total_ask_volume, imbalance

def analyze_specific_pairs():
    """
    Анализирует дисбаланс для конкретных пар.
    """
    results = []
    for pair in SPECIFIC_PAIRS:
        order_book = get_order_book(pair)
        if order_book:
            bid_volume, ask_volume, imbalance = analyze_order_book(order_book)
            result = (
                f"Анализ стакана для {pair}:\n"
                f"Общий объем покупок (bids): {bid_volume:.2f}\n"
                f"Общий объем продаж (asks): {ask_volume:.2f}\n"
                f"Дисбаланс: {imbalance:.2f}%\n"
            )
            if imbalance > 10:
                result += "Рынок перекуплен (давление покупателей).\n"
            elif imbalance < -10:
                result += "Рынок перепродан (давление продавцов).\n"
            else:
                result += "Рынок сбалансирован.\n"
            results.append(result)
        else:
            results.append(f"Не удалось получить данные стакана ордеров для {pair}.\n")
    return results

def analyze_all_pairs():
    """
    Анализирует общий дисбаланс для всех фьючерсных пар.
    """
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    response = requests.get(url)
    if response.status_code == 200:
        symbols = response.json()['symbols']
        # Фильтруем пары, которые торгуются против USDT
        usdt_pairs = [symbol['symbol'] for symbol in symbols if symbol['quoteAsset'] == 'USDT']
    else:
        return "Не удалось получить список фьючерсных пар."

    total_bid_volume_all = 0
    total_ask_volume_all = 0

    for symbol in usdt_pairs:
        order_book = get_order_book(symbol)
        if order_book:
            bid_volume, ask_volume, imbalance = analyze_order_book(order_book)
            total_bid_volume_all += bid_volume
            total_ask_volume_all += ask_volume

    # Проверяем, чтобы суммарный объем не был равен нулю
    if total_bid_volume_all + total_ask_volume_all == 0:
        return "Суммарный объем покупок и продаж равен нулю. Невозможно рассчитать дисбаланс."

    total_imbalance = (total_bid_volume_all - total_ask_volume_all) / (total_bid_volume_all + total_ask_volume_all) * 100

    result = (
        f"Анализ стакана для всех фьючерсных пар:\n"
        f"Общий объем покупок (bids): {total_bid_volume_all:.2f}\n"
        f"Общий объем продаж (asks): {total_ask_volume_all:.2f}\n"
        f"Общий дисбаланс: {total_imbalance:.2f}%\n"
    )

    if total_imbalance > 10:
        result += "Рынок перекуплен (давление покупателей)."
    elif total_imbalance < -10:
        result += "Рынок перепродан (давление продавцов)."
    else:
        result += "Рынок сбалансирован."

    return result

async def send_message(chat_id, message):
    """
    Отправляет сообщение в Telegram.
    """
    await bot.send_message(chat_id=chat_id, text=message)

async def job():
    """
    Задача, которая выполняется каждую минуту.
    """
    print("Анализ рынка...")

    # Анализ конкретных пар
    specific_results = analyze_specific_pairs()
    for result in specific_results:
        await send_message(TELEGRAM_CHAT_ID, result)

    # Анализ всех пар
    all_pairs_result = analyze_all_pairs()
    await send_message(TELEGRAM_CHAT_ID, all_pairs_result)

    print("Сообщения отправлены.")

async def scheduler():
    """
    Запуск задачи каждую минуту.
    """
    while True:
        await job()
        await asyncio.sleep(60)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(scheduler())
    executor.start_polling(dp, skip_updates=True)
