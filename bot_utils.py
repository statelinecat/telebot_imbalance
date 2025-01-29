from aiogram import Bot
from configs import BOT_TOKEN

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