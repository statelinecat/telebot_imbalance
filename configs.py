from decouple import config

# Замените PIN_CODE на PIN_CODE_HASH
BOT_TOKEN = config("BOT_TOKEN")
PIN_CODE_HASH = config("PIN_CODE_HASH")  # Теперь здесь хранится хеш
DATABASE_NAME = config("DATABASE_NAME")