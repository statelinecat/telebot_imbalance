import bcrypt
pin = "1212".encode()
hashed_pin = bcrypt.hashpw(pin, bcrypt.gensalt())
print(hashed_pin.decode())  # Скопируйте вывод в .env


# Скрипт для генерации хеша (запустите один раз)
# import bcrypt
# pin = "1212".encode()  # Ваш исходный PIN-код
# hashed_pin = bcrypt.hashpw(pin, bcrypt.gensalt())
# print(hashed_pin.decode())  # Скопируйте вывод в .env