#!/usr/bin/env python3
import logging
import os
from aiogram import Bot, Dispatcher, types
from aiogram import executor
from dotenv import load_dotenv

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Получение токена из переменных окружения
API_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
if not API_TOKEN:
    logger.error("Не удалось загрузить TELEGRAM_BOT_TOKEN из .env файла")
    exit(1)

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

@dp.message_handler(commands=['start', 'help'])
async def send_welcome(message: types.Message):
    """Обработка команд /start и /help"""
    try:
        await message.reply(
            "👋 Привет! Я бот для отправки благодарностей.\n\n"
            "Отправь мне сообщение в формате:\n"
            "<code>@username текст сообщения</code>\n\n"
            "Пример:\n"
            "<code>@user123 спасибо за помощь!</code>",
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Ошибка в send_welcome: {e}")

@dp.message_handler()
async def process_message(message: types.Message):
    """Обработка текстовых сообщений"""
    try:
        text = message.text
        
        # Разделяем текст на части
        parts = text.split(maxsplit=1)
        
        if len(parts) < 2:
            await message.reply("ℹ️ Пожалуйста, укажите @username и сообщение")
            return
        
        username, user_message = parts
        username = username.strip()
        user_message = user_message.strip()
        
        # Проверяем, что username начинается с @
        if not username.startswith('@'):
            await message.reply("❌ Первым словом должен быть @username")
            return
        
        # Логируем полученное сообщение
        logger.info(f"Новое сообщение от {message.from_user.id}: {username} {user_message}")
        
        # Отправляем подтверждение
        await message.reply(
            f"✅ Сообщение принято!\n\n"
            f"👤 Аккаунт: <code>{username}</code>\n"
            f"📝 Текст: <code>{user_message}</code>",
            parse_mode='HTML'
        )
        
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}")
        await message.reply("⚠️ Произошла ошибка при обработке вашего сообщения")

if __name__ == '__main__':
    logger.info("Запуск бота...")
    executor.start_polling(dp, skip_updates=True)
