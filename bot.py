#!/usr/bin/env python3
import logging
import psycopg2
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram import executor
from dotenv import load_dotenv
import os

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения
load_dotenv()

# Конфигурация
API_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
if not API_TOKEN:
    logger.error("Не указан TELEGRAM_BOT_TOKEN в .env файле")
    exit(1)

# Конфигурация PostgreSQL
DB_CONFIG = {
    "dbname": os.getenv('DB_NAME', 'thanks_bot_db'),
    "user": os.getenv('DB_USER', 'bot_user'),
    "password": os.getenv('DB_PASSWORD', 'secure_password'),
    "host": os.getenv('DB_HOST', 'localhost')
}

# Инициализация бота
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)

async def init_db():
    """Инициализация таблиц в базе данных"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS thanks_messages (
                id SERIAL PRIMARY KEY,
                sender_id BIGINT NOT NULL,
                sender_username VARCHAR(100),
                recipient_username VARCHAR(100) NOT NULL,
                message_text TEXT NOT NULL,
                message_date DATE NOT NULL DEFAULT CURRENT_DATE,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_message_date 
            ON thanks_messages(message_date)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_recipient 
            ON thanks_messages(recipient_username)
        """)
        
        conn.commit()
        logger.info("База данных инициализирована")
    except Exception as e:
        logger.error(f"Ошибка при инициализации БД: {e}")
        raise
    finally:
        if conn:
            conn.close()

async def save_to_db(message: types.Message, recipient: str, text: str):
    """Сохранение сообщения в базу данных"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO thanks_messages 
            (sender_id, sender_username, recipient_username, message_text, message_date)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        """, (
            message.from_user.id,
            message.from_user.username,
            recipient,
            text,
            datetime.now().strftime('%d-%m-%Y')
        ))
        
        message_id = cursor.fetchone()[0]
        conn.commit()
        logger.info(f"Сообщение #{message_id} сохранено в БД")
        return True
    except Exception as e:
        logger.error(f"Ошибка при сохранении в БД: {e}")
        return False
    finally:
        if conn:
            conn.close()

@dp.message_handler(commands=['start', 'help'])
async def send_welcome(message: types.Message):
    """Обработка команд /start и /help"""
    try:
        await message.reply(
            "👋 Привет! Я бот для отправки благодарностей.\n\n"
            "Просто напиши мне в формате:\n"
            "<code>@username текст_благодарности</code>\n\n"
            "Пример:\n"
            "<code>@kolya спасибо за помощь с проектом!</code>\n\n"
            "Все сообщения сохраняются в нашу базу благодарностей 💾",
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Ошибка в send_welcome: {e}")

@dp.message_handler(commands=['stats'])
async def show_stats(message: types.Message):
    """Показывает статистику благодарностей"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Получаем общее количество сообщений
        cursor.execute("SELECT COUNT(*) FROM thanks_messages")
        total = cursor.fetchone()[0]
        
        # Получаем топ-5 получателей
        cursor.execute("""
            SELECT recipient_username, COUNT(*) as cnt 
            FROM thanks_messages 
            GROUP BY recipient_username 
            ORDER BY cnt DESC 
            LIMIT 5
        """)
        top_recipients = "\n".join(
            [f"{row[0]} - {row[1]}" for row in cursor.fetchall()]
        )
        
        await message.reply(
            f"📊 Статистика благодарностей:\n\n"
            f"Всего сообщений: {total}\n\n"
            f"Топ-5 получателей:\n{top_recipients}",
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        await message.reply("⚠️ Не удалось получить статистику")
    finally:
        if conn:
            conn.close()

@dp.message_handler()
async def process_message(message: types.Message):
    """Обработка текстовых сообщений"""
    try:
        text = message.text.strip()
        
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
        
        # Сохраняем в базу данных
        success = await save_to_db(message, username, user_message)
        
        if success:
            await message.reply(
                f"✅ Сообщение принято!\n\n"
                f"👤 Кому: <code>{username}</code>\n"
                f"📝 Текст: <code>{user_message}</code>\n"
                f"📅 Дата: <code>{datetime.now().strftime('%d-%m-%Y')}</code>",
                parse_mode='HTML'
            )
        else:
            await message.reply("⚠️ Сообщение не сохранено. Ошибка базы данных")
            
    except Exception as e:
        logger.error(f"Ошибка при обработке сообщения: {e}")
        await message.reply("⚠️ Произошла ошибка при обработке вашего сообщения")

async def on_startup(dp):
    """Действия при запуске бота"""
    logger.info("Бот запускается...")
    await init_db()
    logger.info("Бот готов к работе!")

if __name__ == '__main__':
    logger.info("Запуск бота...")
    executor.start_polling(
        dp, 
        skip_updates=True,
        on_startup=on_startup
    )
