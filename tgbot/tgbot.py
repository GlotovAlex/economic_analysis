from telegram import Bot
import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

bot = Bot(token=TOKEN)

async def test_send():
    await bot.send_message(chat_id=CHAT_ID, text="Тестовое сообщение от бота!")

if __name__ == "__main__":
    import asyncio
    asyncio.run(test_send())