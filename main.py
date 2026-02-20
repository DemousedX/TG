# main.py

# Required imports
import logging
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher import Dispatcher
from aiogram.utils import executor
from aiogram.types import ParseMode, ChatType

# Constants
API_TOKEN = 'YOUR_API_TOKEN'

# Initial setup
logging.basicConfig(level=logging.INFO)
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# Start command
@dp.message_handler(commands=['start'])
async def start_cmd(message: types.Message):
    await message.answer("Welcome! Use /webapp to start the web application.")

if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)