import logging
import json
import asyncio

from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import Message

from main import aggregate

logging.basicConfig(level=logging.INFO)

API_TOKEN = '6798517132:AAH_HSwiRdK4DDijo46i_K314e-mtYYAH_w'

bot = Bot(token=API_TOKEN)
router = Router()

@router.message(F.text == '/start')
async def start(message: Message):
    await message.reply("""Привет! Отправьте мне JSON с данными для агрегации.
Пример: {
   "dt_from": "2022-09-01T00:00:00",
   "dt_upto": "2022-12-31T23:59:00",
   "group_type": "month"
}
""")


@router.message(F.text)
async def aggregate_data(message: Message):
    try:
        data = json.loads(message.text)
        result = await aggregate(data["dt_from"], data["dt_upto"], data["group_type"])
        await message.reply(json.dumps(result))
    except Exception as e:
        await message.reply(f"Ошибка: {e}")


async def main():
    dp = Dispatcher()
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())

