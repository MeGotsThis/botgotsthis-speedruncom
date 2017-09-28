import datetime

from bot.coroutine import background
from . import tasks


async def call_refresh(timestamp: datetime.datetime) -> None:
    await tasks.refresh(timestamp)


background.add_task(call_refresh, datetime.timedelta(seconds=0.5))
