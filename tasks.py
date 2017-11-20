from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union  # noqa: F401

import aioodbc.cursor

import bot
from bot import data, globals  # noqa: F401
from lib.database import DatabaseMain

from .library import speedruncom, speedrundata

calls: List[datetime] = []
callLimit: int = 90
callDuration: timedelta = timedelta(minutes=1)

leaderboardCache: timedelta = timedelta(minutes=60)
cache: timedelta = timedelta(hours=24)

if bot.config.development:
    leaderboardCache = timedelta(minutes=5)
    cache = timedelta(minutes=30)


async def refresh(timestamp: datetime) -> None:
    global calls
    if 'srcRefreshCalls' not in globals.globalSessionData:
        globals.globalSessionData['srcRefreshCalls'] = []
    calls = globals.globalSessionData['srcRefreshCalls']
    recentCalls: List[datetime]
    recentCalls = [c for c in calls if timestamp - c < callDuration]
    if len(recentCalls) >= callLimit:
        return
    globals.globalSessionData['srcRefreshCalls'] = recentCalls
    calls = recentCalls

    t: str
    id: Union[speedrundata.LeaderboardId, str]
    for t, id in ((t, i) for t, i in speedruncom.cache.copy()
                  if t == 'leaderboards'):
        assert isinstance(id, speedrundata.LeaderboardId)
        if (t, id) not in speedruncom.cache:
            continue
        if timestamp - speedruncom.cache[t, id] < leaderboardCache:
            continue
        requestTime: datetime = speedruncom.leaderboardRequest[id]
        if timestamp - requestTime > leaderboardCache * 2:
            continue
        speedruncom.cache[t, id] = timestamp
        calls.append(timestamp)
        await speedruncom.read_leaderboard(id, timestamp)
        return
    for t, id in ((t, i) for t, i in speedruncom.cache if t == 'games'):
        assert isinstance(id, str)
        if (t, id) not in speedruncom.cache:
            continue
        if timestamp - speedruncom.cache[t, id] < cache:
            continue
        speedruncom.cache[t, id] = timestamp
        calls.append(timestamp)
        await speedruncom.read_speedrun_game_by_id(id, timestamp)
        return
    oldCache = ((t, i) for t, i in speedruncom.cache.copy()
                if t in ['gameSearch', 'bestSearch'])
    for t, search in oldCache:
        assert isinstance(search, str)
        if (t, search) not in speedruncom.cache:
            continue
        if timestamp - speedruncom.cache[t, search] < cache:
            continue
        speedruncom.cache[t, search] = timestamp
        calls.append(timestamp)
        calls.append(timestamp)
        await speedruncom.read_speedrun_search_game(search, timestamp)
        return
    for t, id in ((t, i) for t, i in speedruncom.cache.copy()
                  if t == 'playerLookup'):
        assert isinstance(id, str)
        if timestamp - speedruncom.cache[t, id] < cache:
            continue
        speedruncom.cache[t, id] = timestamp
        calls.append(timestamp)
        calls.append(timestamp)
        calls.append(timestamp)
        calls.append(timestamp)
        await speedruncom.read_user(id, timestamp)
        return
    for t, id in ((t, i) for t, i in speedruncom.cache.copy()
                  if t == 'platforms'):
        assert isinstance(id, str)
        if (t, id) not in speedruncom.cache:
            continue
        if timestamp - speedruncom.cache[t, id] < cache:
            continue
        speedruncom.cache[t, id] = timestamp
        calls.append(timestamp)
        await speedruncom.read_platforms(timestamp)
        return
    for t, id in ((t, i) for t, i in speedruncom.cache.copy()
                  if t == 'regions'):
        assert isinstance(id, str)
        if (t, id) not in speedruncom.cache:
            continue
        if timestamp - speedruncom.cache[t, id] < cache:
            continue
        speedruncom.cache[t, id] = timestamp
        calls.append(timestamp)
        await speedruncom.read_regions(timestamp)
        return

    # Proactive loading
    db: DatabaseMain
    async with DatabaseMain.acquire() as db, await db.cursor() as cursor:
        await load_info(timestamp, cursor)


async def load_info(timestamp: datetime,
                    cursor: aioodbc.cursor.Cursor) -> None:
    activeChannels = await speedruncom.channels_active(cursor)
    live: List[str] = [c for c, ch in globals.channels.items()
                       if (ch.isStreaming or bot.config.development)
                       if c in activeChannels]
    channel: str
    for channel in live:
        if channel not in globals.channels:
            continue
        chat: data.Channel = globals.channels[channel]
        user: str = await speedruncom.channel_user(cursor, chat)
        if need_load_channel(user, timestamp):
            calls.append(timestamp)
            calls.append(timestamp)
            calls.append(timestamp)
            calls.append(timestamp)
            await speedruncom.read_user(user, timestamp)
            return
        gameId: Optional[str] = await speedruncom.channel_gameid(cursor, chat)
        if gameId is None and chat.twitchGame:
            game: str = chat.twitchGame.lower()
            gameId = await speedruncom.twitch_gameid(cursor, game)
            if not gameId:
                if need_load_game_search(game, timestamp):
                    calls.append(timestamp)
                    await speedruncom.read_speedrun_search_game(
                        game, timestamp)
                    return
                if game in speedruncom.gameSearch:
                    gameId = speedruncom.gameSearch[game]
        if gameId is None:
            continue
        if need_load_game(gameId, timestamp):
            calls.append(timestamp)
            await speedruncom.read_speedrun_game_by_id(gameId, timestamp)
            return
        if gameId not in speedruncom.games:
            return
        levelId: Optional[str] = await speedruncom.channel_levelid(
            cursor, chat, gameId)
        maybeCategoryId: Optional[str] = await speedruncom.channel_categoryid(
            cursor, chat, gameId, levelId)
        categoryId: str
        if maybeCategoryId is not None:
            categoryId = maybeCategoryId
        else:
            cId: Optional[str] = speedruncom.default_categoryid(
                speedruncom.games[gameId].game_categories)
            if cId is None:
                return
            categoryId = cId
        variables: Dict[str, str] = speedruncom.default_sub_categories(
            speedruncom.games[gameId], levelId, categoryId)
        variables.update(await speedruncom.channel_variables(
            cursor, chat, gameId, levelId, categoryId))
        regionId: Optional[str]
        platformId: Optional[str]
        regionId = await speedruncom.channel_region(cursor, chat, gameId)
        platformId = await speedruncom.channel_platform(cursor, chat, gameId)
        leaderboardId: speedrundata.LeaderboardId = speedrundata.LeaderboardId(
            gameId, levelId, categoryId, regionId, platformId, variables)
        if need_load_leaderboard(leaderboardId, timestamp):
            calls.append(timestamp)
            await speedruncom.load_leaderboard(leaderboardId, timestamp)
            return
        speedruncom.active_leaderboard(leaderboardId, timestamp)


def need_load_channel(channel: str,
                      timestamp: datetime) -> bool:
    if ('playerLookup', channel) not in speedruncom.cache:
        return True
    if timestamp - speedruncom.cache['playerLookup', channel] >= cache:
        return True
    return False


def need_load_game_search(game: str,
                          timestamp: datetime) -> bool:
    if ('gameSearch', game) not in speedruncom.cache:
        return True
    if timestamp - speedruncom.cache['gameSearch', game] >= cache:
        return True
    return False


def need_load_game(gameId: str,
                   timestamp: datetime) -> bool:
    if ('games', gameId) not in speedruncom.cache:
        return True
    if timestamp - speedruncom.cache['games', gameId] >= cache:
        return True
    return False


def need_load_leaderboard(leaderboardId: speedrundata.LeaderboardId,
                          timestamp: datetime) -> bool:
    if ('leaderboards', leaderboardId) not in speedruncom.cache:
        return True
    if timestamp - speedruncom.cache['leaderboards', leaderboardId] >= cache:
        return True
    return False
