import urllib.parse
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Set, Tuple, Union  # noqa: F401,E501

import aiohttp
import aioodbc.cursor

import bot
from bot import data, utils  # noqa: F401
from bot.coroutine import logging
from lib.helper import message
from lib.data import Send
from lib.database import DatabaseMain
from . import speedrundata

dateFormat = '%b %d, %Y'

cache: Dict[Tuple[str, Union[speedrundata.LeaderboardId, str]], datetime] = {}
leaderboardRequest: Dict[speedrundata.LeaderboardId, datetime] = {}

twitchPlayer: Dict[str, Optional[str]] = {}
playerLookup: Dict[str, Optional[str]] = {}
gameSearch: Dict[str, Optional[str]] = {}
bestSearch: Dict[str, Optional[str]] = {}

platforms: Dict[str, speedrundata.Platform] = {}
regions: Dict[str, speedrundata.Region] = {}
games: Dict[str, speedrundata.Game] = {}
levels: Dict[Optional[str], Optional[speedrundata.Level]] = {None: None}
categories: Dict[str, speedrundata.Category] = {}
variables: Dict[str, speedrundata.Variable] = {}
players: Dict[str, speedrundata.Player] = {}
runs: Dict[str, speedrundata.Run] = {}
leaderboards: Dict[speedrundata.LeaderboardId, speedrundata.Leaderboard] = {}


async def channels_active(cursor: aioodbc.cursor.Cursor) -> List[str]:
    query: str = 'SELECT broadcaster FROM chat_features WHERE feature=?'
    await cursor.execute(query, ('speedrun.com',))
    return [r[0] async for r in cursor]


async def channel_user(cursor: aioodbc.cursor.Cursor,
                       chat: 'data.Channel') -> str:
    query: str = 'SELECT userid FROM speedruncom_user WHERE broadcaster=?'
    await cursor.execute(query, (chat.channel,))
    row: Optional[Tuple[str]] = await cursor.fetchone()
    return row[0] if row is not None else chat.channel


async def channel_gameid(cursor: aioodbc.cursor.Cursor,
                         chat: 'data.Channel') -> Optional[str]:
    query: str = 'SELECT game FROM speedruncom_game WHERE broadcaster=?'
    await cursor.execute(query, (chat.channel,))
    row: Optional[Tuple[str]] = await cursor.fetchone()
    return row[0] if row is not None else None


async def channel_levelid(cursor: aioodbc.cursor.Cursor,
                          chat: 'data.Channel',
                          gameid: str) -> Optional[str]:
    query: str = '''
SELECT level FROM speedruncom_level WHERE broadcaster=? AND game=?
'''
    await cursor.execute(query, (chat.channel, gameid))
    row: Optional[Tuple[str]] = await cursor.fetchone()
    return row[0] if row is not None and row[0] else None


async def channel_categoryid(cursor: aioodbc.cursor.Cursor,
                             chat: 'data.Channel',
                             gameid: str,
                             levelid: Optional[str]) -> Optional[str]:
    query: str = '''
SELECT category FROM speedruncom_category
    WHERE broadcaster=? AND game=? AND level=?
'''
    await cursor.execute(query, (chat.channel, gameid, levelid or ''))
    row: Optional[Tuple[str]] = await cursor.fetchone()
    return row[0] if row is not None else None


async def channel_variable(cursor: aioodbc.cursor.Cursor,
                           chat: 'data.Channel',
                           gameid: str,
                           levelid: Optional[str],
                           categoryid: Optional[str],
                           variableid: str) -> Optional[str]:
    query: str = '''
SELECT value FROM speedruncom_variable
    WHERE broadcaster=? AND game=? AND level=? AND category=?
        AND variable=?
'''
    params: Tuple[Any, ...]
    params = chat.channel, gameid, levelid or '', categoryid or '', variableid
    await cursor.execute(query, params)
    row: Optional[Tuple[str]] = await cursor.fetchone()
    return row[0] if row is not None else None


async def channel_variables(cursor: aioodbc.cursor.Cursor,
                            chat: 'data.Channel',
                            gameid: str,
                            levelid: Optional[str],
                            categoryid: Optional[str]) -> Dict[str, str]:
    query: str = '''
SELECT variable, value FROM speedruncom_variable
    WHERE broadcaster=? AND game=? AND level=? AND category=?
'''
    variableValues: Dict[str, str] = {}
    params: Tuple[Any, ...]
    params = chat.channel, gameid, levelid or '', categoryid or ''
    variableId: str
    value: str
    async for variableId, value in await cursor.execute(query, params):
        variable: speedrundata.Variable = variables[variableId]
        if variable.scope == 'full-game':
            if levelid is not None:
                continue
        elif variable.scope == 'all-levels':
            if levelid is None:
                continue
        elif variable.scope == 'single-level':
            if levelid != variable.levelId:
                continue
        elif variable.scope != 'global':
            continue
        if (variable.categoryId is not None
                and variable.categoryId != categoryid):
            continue
        variableValues[variableId] = value
    return variableValues


async def channel_region(cursor: aioodbc.cursor.Cursor,
                         chat: 'data.Channel',
                         gameid: str) -> Optional[str]:
    query: str = '''
SELECT region FROM speedruncom_game_options WHERE broadcaster=? AND game=?
'''
    await cursor.execute(query, (chat.channel, gameid))
    row: Optional[Tuple[Optional[str]]] = await cursor.fetchone()
    return row[0] if row is not None else None


async def channel_platform(cursor: aioodbc.cursor.Cursor,
                           chat: 'data.Channel',
                           gameid: str) -> Optional[str]:
    query: str = '''
SELECT platform FROM speedruncom_game_options WHERE broadcaster=? AND game=?
'''
    await cursor.execute(query, (chat.channel, gameid))
    row: Optional[Tuple[Optional[str]]] = await cursor.fetchone()
    return row[0] if row is not None else None


async def clear_user(database: DatabaseMain,
                     channel: str) -> bool:
    cursor: aioodbc.cursor.Cursor
    async with await database.cursor() as cursor:
        query: str = 'DELETE FROM speedruncom_user WHERE broadcaster=?'
        await cursor.execute(query, (channel,))
        await database.commit()
        return cursor.rowcount != 0


async def set_user(database: DatabaseMain,
                   channel: str,
                   identifier: str) -> bool:
    cursor: aioodbc.cursor.Cursor
    async with await database.cursor() as cursor:
        query: str
        params: Tuple[Any, ...]
        if database.isSqlite:
            query = '''
REPLACE INTO speedruncom_user (broadcaster, userid) VALUES (?, ?)
'''
            params = channel, identifier,
        else:
            query = '''\
INSERT INTO speedruncom_user (broadcaster, userid) VALUES (?, ?)
    ON CONFLICT ON CONSTRAINT speedruncom_user_pkey
    DO UPDATE SET userid=?
'''
            params = channel, identifier, identifier,
        await cursor.execute(query, params)
        await database.commit()
        return cursor.rowcount != 0


async def clear_game(database: DatabaseMain,
                     channel: str) -> bool:
    cursor: aioodbc.cursor.Cursor
    async with await database.cursor() as cursor:
        query: str = 'DELETE FROM speedruncom_game WHERE broadcaster=?'
        await cursor.execute(query, (channel,))
        await database.commit()
        return cursor.rowcount != 0


async def set_game(database: DatabaseMain,
                   channel: str,
                   gameId: str) -> bool:
    cursor: aioodbc.cursor.Cursor
    async with await database.cursor() as cursor:
        query: str
        params: Tuple[Any, ...]
        if database.isSqlite:
            query = '''
REPLACE INTO speedruncom_game (broadcaster, game) VALUES (?, ?)
'''
            params = channel, gameId,
        else:
            query = '''\
INSERT INTO speedruncom_game (broadcaster, game) VALUES (?, ?)
    ON CONFLICT ON CONSTRAINT speedruncom_game_pkey
    DO UPDATE SET game=?
'''
            params = channel, gameId, gameId,
        await cursor.execute(query, params)
        await database.commit()
        return cursor.rowcount != 0


async def clear_level(database: DatabaseMain,
                      channel: str,
                      gameId: str) -> bool:
    cursor: aioodbc.cursor.Cursor
    async with await database.cursor() as cursor:
        query: str = '''
DELETE FROM speedruncom_level WHERE broadcaster=? AND game=?
'''
        await cursor.execute(query, (channel, gameId))
        await database.commit()
        return cursor.rowcount != 0


async def set_level(database: DatabaseMain,
                    channel: str,
                    gameId: str,
                    levelId: str) -> bool:
    cursor: aioodbc.cursor.Cursor
    async with await database.cursor() as cursor:
        query: str
        params: Tuple[Any, ...]
        if database.isSqlite:
            query = '''
REPLACE INTO speedruncom_level (broadcaster, game, level) VALUES (?, ?, ?)
'''
            params = channel, gameId, levelId
        else:
            query = '''\
INSERT INTO speedruncom_level (broadcaster, game, level) VALUES (?, ?, ?)
    ON CONFLICT ON CONSTRAINT speedruncom_level_pkey
    DO UPDATE SET level=?
'''
            params = channel, gameId, levelId, levelId,
        await cursor.execute(query, params)
        await database.commit()
        return cursor.rowcount != 0


async def clear_category(database: DatabaseMain,
                         channel: str,
                         gameId: str,
                         levelId: Optional[str]) -> bool:
    cursor: aioodbc.cursor.Cursor
    async with await database.cursor() as cursor:
        query: str = '''
DELETE FROM speedruncom_category
    WHERE broadcaster=? AND game=? AND level=?
'''
        await cursor.execute(query, (channel, gameId, levelId or ''))
        await database.commit()
        return cursor.rowcount != 0


async def set_category(database: DatabaseMain,
                       channel: str,
                       gameId: str,
                       levelId: Optional[str],
                       categoryId: str) -> bool:
    cursor: aioodbc.cursor.Cursor
    async with await database.cursor() as cursor:
        query: str
        params: Tuple[Any, ...]
        if database.isSqlite:
            query = '''
REPLACE INTO speedruncom_category (broadcaster, game, level, category)
    VALUES (?, ?, ?, ?)
'''
            params = channel, gameId, levelId or '', categoryId
        else:
            query = '''\
INSERT INTO speedruncom_category (broadcaster, game, level, category)
    VALUES (?, ?, ?, ?)
    ON CONFLICT ON CONSTRAINT speedruncom_category_pkey
    DO UPDATE SET category=?
'''
            params = channel, gameId, levelId or '', categoryId, categoryId
        await cursor.execute(query, params)
        await database.commit()
        return cursor.rowcount != 0


async def clear_variable(database: DatabaseMain,
                         channel: str,
                         gameId: str,
                         levelId: Optional[str],
                         categoryId: Optional[str],
                         variableId: str) -> bool:
    cursor: aioodbc.cursor.Cursor
    async with await database.cursor() as cursor:
        query: str = '''
DELETE FROM speedruncom_variable
    WHERE broadcaster=? AND game=? AND level=? AND category=?
        AND variable=?
'''
        await cursor.execute(query, (channel, gameId, levelId or '',
                                     categoryId or '', variableId))
        await database.commit()
        return cursor.rowcount != 0


async def set_variable(database: DatabaseMain,
                       channel: str,
                       gameId: str,
                       levelId: Optional[str],
                       categoryId: Optional[str],
                       variableId: str,
                       value: str) -> bool:
    cursor: aioodbc.cursor.Cursor
    async with await database.cursor() as cursor:
        query: str
        params: Tuple[Any, ...]
        if database.isSqlite:
            query = '''
REPLACE INTO speedruncom_variable
    (broadcaster, game, level, category, variable, value)
    VALUES (?, ?, ?, ?, ?, ?)
        '''
            params = (channel, gameId, levelId or '', categoryId or '',
                      variableId, value)
        else:
            query = '''\
INSERT INTO speedruncom_variable
    (broadcaster, game, level, category, variable, value)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT ON CONSTRAINT speedruncom_variable_pkey
    DO UPDATE SET value=?
        '''
            params = (channel, gameId, levelId or '', categoryId or '',
                      variableId, value, value)
        await cursor.execute(query, params)
        await database.commit()
        return cursor.rowcount != 0


async def clear_region(database: DatabaseMain,
                       channel: str,
                       gameId: str) -> bool:
    cursor: aioodbc.cursor.Cursor
    async with await database.cursor() as cursor:
        query: str
        if database.isSqlite:
            query = '''
INSERT OR IGNORE INTO speedruncom_game_options (broadcaster, game)
    VALUES (?, ?)
'''
        else:
            query = '''
INSERT INTO speedruncom_game_options (broadcaster, game) VALUES (?, ?)
    ON CONFLICT DO NOTHING
'''
        await cursor.execute(query, (channel, gameId))
        query = '''
UPDATE speedruncom_game_options SET region=NULL
    WHERE broadcaster=? AND game=?
'''
        await cursor.execute(query, (channel, gameId))
        await database.commit()
        return cursor.rowcount != 0


async def set_region(database: DatabaseMain,
                     channel: str,
                     gameId: str,
                     regionId: str) -> bool:
    cursor: aioodbc.cursor.Cursor
    async with await database.cursor() as cursor:
        query: str
        if database.isSqlite:
            query = '''
INSERT OR IGNORE INTO speedruncom_game_options (broadcaster, game)
    VALUES (?, ?)
'''
        else:
            query = '''
INSERT INTO speedruncom_game_options (broadcaster, game) VALUES (?, ?)
    ON CONFLICT DO NOTHING
'''
        await cursor.execute(query, (channel, gameId))
        query = '''
UPDATE speedruncom_game_options SET region=? WHERE broadcaster=? AND game=?
'''
        await cursor.execute(query, (regionId, channel, gameId))
        await database.commit()
        return cursor.rowcount != 0


async def clear_platform(database: DatabaseMain,
                         channel: str,
                         gameId: str) -> bool:
    cursor: aioodbc.cursor.Cursor
    async with await database.cursor() as cursor:
        query: str
        if database.isSqlite:
            query = '''
INSERT OR IGNORE INTO speedruncom_game_options (broadcaster, game)
    VALUES (?, ?)
'''
        else:
            query = '''
INSERT INTO speedruncom_game_options (broadcaster, game) VALUES (?, ?)
    ON CONFLICT DO NOTHING
'''
        await cursor.execute(query, (channel, gameId))
        await database.commit()
        return cursor.rowcount != 0


async def set_platform(database: DatabaseMain,
                       channel: str,
                       gameId: str,
                       platformId: str) -> bool:
    cursor: aioodbc.cursor.Cursor
    async with await database.cursor() as cursor:
        query: str
        if database.isSqlite:
            query = '''
INSERT OR IGNORE INTO speedruncom_game_options (broadcaster, game)
    VALUES (?, ?)
'''
        else:
            query = '''
INSERT INTO speedruncom_game_options (broadcaster, game) VALUES (?, ?)
    ON CONFLICT DO NOTHING
'''
        await cursor.execute(query, (channel, gameId))
        query = '''
UPDATE speedruncom_game_options SET platform=?
    WHERE broadcaster=? AND game=?
'''
        await cursor.execute(query, (platformId, channel, gameId))
        await database.commit()
        return cursor.rowcount != 0


async def twitch_gameid(cursor: aioodbc.cursor.Cursor,
                        twitchGame: str) -> Optional[str]:
    query: str = '''
SELECT game FROM speedruncom_twitch_game WHERE LOWER(twitchGame)=?
'''
    await cursor.execute(query, (twitchGame.lower(),))
    row: Optional[Tuple[str]] = await cursor.fetchone()
    if row is None:
        return None
    return row[0] if row[0] is not None else ''


async def load_speedruncom_data(timestamp: Optional[datetime]=None) -> None:
    if not platforms and ('platforms', '') not in cache:
        await read_platforms(timestamp)
    if not regions and ('regions', '') not in cache:
        await read_regions(timestamp)


async def load_game(chat: 'data.Channel',
                    cursor: aioodbc.cursor.Cursor,
                    gameId: Optional[str]=None,
                    search: Optional[str]=None,
                    timestamp: Optional[datetime]=None) -> str:
    await load_speedruncom_data(timestamp)
    if search:
        search = search.lower()
        if search not in gameSearch:
            await read_speedrun_search_game(search, timestamp)
            if search in gameSearch and gameSearch[search] is not None:
                await read_speedrun_game_by_id(gameSearch[search], timestamp)
            elif search in bestSearch and bestSearch[search] is not None:
                await read_speedrun_game_by_id(bestSearch[search], timestamp)
        return search
    else:
        if chat.twitchGame is None:
            return ''
        game: str = chat.twitchGame.lower()
        if gameId is None:
            gameId = await twitch_gameid(cursor, game)
        if gameId:
            if gameId not in gameSearch:
                await read_speedrun_game_by_id(gameId, timestamp)
            return gameId
        else:
            if game and game not in gameSearch:
                await read_speedrun_search_game(game, timestamp)
                if game in gameSearch and gameSearch[game] is not None:
                    await read_speedrun_game_by_id(gameSearch[game], timestamp)
                elif game in bestSearch and bestSearch[game] is not None:
                    await read_speedrun_game_by_id(bestSearch[game], timestamp)
            return game


async def load_leaderboard(id: speedrundata.LeaderboardId,
                           timestamp: datetime) -> None:
    active_leaderboard(id, timestamp)
    if id in leaderboards:
        return
    await read_leaderboard(id, timestamp)


async def load_user(identifier: str,
                    timestamp: datetime) -> None:
    identifier = identifier.lower()
    if identifier in playerLookup:
        return
    await read_user(identifier, timestamp)


def active_leaderboard(id: speedrundata.LeaderboardId,
                       timestamp: datetime) -> None:
    leaderboardRequest[id] = timestamp


async def read_speedruncom_api(url: str) -> Dict[str, Any]:
    try:
        utils.print(url)
        logging.log('speedruncom.log', f'{utils.now()} {url}\n')
        headers: Dict[str, str] = {
            'User-Agent': 'MeGotsThis/BotGotsThis',
            }
        session: aiohttp.ClientSession
        response: aiohttp.ClientResponse
        async with aiohttp.ClientSession(raise_for_status=True) as session, \
                session.get(url,
                            timeout=bot.config.httpTimeout,
                            headers=headers) as response:
            return await response.json()
    except ValueError:
        logging.log('speedruncom#error.log', f'{url}\n')
        raise
    except aiohttp.ClientError:
        return {}


async def read_platforms(timestamp: Optional[datetime]=None) -> None:
    now: datetime = utils.now() if timestamp is None else timestamp
    cache['platforms', ''] = now
    url: str = 'http://www.speedrun.com/api/v1/platforms?max=200'
    data_: Dict[str, Any] = await read_speedruncom_api(url)
    if data_ and 'data' in data_ and data_['data']:
        platformData: Dict[Any, Any]
        for platformData in data_['data']:
            platform: speedrundata.Platform
            if platformData['id'] in platforms:
                platform = platforms[platformData['id']]
            else:
                platform = speedrundata.Platform(platformData)
                platforms[platform.id] = platform
            platform.update(platformData)
        cache['platforms', ''] = now


async def read_regions(timestamp: Optional[datetime]=None) -> None:
    now: datetime = utils.now() if timestamp is None else timestamp
    cache['regions', ''] = now
    url: str = 'http://www.speedrun.com/api/v1/regions'
    data_: Dict[str, Any] = await read_speedruncom_api(url)
    if data_ and 'data' in data_ and data_['data']:
        regionData: Dict[Any, Any]
        for regionData in data_['data']:
            region: speedrundata.Region
            if regionData['id'] in regions:
                region = regions[regionData['id']]
            else:
                region = speedrundata.Region(regionData)
                regions[region.id] = region
            region.update(regionData)
        cache['regions', ''] = now


async def read_speedrun_search_game(
        search: str,
        timestamp: Optional[datetime]=None) -> None:
    now: datetime = utils.now() if timestamp is None else timestamp
    cache['gameSearch', search] = now
    cache['bestSearch', search] = now
    url: str
    data_: Dict[str, Any]
    game: Dict[Any, Any]
    correct: List[str]
    url = 'http://www.speedrun.com/api/v1/games/' + urllib.parse.quote(search)
    data_ = await read_speedruncom_api(url)
    if data_ and 'data' in data_ and data_['data']:
        game = data_['data']
        correct = [game['names']['international'].lower(),
                   game['abbreviation'].lower()]
        if search in correct:
            gameSearch[search] = game['id']
            bestSearch[search] = game['id']
            cache['gameSearch', search] = now
            cache['bestSearch', search] = now
            return
    url = ('http://www.speedrun.com/api/v1/games?name='
           + urllib.parse.quote(search) + '&max=1')
    data_ = await read_speedruncom_api(url)
    if data_ and 'data' in data_ and data_['data']:
        game = data_['data'][0]
        correct = [game['names']['international'].lower(),
                   game['abbreviation'].lower()]
        if search in correct:
            gameSearch[search] = game['id']
            bestSearch[search] = game['id']
            cache['gameSearch', search] = now
            cache['bestSearch', search] = now
            return
    gameSearch[search] = None
    if len(data_['data']):
        game = data_['data'][0]
        bestSearch[search] = game['id']
    else:
        bestSearch[search] = None
    cache['gameSearch', search] = now
    cache['bestSearch', search] = now


async def read_speedrun_game_by_id(gameId: str,
                                   timestamp: Optional[datetime]=None) -> None:
    now: datetime = utils.now() if timestamp is None else timestamp
    cache['gameSearch', gameId] = now
    cache['games', gameId] = now
    url: str = ('http://www.speedrun.com/api/v1/games/' + gameId
                + '?embed=categories,levels.categories,variables')
    data_: Dict[str, Any] = await read_speedruncom_api(url)
    if not data_ or not data_['data']:
        gameSearch[gameId] = None
        cache['gameSearch', gameId] = now
        return None
    game: speedrundata.Game
    if gameId in games:
        game = games[gameId]
    else:
        game = speedrundata.Game(data_['data'])
        games[game.id] = game
    parse_speedruncom_game_category_level(game, data_['data'])
    gameSearch[gameId] = gameId
    cache['gameSearch', gameId] = now
    cache['games', gameId] = now


async def read_leaderboard(id: speedrundata.LeaderboardId,
                           timestamp: Optional[datetime]=None) -> None:
    now: datetime = utils.now() if timestamp is None else timestamp
    cache['leaderboards', id] = now
    url: str
    if id.levelid is None:
        url = ('http://www.speedrun.com/api/v1/leaderboards/' + id.gameid
               + '/category/' + id.categoryid)
    else:
        url = ('http://www.speedrun.com/api/v1/leaderboards/' + id.gameid
               + '/level/' + id.levelid + '/' + id.categoryid)
    url += '?embed=players'
    if id.regionid is not None:
        url += '&region=' + id.regionid
    if id.platformid is not None:
        url += '&platform=' + id.platformid
    variableId: str
    value: str
    for variableId, value in id.variables.items():
        url += '&var-' + variableId + '=' + urllib.parse.quote(value)
    data_: Dict[str, Any] = await read_speedruncom_api(url)
    leaderboard: speedrundata.Leaderboard
    if id in leaderboards:
        leaderboard = leaderboards[id]
        leaderboard.reset()
    else:
        leaderboard = speedrundata.Leaderboard(url, data_['data'])
        leaderboards[id] = leaderboard
    runData: Dict[Any, Any]
    for runData in data_['data']['runs']:
        run: speedrundata.Run
        if runData['run']['id'] in runs:
            run = runs[runData['run']['id']]
            run.update(runData['run'])
        else:
            run = speedrundata.Run(runData['run'])
            runs[run.id] = run
        leaderboard.add_run(runData['place'], run)
    for playerData in data_['data']['players']['data']:
        if playerData['rel'] == 'guest':
            continue
        if playerData['id'] in players:
            player: speedrundata.Player = players[playerData['id']]
            oldTwitch: Optional[str]
            oldTwitch = player.twitch.lower() if player.twitch else None
            player.update(playerData)
            currentTwitch: Optional[str] = None
            if player.twitch is not None:
                currentTwitch = player.twitch.lower()
            if oldTwitch is not None and oldTwitch != currentTwitch:
                del twitchPlayer[oldTwitch]
                del cache['twitchPlayer', oldTwitch]
        else:
            player = speedrundata.Player(playerData)
            players[player.id] = player
        if player.twitch is not None:
            twitch: str = player.twitch.lower()
            playerLookup[twitch] = player.id
            twitchPlayer[twitch] = player.id
            if ('playerLookup', twitch) in cache:
                cache['playerLookup', twitch] = now
        playerLookup[player.name] = player.id
        playerLookup[player.id] = player.id
        cache['players', player.id] = now
        if ('playerLookup', player.name) in cache:
            cache['playerLookup', player.name] = now
        if ('playerLookup', player.id) in cache:
            cache['playerLookup', player.id] = now
    cache['leaderboards', id] = now


async def read_user(identifier: str,
                    timestamp: Optional[datetime]=None) -> None:
    now: datetime = utils.now() if timestamp is None else timestamp
    cache['playerLookup', identifier] = now
    url: str
    data_: Dict[str, Any]
    url = ('http://www.speedrun.com/api/v1/users/'
           + urllib.parse.quote(identifier))
    data_ = await read_speedruncom_api(url)
    if data_ and 'data' in data_ and data_['data']:
        parse_user(data_['data'], identifier, now)
        return
    url = ('http://www.speedrun.com/api/v1/users?twitch='
           + urllib.parse.quote(identifier))
    data_ = await read_speedruncom_api(url)
    if data_ and 'data' in data_ and data_['data']:
        parse_user(data_['data'][0], identifier, now)
        return
    url = ('http://www.speedrun.com/api/v1/users?name='
           + urllib.parse.quote(identifier))
    data_ = await read_speedruncom_api(url)
    if data_ and 'data' in data_ and data_['data']:
        parse_user(data_['data'][0], identifier, now)
        return
    url = ('http://www.speedrun.com/api/v1/users?lookup='
           + urllib.parse.quote(identifier))
    data_ = await read_speedruncom_api(url)
    if data_ and 'data' in data_ and data_['data']:
        parse_user(data_['data'][0], identifier, now)
        return


def parse_speedruncom_game_category_level(game: speedrundata.Game,
                                          data_: Dict[Any, Any],
                                          timestamp: Optional[datetime]=None
                                          ) -> None:
    now: datetime = utils.now() if timestamp is None else timestamp
    game.update(data_)
    categoriesToClear: Set[str]
    categoriesToClear = set(game.game_categories) | set(game.level_categories)
    data_category: Dict[Any, Any]
    for data_category in data_['categories']['data']:
        category: speedrundata.Category
        if data_category['id'] in categories:
            category = categories[data_category['id']]
        else:
            category = speedrundata.Category(game, data_category)
            categories[category.id] = category
        category.update(data_category)
        cache['categories', category.id] = now
        if category.type == 'per-game':
            if category.id in game.level_categories:
                del game.level_categories[category.id]
            game.game_categories[category.id] = category
        else:
            if category.id in game.game_categories:
                del game.game_categories[category.id]
            game.level_categories[category.id] = category
        categoriesToClear.discard(category.id)
    levelsToClear: Set[str]
    levelsToClear = set(k for k in game.levels.keys() if k is not None)
    data_level: Dict[Any, Any]
    for data_level in data_['levels']['data']:
        level: speedrundata.Level
        if data_level['id'] in levels:
            level = levels[data_level['id']]
        else:
            level = speedrundata.Level(game, data_level)
            levels[level.id] = level
        level.update(data_level)
        cache['levels', level.id] = now
        game.levels[level.id] = level
        for data_category in data_level['categories']['data']:
            category = speedrundata.Category(game, data_category)
            level.categories[category.id] = category
        levelsToClear.discard(level.id)
    variablesToClear: Set[str]
    variablesToClear = set(v for v in game.variables.keys() if v is not None)
    data_variable: Dict[Any, Any]
    for data_variable in data_['variables']['data']:
        variable: speedrundata.Variable
        if data_variable['id'] in variables:
            variable = variables[data_variable['id']]
        else:
            variable = speedrundata.Variable(data_variable)
            variables[variable.id] = variable
        variable.update(data_variable)
        cache['variables', variable.id] = now
        game.variables[variable.id] = variable
        variablesToClear.discard(variable.id)
    categoryId: str
    for categoryId in categoriesToClear:
        if categoryId in game.game_categories:
            del game.game_categories[categoryId]
        if categoryId in game.level_categories:
            del game.level_categories[categoryId]
        if categoryId in categories:
            del categories[categoryId]
    levelId: str
    for levelId in levelsToClear:
        if levelId in game.levels:
            del game.levels[levelId]
        if levelId in levels:
            del levels[levelId]
    variableId: str
    for variableId in variablesToClear:
        if variableId in game.variables:
            del game.variables[variableId]
        if variableId in variables:
            del variables[variableId]


def parse_user(data_: Dict[Any, Any],
               identifier: str,
               timestamp: Optional[datetime]=None) -> None:
    now: datetime = utils.now() if timestamp is None else timestamp
    player: speedrundata.Player
    if data_['id'] in players:
        player = players[data_['id']]
        oldTwitch: Optional[str]
        oldTwitch = player.twitch.lower() if player.twitch else None
        player.update(data_)
        if oldTwitch is not None and oldTwitch != player.twitch.lower():
            del twitchPlayer[oldTwitch]
            del cache['twitchPlayer', oldTwitch]
    else:
        player = speedrundata.Player(data_)
        players[player.id] = player
    twitchPlayer[identifier] = player.id
    playerLookup[identifier] = player.id
    playerLookup[player.name] = player.id
    playerLookup[player.id] = player.id
    if player.twitch is not None:
        twitch: str = player.twitch.lower()
        playerLookup[twitch] = player.id
        cache['twitchPlayer', twitch] = now
        cache['playerLookup', twitch] = now
    cache['players', player.id] = now
    cache['playerLookup', identifier] = now
    cache['playerLookup', player.name] = now
    cache['playerLookup', player.id] = now


def botReloadSpeedrun(send: Send) -> None:
    send('Invalidating Speedrun.com cache')

    twitchPlayer.clear()
    playerLookup.clear()
    gameSearch.clear()
    bestSearch.clear()
    games.clear()
    levels.clear()
    categories.clear()
    players.clear()
    runs.clear()
    leaderboards.clear()
    cache.clear()
    levels[None] = None

    send('Done')


def default_categoryid(categories: Dict[str, speedrundata.Category]) -> str:
    for id, category in categories.items():
        if not category.miscellaneous:
            return id
    return next(iter(categories))


def run_players(runId: str, includeTwitchUrl: bool=False) -> str:
    if len(runs[runId].playerids) == 1:
        return player_name(runs[runId].playerids[0], includeTwitchUrl)
    return ' & '.join(map(player_name, runs[runId].playerids))


def player_name(playerid: Union[str, speedrundata.Guest],
                includeTwitchUrl: bool=False) -> str:
    if isinstance(playerid, str):
        if includeTwitchUrl and players[playerid].twitchUrl is not None:
            return players[playerid].name + ' ' + players[playerid].twitchUrl
        return players[playerid].name
    return playerid.name


def format_seconds(seconds: int=0) -> str:
    if seconds < 0:
        return 'Kappa'
    seconds = seconds
    s: str = ''
    if seconds > 3600:
        s += str(seconds // 3600) + 'h '
        seconds %= 3600
    if s or seconds > 60:
        if s:
            s += str(seconds // 60).rjust(2, '0') + 'm '
        else:
            s += str(seconds // 60) + 'm '
        seconds %= 60
    if s or s:
        s += str(seconds).rjust(2, '0') + 's'
    else:
        s += str(seconds) + 's'
    return s


def format_ordinal(number: int) -> str:
    i: int = (number / 10 % 10 != 1) * (number % 10 < 4) * number % 10
    ordinal: str = "tsnrhtdd"[i::4]
    return f'{number}{ordinal}'


def messages_world_records(id: speedrundata.LeaderboardId,
                           runIds: List[str]) -> Generator[str, None, None]:
    game: speedrundata.Game = games[id.gameid]
    level: Optional[speedrundata.Level] = levels[id.levelid]
    category: speedrundata.Category = categories[id.categoryid]
    levelText: str = ''
    if level is not None:
        levelText = f'{level.name} - '
    time: int
    run: speedrundata.Run
    date: str
    if len(runIds) == 0:
        yield f'''\
No Record have been set for \
'{game.internationalName} - {levelText}{category.name}' \
on speedrun.com'''
    elif len(runIds) == 1:
        time = int(runs[runIds[0]].time.total_seconds())
        run = runs[runIds[0]]
        playerNames: str = run_players(runIds[0], True)
        date = ''
        if run.date is not None:
            date = f'on {run.date.strftime(dateFormat)} '
        elif run.submitted is not None:
            date = f'submitted on {run.submitted.strftime(dateFormat)} '
        yield f'''\
The world record for \
'{game.internationalName} - {levelText}{category.name}' \
by {playerNames} with a time of {format_seconds(time)} \
{date}- {run.weblink}'''
    elif len(runIds) < 4:
        time = int(runs[runIds[0]].time.total_seconds())
        yield f'''\
The world record for \
'{game.internationalName} - {levelText}{category.name}' \
has a {len(runIds)} with a time of {format_seconds(time)} '''
        runId: str
        for runId in runIds:
            run = runs[runId]
            players: str = run_players(runId)
            date = ''
            if run.date is not None:
                date = f'on {run.date.strftime(dateFormat)} '
            elif run.submitted is not None:
                date = f'submitted on {run.submitted.strftime(dateFormat)} '
            yield f'By {players} {date}- {run.weblink}'
    else:
        time = int(runs[runIds[0]].time.total_seconds())
        yield f'''\
The world record for \
'{game.internationalName} - {levelText}{category.name}' \
has a {len(runIds)} with a time of {format_seconds(time)}'''
        yield from message.messagesFromItems(map(run_players, runIds),
                                             prepend='By: ')


def messages_world_records_lite(id: speedrundata.LeaderboardId,
                                runIds: List[str]
                                ) -> Generator[str, None, None]:
    game: speedrundata.Game = games[id.gameid]
    level: Optional[speedrundata.Level] = levels[id.levelid]
    category: speedrundata.Category = categories[id.categoryid]
    levelText: str = ''
    if level is not None:
        levelText = f'{level.name} - '
    if len(runIds) == 0:
        yield f'''\
No Record have been set for \
'{game.internationalName} - {levelText}{category.name}' on speedrun.com'''
    else:
        time: int = int(runs[runIds[0]].time.total_seconds())
        playerNames: str = ', '.join(map(run_players, runIds))
        if len(runIds) == 1:
            yield f'''\
{game.internationalName} - {levelText}{category.name} \
WR is {format_seconds(time)} by {playerNames}'''
        else:
            yield f'''\
{game.internationalName} - {levelText}{category.name} \
WR is {format_seconds(time)} by {playerNames} ({len(runIds)}-way tie)'''


def messages_personal_best(id: speedrundata.LeaderboardId,
                           runId: Optional[str],
                           chat: 'data.Channel') -> Generator[str, None, None]:
    game: speedrundata.Game = games[id.gameid]
    level: Optional[speedrundata.Level] = levels[id.levelid]
    category: speedrundata.Category = categories[id.categoryid]
    leaderboard: speedrundata.Leaderboard = leaderboards[id]
    levelText: str = ''
    if level is not None:
        levelText = f'{level.name} - '
    if runId is None or runId not in runs:
        yield f'''\
{chat.channel} has no personal best in \
'{game.internationalName} - {levelText}{category.name}'\
'''
        return
    time: int = int(runs[runId].time.total_seconds())
    run: speedrundata.Run = runs[runId]
    playerNames: str = run_players(runId)
    date: str = ''
    if run.date is not None:
        date = f'on {run.date.strftime(dateFormat)} '
    elif run.submitted is not None:
        date = f'submitted on {run.submitted.strftime(dateFormat)} '
    place: str = format_ordinal(leaderboard.place[runId])
    yield f'''\
The personal best in \
'{game.internationalName} - {levelText}{category.name}' \
by {playerNames} with a time of {format_seconds(time)} in {place} place \
{date} - {run.weblink}'''


def messages_personal_best_lite(id: speedrundata.LeaderboardId,
                                runId: Optional[str],
                                chat: 'data.Channel'
                                ) -> Generator[str, None, None]:
    game: speedrundata.Game = games[id.gameid]
    level: Optional[speedrundata.Level] = levels[id.levelid]
    category: speedrundata.Category = categories[id.categoryid]
    leaderboard: speedrundata.Leaderboard = leaderboards[id]
    levelText: str = ''
    if level is not None:
        levelText = f'{level.name} - '
    if runId is None or runId not in runs:
        yield f'''\
{chat.channel} has no personal best in \
'{game.internationalName} - {levelText}{category.name}'\
'''
        return
    time: int = int(runs[runId].time.total_seconds())
    run: speedrundata.Run = runs[runId]
    playerNames: str
    playerNames = f' by {run_players(runId)}' if len(run.playerids) > 1 else ''
    place: str = format_ordinal(leaderboard.place[runId])
    yield f'''
{game.internationalName} - {levelText}{category.name}'\
 PB is {format_seconds(time)} in {place} place {playerNames}'''


def valid_variables(game: speedrundata.Game,
                    levelId: Optional[str],
                    categoryId: Optional[str]
                    ) -> Generator[speedrundata.Variable, None, None]:
    variable: speedrundata.Variable
    for variable in game.variables.values():
        if variable.scope == 'full-game':
            if levelId is not None:
                continue
        elif variable.scope == 'all-levels':
            if levelId is None:
                continue
        elif variable.scope == 'single-level':
            if levelId != variable.levelId:
                continue
        elif variable.scope != 'global':
            continue
        if (variable.categoryId is not None
                and variable.categoryId != categoryId):
            continue
        yield variable


def default_sub_categories(game: speedrundata.Game,
                           levelId: Optional[str],
                           categoryId: Optional[str]) -> Dict[str, str]:
    variables: Dict[str, str] = {}
    variable: speedrundata.Variable
    for variable in valid_variables(game, levelId, categoryId):
        if not variable.sub_category:
            continue
        variables[variable.id] = variable.default
    return variables
