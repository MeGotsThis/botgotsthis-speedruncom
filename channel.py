from datetime import timedelta
from typing import Dict, List, Optional  # noqa: F401

import aioodbc.cursor  # noqa: F401

from lib.data import ChatCommandArgs
from lib.database import DatabaseMain
from lib.helper.chat import cooldown, feature, permission

from .library import speedruncom, speedrundata


@permission('moderator')
async def commandWRFull(args: ChatCommandArgs) -> bool:
    return await commandWRCommand(args, True)


async def commandWR(args: ChatCommandArgs) -> bool:
    liteFormat: bool = await args.data.hasFeature(args.chat.channel,
                                                  'speedrun.com-lite')
    return await commandWRCommand(args, liteFormat)


@cooldown(timedelta(seconds=60), 'wr', 'owner')
@feature('speedrun.com')
async def commandWRCommand(args: ChatCommandArgs,
                           liteFormat: bool) -> bool:
    db: DatabaseMain
    cursor: aioodbc.cursor.Cursor
    async with DatabaseMain.acquire() as db, await db.cursor() as cursor:
        chanGameId: Optional[str]
        chanGameId = await speedruncom.channel_gameid(cursor, args.chat)

        searched: str = await speedruncom.load_game(
            args.chat, cursor, chanGameId, args.message.query, args.timestamp)

        game: Optional[speedrundata.Game] = None
        if (searched in speedruncom.gameSearch
                and speedruncom.gameSearch[searched] in speedruncom.games):
            game = speedruncom.games[speedruncom.gameSearch[searched]]

        if game is None:
            args.chat.send(f"Cannot find game '{searched}' on speedrun.com")
            return True
        categories: Dict[str, speedrundata.Category] = game.game_categories

        levelId: Optional[str] = await speedruncom.channel_levelid(
            cursor, args.chat, game.id)
        if levelId is not None and levelId in game.levels:
            categories = game.levels[levelId].categories

        categoryId: Optional[str] = await speedruncom.channel_categoryid(
            cursor, args.chat, game.id, levelId)
        if categoryId is None:
            categoryId = speedruncom.default_categoryid(categories)
        if categoryId not in categories:
            args.chat.send(f'''\
Cannot find category for '{game.internationalName}'. Use !wrcategory to \
change categories''')
        variables: Dict[str, str] = speedruncom.default_sub_categories(
            game, levelId, categoryId)
        variables.update(
            await speedruncom.channel_variables(cursor, args.chat, game.id,
                                                levelId, categoryId))

        regionId: Optional[str]
        platformId: Optional[str]
        regionId = await speedruncom.channel_region(cursor, args.chat, game.id)
        platformId = await speedruncom.channel_platform(cursor, args.chat,
                                                        game.id)

        id: speedrundata.LeaderboardId = speedrundata.LeaderboardId(
            game.id, levelId, categoryId, regionId, platformId, variables
            )
        await speedruncom.load_leaderboard(id, args.timestamp)
        leaderboard: speedrundata.Leaderboard = speedruncom.leaderboards[id]
        runIds: List[str] = [i for i, p in leaderboard.place.items() if p == 1]
        if liteFormat:
            args.chat.send(
                speedruncom.messages_world_records_lite(id, runIds))
        else:
            args.chat.send(
                speedruncom.messages_world_records(id, runIds))
    return True


@permission('moderator')
async def commandPBFull(args: ChatCommandArgs) -> bool:
    return await commandPBCommand(args, False)


async def commandPB(args: ChatCommandArgs) -> bool:
    liteFormat: bool = await args.data.hasFeature(args.chat.channel,
                                                  'speedrun.com-lite')
    return await commandPBCommand(args, liteFormat)


@cooldown(timedelta(seconds=60), 'pb', 'owner')
@feature('speedrun.com')
async def commandPBCommand(args: ChatCommandArgs,
                           liteFormat: bool) -> bool:
    db: DatabaseMain
    cursor: aioodbc.cursor.Cursor
    async with DatabaseMain.acquire() as db, await db.cursor() as cursor:
        userId: str = await speedruncom.channel_user(cursor, args.chat)
        await speedruncom.load_user(userId, args.timestamp)
        playerId: str = ''
        if args.chat.channel in speedruncom.playerLookup:
            playerId = speedruncom.playerLookup[userId]
        if playerId not in speedruncom.players:
            args.chat.send(f'Cannot find {args.chat.channel} on speedrun.com')
            return True

        chanGameId: Optional[str] = await speedruncom.channel_gameid(
            cursor, args.chat)

        searched: str = await speedruncom.load_game(
            args.chat, cursor, chanGameId, args.message.query, args.timestamp)

        game: Optional[speedrundata.Game] = None
        if (searched in speedruncom.gameSearch
                and speedruncom.gameSearch[searched] in speedruncom.games):
            game = speedruncom.games[speedruncom.gameSearch[searched]]

        if game is None:
            args.chat.send(f"Cannot find game '{searched}' on speedrun.com")
            return True
        categories: Dict[str, speedrundata.Category] = game.game_categories

        levelId: Optional[str] = await speedruncom.channel_levelid(
            cursor, args.chat, game.id)
        if levelId is not None and levelId in game.levels:
            categories = game.levels[levelId].categories

        categoryId: Optional[str] = await speedruncom.channel_categoryid(
            cursor, args.chat, game.id, levelId)
        if categoryId is None:
            categoryId = speedruncom.default_categoryid(categories)
        if categoryId not in categories:
            args.chat.send(f'''\
Cannot find category for '{game.internationalName}'. Use !wrcategory to \
change categories''')
        variables: Dict[str, str] = speedruncom.default_sub_categories(
            game, levelId, categoryId)
        variables.update(
            await speedruncom.channel_variables(cursor, args.chat, game.id,
                                                levelId, categoryId))

        regionId: Optional[str]
        platformId: Optional[str]
        regionId = await speedruncom.channel_region(cursor, args.chat, game.id)
        platformId = await speedruncom.channel_platform(cursor, args.chat,
                                                        game.id)

        id: speedrundata.LeaderboardId = speedrundata.LeaderboardId(
            game.id, levelId, categoryId, regionId, platformId, variables)
        await speedruncom.load_leaderboard(id, args.timestamp)
        leaderboard: speedrundata.Leaderboard = speedruncom.leaderboards[id]
        runId: Optional[str] = None
        if playerId in leaderboard.runs_by_player:
            runId = leaderboard.runs_by_player[playerId]
        if liteFormat:
            args.chat.send(
                speedruncom.messages_personal_best_lite(id, runId, args.chat))
        else:
            args.chat.send(
                speedruncom.messages_personal_best(id, runId, args.chat))
    return True


@feature('speedrun.com')
@permission('broadcaster')
async def commandSpeedrunComUser(args: ChatCommandArgs) -> bool:
    db: DatabaseMain
    cursor: aioodbc.cursor.Cursor
    if len(args.message) < 2:
        async with DatabaseMain.acquire() as db:
            await speedruncom.clear_user(db, args.chat.channel)
        args.chat.send(
            f'Set the speedrun.com user for {args.chat.channel} to default')
    else:
        identifier: str = args.message.lower[1:]
        await speedruncom.load_user(identifier, args.timestamp)

        if speedruncom.playerLookup[identifier] is None:
            args.chat.send(
                f"Cannot find '{args.message.query}' on speedrun.com")
        else:
            async with DatabaseMain.acquire() as db:
                await speedruncom.set_user(db, args.chat.channel, identifier)
            args.chat.send(f'''\
Set the speedrun.user for {args.chat.channel} to using {args.message.query}''')
    return True


@feature('speedrun.com')
@permission('moderator')
async def commandSpeedrunComGame(args: ChatCommandArgs) -> bool:
    db: DatabaseMain
    cursor: aioodbc.cursor.Cursor
    if len(args.message) < 2:
        async with DatabaseMain.acquire() as db:
            await speedruncom.clear_game(db, args.chat.channel)
        args.chat.send(f'''\
Set the game for !wr and !pb to {args.chat.channel} Twitch game \
(Currently: {args.chat.twitchGame})''')
        return True

    search = args.message.lower[1:]
    async with DatabaseMain.acquire() as db, await db.cursor() as cursor:
        await speedruncom.load_game(args.chat, cursor, None, search,
                                    args.timestamp)

        if speedruncom.gameSearch[search] is None:
            args.chat.send(f'''\
Cannot find '{args.message.query}' on speedrun.com''')
            return True
        game: speedrundata.Game
        game = speedruncom.games[speedruncom.gameSearch[search]]
        await speedruncom.set_game(db, args.chat.channel, game.id)
    msg: str
    if (game.internationalName == search
            or game.abbreviation.lower() == search):
        msg = f'''\
Set the game for !wr and !pb to {game.internationalName}'''
    else:
        msg = f'''\
Could not find '{args.message.query}' on speedrun.com. Set the game using \
best guess for !wr and !pb to {game.internationalName}'''
    args.chat.send(msg)
    return True


@feature('speedrun.com')
@permission('moderator')
async def commandSpeedrunComLevel(args: ChatCommandArgs) -> bool:
    db: DatabaseMain
    cursor: aioodbc.cursor.Cursor
    async with DatabaseMain.acquire() as db, await db.cursor() as cursor:
        chanGameId: Optional[str]
        chanGameId = await speedruncom.channel_gameid(cursor, args.chat)

        searched: str = await speedruncom.load_game(
            args.chat, cursor, chanGameId, None, args.timestamp)

    game: Optional[speedrundata.Game] = None
    if (searched in speedruncom.gameSearch
            and speedruncom.gameSearch[searched] in speedruncom.games):
        game = speedruncom.games[speedruncom.gameSearch[searched]]

    if game is None:
        args.chat.send(f"Cannot find game '{searched}' on speedrun.com")
        return True

    if len(args.message) < 2:
        await speedruncom.clear_level(db, args.chat.channel, game.id)
        args.chat.send(f'''\
Set to full-game for !wr and !pb in the game '{game.internationalName}'\
''')
    else:
        if not game.levels:
            args.chat.send(f'''\
'{game.internationalName}' does not have individual levels on speedrun.com''')
            return True

        levelId: Optional[str] = None
        levelSearch = args.message.lower[1:]
        l: Optional[speedrundata.Level]
        for l in game.levels.values():
            if l is None:
                continue
            if l.name.lower() == levelSearch or l.id == levelSearch:
                levelId = l.id
                break

        if levelId is None:
            args.chat.send(f'''\
Cannot find individual level \
'{game.internationalName} - {args.message.query}' on speedrun.com''')
        else:
            await speedruncom.set_level(db, args.chat.channel, game.id,
                                        levelId)
            args.chat.send(f'''\
Set the level to '{game.levels[levelId].name}' for !wr and !pb for the game \
'{game.internationalName}'\
''')
    return True


@feature('speedrun.com')
@permission('moderator')
async def commandSpeedrunComCategory(args: ChatCommandArgs) -> bool:
    db: DatabaseMain
    cursor: aioodbc.cursor.Cursor
    async with DatabaseMain.acquire() as db, await db.cursor() as cursor:
        chanGameId: Optional[str]
        chanGameId = await speedruncom.channel_gameid(cursor, args.chat)

        searched: str = await speedruncom.load_game(
            args.chat, cursor, chanGameId, None, args.timestamp)

        game: Optional[speedrundata.Game] = None
        if (searched in speedruncom.gameSearch
                and speedruncom.gameSearch[searched] in speedruncom.games):
            game = speedruncom.games[speedruncom.gameSearch[searched]]

        if game is None:
            args.chat.send(f"Cannot find game '{searched}' on speedrun.com")
            return True

        categories: Dict[str, speedrundata.Category] = game.game_categories

        levelText: str = ''
        levelId: Optional[str] = await speedruncom.channel_levelid(
            cursor, args.chat, game.id)
        if levelId is not None and levelId in game.levels:
            levelText = f' - {game.levels[levelId].name}'
            categories = game.levels[levelId].categories

    if len(args.message) < 2:
        await speedruncom.clear_category(db, args.chat.channel, game.id,
                                         levelId)
        categoryId: Optional[str] = speedruncom.default_categoryid(categories)
        if categoryId is None:
            args.chat.send(f'''\
Set category to default 'Unknown' for !wr and !pb in \
'{game.internationalName}{levelText}'\
''')
        else:
            category: speedrundata.Category = categories[categoryId]
            args.chat.send(f'''\
Set category to default '{category.name}' for !wr and !pb in \
'{game.internationalName}{levelText}'\
''')
    else:
        searchCategoryId: Optional[str] = None
        categorySearch: str = args.message.lower[1:]
        c: speedrundata.Category
        for c in categories.values():
            if c.name.lower() == categorySearch or c.id == categorySearch:
                searchCategoryId = c.id
                break

        if searchCategoryId is None:
            categorySearch = args.message.query
            args.chat.send(f'''\
Cannot find category \
'{game.internationalName}{levelText} - {categorySearch}' on speedrun.com''')
        else:
            await speedruncom.set_category(db, args.chat.channel, game.id,
                                           levelId, searchCategoryId)
            name: str = categories[searchCategoryId].name
            args.chat.send(f'''\
Set the category to '{name}' for !wr and !pb for the game \
'{game.internationalName}{levelText}'\
''')
    return True


@feature('speedrun.com')
@permission('moderator')
async def commandSpeedrunComSubCategory(args: ChatCommandArgs) -> bool:
    db: DatabaseMain
    cursor: aioodbc.cursor.Cursor
    async with DatabaseMain.acquire() as db, await db.cursor() as cursor:
        chanGameId: Optional[str]
        chanGameId = await speedruncom.channel_gameid(cursor, args.chat)

        searched: str = await speedruncom.load_game(
            args.chat, cursor, chanGameId, None, args.timestamp)

        game: Optional[speedrundata.Game] = None
        if (searched in speedruncom.gameSearch
                and speedruncom.gameSearch[searched] in speedruncom.games):
            game = speedruncom.games[speedruncom.gameSearch[searched]]

        if game is None:
            args.chat.send(f"Cannot find game '{searched}' on speedrun.com")
            return True

        categories: Dict[str, speedrundata.Category] = game.game_categories

        levelText: str = ''
        levelId: Optional[str] = await speedruncom.channel_levelid(
            cursor, args.chat, game.id)
        if levelId is not None and levelId in game.levels:
            levelText = f' - {game.levels[levelId].name}'
            categories = game.levels[levelId].categories

        categoryId: Optional[str] = await speedruncom.channel_categoryid(
            cursor, args.chat, game.id, levelId)
        if categoryId is None:
            categoryId = speedruncom.default_categoryid(categories)
        if categoryId not in categories:
            args.chat.send(f'''\
Cannot find category for '{game.internationalName}'. Use !wrcategory to \
change categories''')
        category: speedrundata.Category = categories[categoryId]

        variables: Dict[str, str] = speedruncom.default_sub_categories(
            game, levelId, categoryId)

    variable: speedrundata.Variable
    variableId: str
    if len(args.message) < 2:
        values = []
        for variableId in variables:
            await speedruncom.clear_variable(
                db, args.chat.channel, game.id, levelId, categoryId,
                variableId)
            variable = speedruncom.variables[variableId]
            values.append(variable.values[variable.default])
        default_subcategories: str = ', '.join(values)
        args.chat.send(f'''\
Set subcategories to default '{default_subcategories}' for !wr and !pb in \
'{game.internationalName}{levelText} - {category.name}'\
''')
    else:
        searchVariableId: Optional[str] = None
        searchValue: Optional[str] = None
        valueSearch: str = args.message.lower[1:]
        for variableId in variables:
            variable = speedruncom.variables[variableId]
            valueId: str
            value: str
            for valueId, value in variable.values.items():
                if value.lower() == valueSearch or valueId == valueSearch:
                    searchVariableId = variableId
                    searchValue = valueId
                    break
            if searchVariableId is not None:
                break

        if searchVariableId is None:
            args.chat.send(f'''\
Cannot find variable value matching '{args.message.query}' for \
'{game.internationalName}{levelText} - {category.name}' on speedrun.com''')
        else:
            variable = speedruncom.variables[searchVariableId]
            await speedruncom.set_variable(db, args.chat.channel, game.id,
                                           levelId, categoryId,
                                           searchVariableId, searchValue)
            args.chat.send(f'''\
Set the variable '{variable.name}' to '{args.message.query}' \
for !wr and !pb for \
'{game.internationalName}{levelText} - {category.name}'\
''')
    return True


@feature('speedrun.com')
@permission('moderator')
async def commandSpeedrunComVariable(args: ChatCommandArgs) -> bool:
    db: DatabaseMain
    cursor: aioodbc.cursor.Cursor
    async with DatabaseMain.acquire() as db, await db.cursor() as cursor:
        chanGameId: Optional[str]
        chanGameId = await speedruncom.channel_gameid(cursor, args.chat)

        searched: str = await speedruncom.load_game(
            args.chat, cursor, chanGameId, None, args.timestamp)

        game: Optional[speedrundata.Game] = None
        if (searched in speedruncom.gameSearch
                and speedruncom.gameSearch[searched] in speedruncom.games):
            game = speedruncom.games[speedruncom.gameSearch[searched]]

        if game is None:
            args.chat.send(f"Cannot find game '{searched}' on speedrun.com")
            return True

        categories: Dict[str, speedrundata.Category] = game.game_categories

        levelText: str = ''
        levelId: Optional[str] = await speedruncom.channel_levelid(
            cursor, args.chat, game.id)
        if levelId is not None and levelId in game.levels:
            levelText = ' - ' + game.levels[levelId].name
            categories = game.levels[levelId].categories

        categoryId: Optional[str] = await speedruncom.channel_categoryid(
            cursor, args.chat, game.id, levelId)
        if categoryId is None:
            categoryId = speedruncom.default_categoryid(categories)
        if categoryId not in categories:
            args.chat.send(f'''\
Cannot find category for '{game.internationalName}'. Use !wrcategory to \
change categories''')
        category: speedrundata.Category = categories[categoryId]

        variables: List[speedrundata.Variable]
        variables = [v for v
                     in speedruncom.valid_variables(game, levelId, categoryId)
                     if not v.sub_category]

    variable: speedrundata.Variable
    if len(args.message) < 2:
        for variable in variables:
            await speedruncom.clear_variable(
                db, args.chat.channel, game.id, levelId, categoryId,
                variable.id)
        args.chat.send(f'''\
Reverted all variables to any value for !wr and !pb in \
'{game.internationalName}{levelText} - {category.name}'\
''')
    else:
        searchVariableId: Optional[str] = None
        searchValue: Optional[str] = None
        valueSearch = args.message.lower[1:]
        for variable in variables:
            valueId: str
            value: str
            for valueId, value in variable.values.items():
                if value.lower() == valueSearch or valueId == valueSearch:
                    searchVariableId = variable.id
                    searchValue = valueId
                    break
            if searchVariableId is not None:
                break

        if searchVariableId is None:
            args.chat.send(f'''\
Cannot find variable value matching '{args.message.query}' for \
'{game.internationalName}{levelText} - {category.name}'\
 on speedrun.com''')
        else:
            variable = speedruncom.variables[searchVariableId]
            await speedruncom.set_variable(db, args.chat.channel, game.id,
                                           levelId, categoryId,
                                           searchVariableId, searchValue)
            args.chat.send(f'''\
Set the variable '{variable.name}' to '{args.message.query}' \
for !wr and !pb for \
'{game.internationalName}{levelText} - {category.name}'\
''')
    return True


@feature('speedrun.com')
@permission('moderator')
async def commandSpeedrunComRegion(args: ChatCommandArgs) -> bool:
    db: DatabaseMain
    cursor: aioodbc.cursor.Cursor
    async with DatabaseMain.acquire() as db, await db.cursor() as cursor:
        chanGameId: Optional[str]
        chanGameId = await speedruncom.channel_gameid(cursor, args.chat)

        searched: str = await speedruncom.load_game(
            args.chat, cursor, chanGameId, None, args.timestamp)

    game: Optional[speedrundata.Game] = None
    if (searched in speedruncom.gameSearch
            and speedruncom.gameSearch[searched] in speedruncom.games):
        game = speedruncom.games[speedruncom.gameSearch[searched]]

    if game is None:
        args.chat.send(f"Cannot find game '{searched}' on speedrun.com")
        return True

    if len(args.message) < 2:
        speedruncom.clear_region(db, args.chat.channel, game.id)
        args.chat.send(f'''\
Set to any region for !wr and !pb in the game '{game.internationalName}'\
''')
    else:
        regionId: Optional[str] = None
        regionSearch: str = args.message.lower[1:]
        r: str
        for r in game.regions:
            if (regionSearch in speedruncom.regions[r].name.lower()
                    or r == regionSearch):
                regionId = r
                break

        if regionId is None:
            args.chat.send(f'''\
Cannot find individual region '{args.message.query}' for '{game}' on \
speedrun.com''')
        else:
            await speedruncom.set_region(db, args.chat.channel,
                                         game.id, regionId)
            args.chat.send(f'''\
Set the region to '{speedruncom.regions[regionId].name}' for !wr and !pb for \
the game '{game.internationalName}'\
''')
    return True


@feature('speedrun.com')
@permission('moderator')
async def commandSpeedrunComPlatform(args: ChatCommandArgs) -> bool:
    db: DatabaseMain
    cursor: aioodbc.cursor.Cursor
    async with DatabaseMain.acquire() as db, await db.cursor() as cursor:
        chanGameId: Optional[str]
        chanGameId = await speedruncom.channel_gameid(cursor, args.chat)

        searched: str = await speedruncom.load_game(
            args.chat, cursor, chanGameId, None, args.timestamp)

    game: Optional[speedrundata.Game] = None
    if (searched in speedruncom.gameSearch
            and speedruncom.gameSearch[searched] in speedruncom.games):
        game = speedruncom.games[speedruncom.gameSearch[searched]]

    if game is None:
        args.chat.send(f"Cannot find game '{searched}' on speedrun.com")
        return True

    if len(args.message) < 2:
        await speedruncom.clear_platform(db, args.chat.channel, game.id)
        args.chat.send(f'''\
Set to any platform for !wr and !pb in the game '{game.internationalName}'\
''')
    else:
        platformId: Optional[str] = None
        platformSearch: str = args.message.lower[1:]
        p: str
        for p in game.platforms:
            if (platformSearch in speedruncom.platforms[p].name.lower()
                    or p == platformSearch):
                platformId = p
                break

        if platformId is None:
            args.chat.send(f'''\
Cannot find individual platform '{args.message.query}' for '{game}' on \
speedrun.com''')
        else:
            await speedruncom.set_platform(db, args.chat.channel, game.id,
                                           platformId)
            args.chat.send(f'''\
Set the platform to '{speedruncom.platforms[platformId].name}' \
for !wr and !pb for the game '{game.internationalName}'\
''')
    return True


@feature('speedrun.com')
@cooldown(timedelta(seconds=60), 'leaderboard', 'owner')
async def commandLeaderboard(args: ChatCommandArgs) -> bool:
    db: DatabaseMain
    cursor: aioodbc.cursor.Cursor
    async with DatabaseMain.acquire() as db, await db.cursor() as cursor:
        chanGameId: Optional[str]
        chanGameId = await speedruncom.channel_gameid(cursor, args.chat)

        searched: str = await speedruncom.load_game(
            args.chat, cursor, chanGameId, args.message.query, args.timestamp)

        game: Optional[speedrundata.Game] = None
        if (searched in speedruncom.gameSearch
                and speedruncom.gameSearch[searched] in speedruncom.games):
            game = speedruncom.games[speedruncom.gameSearch[searched]]

        if game is None:
            args.chat.send(f"Cannot find game '{searched}' on speedrun.com")
            return True
        categories: Dict[str, speedrundata.Category] = game.game_categories

        level: speedrundata.Level = None
        levelId: Optional[str] = await speedruncom.channel_levelid(
            cursor, args.chat, game.id)
        if levelId is not None and levelId in game.levels:
            level = game.levels[levelId]
            categories = level.categories

        categoryId: Optional[str] = await speedruncom.channel_categoryid(
            cursor, args.chat, game.id, levelId)
        if categoryId is None or categoryId not in categories:
            if level is None:
                args.chat.send(
                    f'{game.internationalName} Leaderboard: {game.weblink}')
            else:
                args.chat.send(f'''\
{game.internationalName} - {level.name} Leaderboard: {level.weblink}''')
            return True
        category: speedrundata.Category = categories[categoryId]

        levelText: str = f'{level.name} - ' if level is not None else ''
        args.chat.send(f'''\
{game.internationalName} - {levelText}{category.name} Leaderboard: \
{category.weblink}''')
    return True
