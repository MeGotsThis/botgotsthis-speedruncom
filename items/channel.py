from typing import Iterable, Mapping, Optional

from lib.data import ChatCommand

from .. import channel


def filterMessage() -> Iterable[ChatCommand]:
    return []


def commands() -> Mapping[str, Optional[ChatCommand]]:
    if not hasattr(commands, 'commands'):
        setattr(commands, 'commands', {
            '!wr': channel.commandWR,
            '!pb': channel.commandPB,
            '!wr-full': channel.commandWR,
            '!pb-full': channel.commandPB,
            '!srcuser': channel.commandSpeedrunComUser,
            '!wruser': channel.commandSpeedrunComUser,
            '!pbuser': channel.commandSpeedrunComUser,
            '!srcgame': channel.commandSpeedrunComGame,
            '!wrgame': channel.commandSpeedrunComGame,
            '!pbgame': channel.commandSpeedrunComGame,
            '!srclevel': channel.commandSpeedrunComLevel,
            '!wrlevel': channel.commandSpeedrunComLevel,
            '!pblevel': channel.commandSpeedrunComLevel,
            '!srccategory': channel.commandSpeedrunComCategory,
            '!wrcategory': channel.commandSpeedrunComCategory,
            '!pbcategory': channel.commandSpeedrunComCategory,
            '!srcsubcategory': channel.commandSpeedrunComSubCategory,
            '!wrsubcategory': channel.commandSpeedrunComSubCategory,
            '!pbsubcategory': channel.commandSpeedrunComSubCategory,
            '!srcvariable': channel.commandSpeedrunComVariable,
            '!wrvariable': channel.commandSpeedrunComVariable,
            '!pbvariable': channel.commandSpeedrunComVariable,
            '!srcregion': channel.commandSpeedrunComRegion,
            '!wrregion': channel.commandSpeedrunComRegion,
            '!pbregion': channel.commandSpeedrunComRegion,
            '!srcplatform': channel.commandSpeedrunComPlatform,
            '!wrplatform': channel.commandSpeedrunComPlatform,
            '!pbplatform': channel.commandSpeedrunComPlatform,
            # '!srcemulator': channel.commandSpeedrunComGame,
            # '!wremulator': channel.commandSpeedrunComGame,
            # '!pbemulator': channel.commandSpeedrunComGame,
            '!leaderboard': channel.commandLeaderboard,
            }
        )
    return getattr(commands, 'commands')


def commandsStartWith() -> Mapping[str, Optional[ChatCommand]]:
    return {}


def processNoCommand() -> Iterable[ChatCommand]:
    return []
