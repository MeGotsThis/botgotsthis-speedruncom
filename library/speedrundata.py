from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union  # noqa: F401

baseApi: str = 'http://www.speedrun.com'


class Platform:
    def __init__(self, data: Dict[str, Any]) -> None:
        self.id: str = data['id']
        self.update(data)

    def update(self, data: Dict[str, Any]) -> None:
        if self.id != data['id']:
            raise ValueError()
        self.name: str = data['name']


class Region:
    def __init__(self, data: Dict[str, Any]) -> None:
        self.id: str = data['id']
        self.update(data)

    def update(self, data: Dict[str, Any]) -> None:
        if self.id != data['id']:
            raise ValueError()
        self.name: str = data['name']


class Game:
    def __init__(self, data: Dict[str, Any]) -> None:
        self.game_categories: Dict[str, Category] = OrderedDict()
        self.levels: Dict[Optional[str], Optional[Level]] = {None: None}
        self.level_categories: Dict[str, Category] = OrderedDict()
        self.variables: Dict[str, Variable] = OrderedDict()
        self.regions: List[str] = []
        self.platforms: List[str] = []
        self.id: str = data['id']
        self.update(data)

    def update(self, data: Dict[str, Any]) -> None:
        if self.id != data['id']:
            raise ValueError()
        self.internationalName: str = data['names']['international']
        self.japaneseName: Optional[str] = data['names']['japanese']
        self.twitchName: Optional[str] = data['names']['twitch']
        self.abbreviation: str = data['abbreviation']
        self.weblink: str = data['weblink']
        self.regions.clear()
        self.regions.extend(data['regions'])
        self.platforms.clear()
        self.platforms.extend(data['platforms'])


class Level:
    def __init__(self,
                 game: Game,
                 data: Dict[str, Any]) -> None:
        self.id: str = data['id']
        self.game: Game = game
        self.categories: Dict[str, Category] = OrderedDict()
        self.update(data)

    def update(self, data: Dict[str, Any]) -> None:
        if self.id != data['id']:
            raise ValueError()
        self.name: str = data['name']
        self.weblink: str = data['weblink']


class Category:
    def __init__(self,
                 game: Game,
                 data: Dict[str, Any]) -> None:
        self.id: str = data['id']
        self.game: Game = game
        self.update(data)

    def update(self, data: Dict[str, Any]) -> None:
        if self.id != data['id']:
            raise ValueError()
        self.name: str = data['name']
        self.type: str = data['type']
        self.weblink: str = data['weblink']
        self.miscellaneous: bool = data['miscellaneous']


class Variable:
    def __init__(self, data: Dict[str, Any]) -> None:
        self.id: str = data['id']
        self.update(data)

    def update(self, data: Dict[str, Any]) -> None:
        if self.id != data['id']:
            raise ValueError()
        self.name: str = data['name']
        self.categoryId: Optional[str] = data['category']
        self.levelId: Optional[str] = None
        if 'level' in data['scope']:
            self.levelId = data['scope']['level']
        self.scope: str = data['scope']['type']
        self.required: bool = data['mandatory']
        self.sub_category: bool = data['is-subcategory']
        self.user_defined: bool = data['user-defined']
        self.values: Dict[str, str] = {}
        valueId: str
        valueData: Dict[str, Any]
        for valueId, valueData in data['values']['values'].items():
            self.values[valueId] = valueData['label']
        self.default: str = data['values']['default']


class Runner:
    def __init__(self, *, name: str) -> None:
        self.name: str = name


class Guest(Runner):
    def __init__(self, data: Dict[str, Any]) -> None:
        super().__init__(name=data['name'])


class Player(Runner):
    twitchTvBaseUrl: str = 'https://www.twitch.tv/'

    def __init__(self, data: Dict[str, Any]) -> None:
        super().__init__(name=data['names']['international'])
        self.id: str = data['id']
        self.api: str = ''
        self.update(data)

    def update(self, data: Dict[str, Any]) -> None:
        if self.id != data['id']:
            raise ValueError()
        self.name = data['names']['international']
        self.japaneseName: Optional[str] = data['names']['japanese']
        self.weblink: str = data['weblink']
        self.twitchUrl: Optional[str]
        self.twitchUrl = data['twitch'] and data['twitch']['uri']
        self.twitch: Optional[str] = None
        if self.twitchUrl is not None:
            self.twitch = self.twitchUrl[len(self.twitchTvBaseUrl):]
        link: Dict[str, str]
        for link in data['links']:
            if link['rel'] == 'self':
                self.api = link['uri'][len(baseApi):]


class Run:
    def __init__(self, data: Dict[str, Any]) -> None:
        self.id: str = data['id']
        self.gameid: str = data['game']
        self.levelid: Optional[str] = data['level']
        self.categoryid: str = data['category']
        self.playerids: List[Union[str, Guest]] = []
        self.update(data)

    def update(self, data: Dict[str, Any]) -> None:
        if self.id != data['id']:
            raise ValueError()
        if self.gameid != data['game']:
            raise ValueError()
        if self.levelid != data['level']:
            raise ValueError()
        if self.categoryid != data['category']:
            raise ValueError()
        self.weblink: str = data['weblink']
        self.date: Optional[datetime] = None
        if data['date'] is not None:
            self.date = datetime.strptime(data['date'], '%Y-%m-%d')
        self.submitted: Optional[datetime] = None
        if data['submitted'] is not None:
            self.submitted = datetime.strptime(data['submitted'],
                                               '%Y-%m-%dT%H:%M:%SZ')
        self.time: timedelta = timedelta(seconds=data['times']['primary_t'])
        self.realtime: timedelta
        self.realtime = timedelta(seconds=data['times']['realtime_t'])
        self.realtime_noload: timedelta = timedelta(
            seconds=data['times']['realtime_noloads_t'])
        self.ingametime: timedelta
        self.ingametime = timedelta(seconds=data['times']['ingame_t'])
        self.playerids.clear()
        player: Dict[str, Any]
        for player in data['players']:
            if player['rel'] == 'guest':
                self.playerids.append(Guest(player))
            else:
                self.playerids.append(player['id'])


class LeaderboardId:
    def __init__(self,
                 gameid: str,
                 levelid: Optional[str],
                 categoryid: str,
                 regionid: Optional[str],
                 platformid: Optional[str],
                 variables: Dict[str, str]) -> None:
        self.gameid: str = gameid
        self.levelid: Optional[str] = levelid
        self.categoryid: str = categoryid
        self.regionid: Optional[str] = regionid
        self.platformid: Optional[str] = platformid
        self.variables: Dict[str, str] = variables

    @staticmethod
    def fromObjects(game: Game,
                    level: Optional[Level],
                    category: Category,
                    region: Region,
                    platform: Platform) -> 'LeaderboardId':
        levelId: Optional[str] = level.id if level is not None else None
        return LeaderboardId(game.id, levelId, category.id, region.id,
                             platform.id, {})

    def __hash__(self) -> int:
        return hash((self.gameid, self.levelid, self.categoryid,
                     self.regionid, self.platformid,
                     frozenset(self.variables.items())))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, LeaderboardId):
            return False
        return ((self.gameid, self.levelid, self.categoryid, self.regionid,
                 self.platformid, self.variables)
                == (other.gameid, other.levelid, other.categoryid,
                    other.regionid, other.platformid, other.variables))


class Leaderboard:
    def __init__(self,
                 uri: str,
                 data: Dict[str, Any]) -> None:
        self.gameid: str = data['game']
        self.levelid: Optional[str] = data['level']
        self.categoryid: str = data['category']
        self.weblink: str = data['weblink']
        self.runs: Dict[str, Run] = {}
        self.place: Dict[str, int] = OrderedDict()
        self.runs_by_player: Dict[str, str] = {}
        link: Dict[str, str]
        for link in data['links']:
            if link['rel'] == 'self':
                self.api = link['uri']
        self.api: str = uri

    def add_run(self,
                place: int,
                run: Run) -> None:
        self.runs[run.id] = run
        self.place[run.id] = place
        player: Union[str, Guest]
        for player in run.playerids:
            if isinstance(player, str):
                if player in self.runs_by_player:
                    continue
                self.runs_by_player[player] = run.id

    def reset(self) -> None:
        self.runs.clear()
        self.place.clear()
        self.runs_by_player.clear()
