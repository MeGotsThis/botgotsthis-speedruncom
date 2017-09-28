from typing import Mapping, Optional

from lib.data import WhisperCommand

from .. import whisper


def commands() -> Mapping[str, Optional[WhisperCommand]]:
    if not hasattr(commands, 'commands'):
        setattr(commands, 'commands', {
            '!reloadspeedrun': whisper.commandReloadSpeedrun,
        })
    return getattr(commands, 'commands')


def commandsStartWith() -> Mapping[str, Optional[WhisperCommand]]:
    return {}
