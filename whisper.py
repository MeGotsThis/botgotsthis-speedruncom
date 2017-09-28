from lib.data import WhisperCommandArgs
from lib.helper.whisper import permission, send

from .library import speedruncom


@permission('manager')
async def commandReloadSpeedrun(args: WhisperCommandArgs) -> bool:
    speedruncom.botReloadSpeedrun(send(args.nick))
    return True
