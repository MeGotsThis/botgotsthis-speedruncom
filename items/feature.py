from typing import Mapping, Optional


def features() -> Mapping[str, Optional[str]]:
    if not hasattr(features, 'features'):
        setattr(features, 'features', {
            'speedrun.com': '!wr and !pb From Speedrun.com',
            'speedrun.com-lite': '!wr and !pb From Speedrun.com (Light text)',
            })
    return getattr(features, 'features')
