import asyncio

import pytest


@pytest.fixture
def event_loop():
    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    yield loop
    loop.close()