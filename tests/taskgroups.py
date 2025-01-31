import asyncio
from asyncio.log import logger
from collections import deque

import pytest


class Host:
    def __init__(self):
        self.started = asyncio.Event()
        self.closing = asyncio.Event()
        self.done = asyncio.Event()
        self.tg = None
        self._main = None
        self._context = None

    async def wait_done(self):
        await self.done.wait()

    async def wait_closing(self):
        await self.closing.wait()

    def start(self):
        self._main = asyncio.create_task(self._run_main())

    async def _run_main(self):
        self._context = asyncio.current_task().get_context()
        try:
            async with asyncio.TaskGroup() as self.tg:
                self.started.set()
                self.run_in_host(self.wait_closing(), name='Waiter')
                logger.debug("Host about to leave start")
        except BaseException:
            raise
        finally:
            self.done.set()

    def close(self):
        self.closing.set()

    async def _run_wrapper(self, coro, name):
        task = self.tg.create_task(coro, name=name)
        try:
            await task
        except Exception as e:
            logger.error(f"Task {name} failed with exception {task.exception()}")
        finally:
            logger.debug(f'Done with {name} - exception {task.exception()}')

    def run_in_host(self, coro, name = 'host-task'):
        self.tg.create_task(self._run_wrapper(coro, name=name), name=f'{name}-wrapper')

    async def sub_group(self):
        await self.started.wait()
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(sleep(2), name='Inner Sleeper')
                tg.create_task(hello('Inner'), name='Inner Greeter')
                #tg.create_task(throw(), name='Inner Thrower')
        except Exception as e:
            logger.error(f"Sub group failed with exception {e}")
            raise
        finally:
            logger.debug("Leaving Inner")

    def launch_sub(self):
        self.run_in_host(self.sub_group(), "sub-task")

async def sleep(how_long: float = 1):
    await asyncio.sleep(how_long)
async def hello(who: str = 'Base'):
    await asyncio.sleep(.1)
    logger.debug(f"Hello world from {who}")
async def throw():
    raise RuntimeError("Goodbye")


@pytest.mark.asyncio
async def test_four():
    host = Host()
    host.start()
    await host.started.wait()
    logger.debug("About to launch host subtasks")
    host.run_in_host(sleep(), name='Sleeper')
    host.run_in_host(hello(), name='Greeter')
    logger.debug("Host subtasks launched")
    host.run_in_host(throw(), name='Thrower')
    host.launch_sub()
    host.close()
    await host.wait_done()
    logger.debug('Host done')
    try:
        await host.wait_done()
    except* Exception as e:
        raise AssertionError(f'Caught ${e}')


@pytest.mark.asyncio
async def test_three():
    host = Host()
    task = asyncio.create_task(host.start())
    await host.started.wait()
    host.tg.create_task(sleep(), name='Sleeper')
    host.tg.create_task(hello(), name='Greeter')
    #host.tg.create_task(throw(), name='Thrower')
    host.close()
    await host.wait_done()
    logger.debug('Host done')
    await task

@pytest.mark.asyncio
async def test_two():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(sleep(), name='Sleeper')
        tg.create_task(hello(), name='Greeter')
        tg.create_task(throw(), name='Thrower')
    logger.debug("Done")


@pytest.mark.asyncio
async def test_one():
    sleeper = asyncio.create_task(sleep(), name='Sleeper')
    asyncio.create_task(hello(), name='Greeter')
    thrower = asyncio.create_task(throw(), name='Thrower')
    await sleeper
    await thrower
    logger.debug("Done")

async def wrapper(tg, coro, name):
    if tg is None:
        task = asyncio.create_task(coro, name=f"Wrapped {name}")
    else:
        task = tg.create_task(coro, name=f"Wrapped {name}")
    try:
        await task
    except Exception as e:
        logger.error(f"{name}: Caught error {e} - ignoring")
    except asyncio.CancelledError:
        logger.error(f"{name} was canceled")
    finally:
        logger.debug(f'Done with {name} - exception: {task.exception()}')

async def direct(coro, name):
    try:
        await coro
    except Exception as e:
        logger.error(f"{name}: Caught error {e} - ignoring")
    except asyncio.CancelledError:
        logger.error(f"{name} was canceled")
    finally:
        logger.debug(f'Done with {name}')

@pytest.mark.asyncio
async def test_wrapped():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(direct(sleep(), name='Sleeper'))
        tg.create_task(direct(hello(), name='Greeter'))
        tg.create_task(wrapper(tg, throw(), name="Thrower"))
        await sleep(1)
    logger.debug("Done")

@pytest.mark.asyncio
async def test_direct():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(direct(sleep(), name='Sleeper'))
        tg.create_task(direct(hello(), name='Greeter'))
        tg.create_task(direct(throw(), name="Thrower"))
    logger.debug("Done")


@pytest.mark.asyncio
async def test_notg():
    tasks = []
    tasks.append(asyncio.create_task(direct(sleep(), name='Sleeper')))
    tasks.append(asyncio.create_task(direct(hello(), name='Greeter')))
    tasks.append(asyncio.create_task(wrapper(None, throw(), name='Thrower')))
    await sleep(3)
    while True:
        wait_for = []
        for task in tasks:
            if not task.done():
                wait_for.append(task)
        if len(wait_for) == 0:
            break
        try:
            await asyncio.gather(*wait_for)
        except Exception as e:
            logger.error(f'Caught {e} - Cancelling')
            for task in tasks:
                task.cancel()

@pytest.mark.asyncio
async def test_notasks():
    tasks = []
    tasks.append(asyncio.Task(sleep(2)))
    tasks.append(asyncio.Task(hello()))
    tasks.append(asyncio.Task(throw()))
    await asyncio.wait(tasks)


@pytest.mark.asyncio
async def test_simple_tg():
    async with asyncio.TaskGroup() as tg:
        try:
            await tg.create_task(throw())
        except* BaseException as e:
            logger.warning(f'Caught BaseException {e}')
        except* RuntimeError as e:
            logger.warning(f'Caught {e}')

def boo(task):
    logger.warning('Boo')

@pytest.mark.asyncio
async def test_simple():
    try:
        task = asyncio.create_task(throw())
        task.add_done_callback(boo)
        await task
    except RuntimeError as e:
        logger.warning(f'Caught {e}')

