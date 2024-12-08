import asyncio
import inspect
import logging

from sim_world.event_loop import SimWorldEventLoopPolicy
from sim_world.log import logger


def simworld_run(async_func):
    exc = None
    def exception_handler(loop, context):
        loop.default_exception_handler(context)
        nonlocal exc
        exc = context['exception']
        loop.stop()

    def wrapper(*args, **kwargs):
        asyncio.set_event_loop_policy(SimWorldEventLoopPolicy())
        asyncio.get_event_loop().set_debug(True)
        logging.basicConfig(level=logging.DEBUG)
        logging.captureWarnings(True)
        logger.warning("Starting test")
        logger.debug(f"Running {async_func.__name__}")
        loop = asyncio.get_event_loop()
        loop.set_exception_handler(exception_handler)
        task = loop.create_task(async_func(*args, **kwargs), name=async_func.__name__)
        nonlocal exc
        try:
            loop.run_until_complete(task)
            ret = task.result()
            logger.debug(f"Done with {async_func.__name__} return value: {ret}")
        except AssertionError as e:
            # When the test throws assertions, we don't want the wrapper in the
            # Stacktrace because of the way pytest prints out assertion failures
            # in tests. When we get an assertion failure in a test, we just want
            # pytest to print the assertion failure info, not the wrapper code as
            # well
            tb = e.__traceback__
            e.__traceback__ = tb.tb_next
            raise
        except Exception as e:
            if exc is None:
                exc = e
        if exc is None:
            return ret
        else:
            raise exc

    wrapper.__signature__ = inspect.signature(async_func)  # without this, fixtures are not injected

    return wrapper
