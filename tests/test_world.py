import asyncio
import inspect
from asyncio import Server
from typing import Optional

import pytest

from sim_world import context
from sim_world.log import logger
from sim_world.testutils import simworld_run
from sim_world.world import Host


class ClientHost(Host):
    def __init__(self, name):
        super().__init__(name)
        self.main_called = False

    async def main(self) -> None:
        self.main_called = True
        while True:
            await asyncio.sleep(1)

    async def connect(self, name, port):
        async def run_connect():
            logger.debug(f"Connecting to {name}:{port} from {context.current_host.get()}")
            reader, writer = await asyncio.open_connection(name, port)
            addr = writer.get_extra_info('peername')
            logger.debug(f'Connected to {addr}')
            return reader, writer

        return await self.run_once(run_connect())

def wrapme(func):
    def wrapper(*args, **kwargs):
        try:
           return func(*args, **kwargs)
        except AssertionError as e:
            # When the test throws assertions, we don't want the wrapper in the
            # Stacktrace because of the way pytest prints out assertion failures
            # in tests. When we get an assertion failure in a test, we just want
            # pytest to print the assertion failure info, not the wrapper code as
            # well
            tb = e.__traceback__
            e.__traceback__ = tb.tb_next
            raise

    wrapper.__signature__ = inspect.signature(func)  # without this, fixtures are not injected

    return wrapper


@wrapme
def test_fail():
    pytest.fail()

@simworld_run
async def test_provision_and_kill():
    client = ClientHost("client")
    await client.start()
    assert client.is_running()
    await asyncio.sleep(1)
    assert client.main_called
    client.shutdown()
    await client.gather()
    assert not client.is_running()


class ServerHost(Host):
    def __init__(self, name):
        super().__init__(name)
        self.port = 5000
        self._server: Optional[Server] = None

    async def handle_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        logger.debug(f'Received connection from {addr}')
        try:
            while True:
                data = await reader.read(100)
                if data:
                    logger.debug(f'Pong: Received {data.decode()}')
                    logger.debug("Pong: Sending pong")
                    writer.write(b'pong')
                    await writer.drain()
        finally:
            logger.debug(f"Closing connection to {addr}")
            writer.close()

    async def main(self) -> None:
        if self._server is not None:
            raise RuntimeError(f"Host {self.name} already running")

        self._server = await asyncio.start_server(self.handle_connection, self.name, self.port,
                                            start_serving=False)

        addr = self._server.sockets[0].getsockname()
        logger.debug(f'Serving on {addr}')

        await self._server.serve_forever()

    def ready(self) -> bool:
        return self._server is not None and self._server.is_serving()

@simworld_run
async def test_connect():
    client_name = "client"
    client = ClientHost(client_name)
    server_name = "server"
    server = ServerHost(server_name)
    await server.start()
    while not server.ready():
        await asyncio.sleep(1)
    await client.connect(server.name, server.port)
    logger.debug(f"Connected to {server_name}")


@simworld_run
async def test_send_receive():
    client_name = "client"
    client = ClientHost(client_name)
    server_name = "server"
    server = ServerHost(server_name)
    await server.start()
    while not server.ready():
        await asyncio.sleep(1)
    reader, writer = await client.connect(server.name, server.port)
    logger.debug(f"Connected to {server_name}")
    writer.write(b'Hello, world')
    await writer.drain()
    data = await reader.read(100)
    logger.debug(f"Received {data!r}")
    assert data == b'pong'

@simworld_run
async def test_partial_read():
    client_name = "client"
    client = ClientHost(client_name)
    server_name = "server"
    server = ServerHost(server_name)
    await server.start()
    while not server.ready():
        await asyncio.sleep(1)
    reader, writer = await client.connect(server.name, server.port)
    logger.debug(f"Connected to {server_name}")
    writer.write(b'Hello, world')
    writer.write(b'Hello, world')
    await writer.drain()
    data = await reader.read(2)
    logger.debug(f"Received {data!r}")
    assert data == b'po'
    data = await reader.readexactly(4)
    logger.debug(f"Received {data!r}")
    assert data == b'ngpo'
