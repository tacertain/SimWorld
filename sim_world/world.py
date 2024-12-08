"""
The simulated world.

The world contains the network and the hosts. It is also the place that defines and runs the
event loop for the simulation.


"""
import asyncio
import contextvars
from abc import ABC, abstractmethod
from asyncio import Task
from typing import Type, TypeVar

from sim_world import network
from sim_world import context
from sim_world.network import ConnectedSocket, ServerSocket

_T = TypeVar("_T")


class Host(ABC):
    def __init__(self, name: str):
        self.name = name
        self._main = None
        self._next_port = 40000
        self._sockets: [ConnectedSocket] = []
        self.task_group: asyncio.TaskGroup | None = None
        self._started = asyncio.Event()

    """
    Implement this method with what the host should run. The host will create a
    coroutine context for all coroutines launched
    """
    async def main(self) -> None:
        pass

    def _run_in_host(self, coro, *, name: str) -> Task[_T]:
        token = context.current_host.set(self)
        try:
            return asyncio.create_task(coro, name=f"{self.name}-main")
        finally:
            context.current_host.reset(token)

    async def start(self):
        async def _run_main():
            assert self.task_group is None
            async with asyncio.TaskGroup() as self.task_group:
                self._started.set()
                self.task_group.create_task(self.main())

        self._main = self._run_in_host(_run_main(), name=f"{self.name}-main")
        await self._started.wait()

    async def run_once(self, coro):
        return await self._run_in_host(coro, name=f"{self.name}-run_once")


    def shutdown(self) -> None:
        if self._main is None:
            return
        self._main.cancel()
        for sock in self._sockets:
            sock.close()

    def is_running(self) -> bool:
        if self._main is None:
            return False
        return not self._main.done()

    async def gather(self) -> None:
        try:
            await self._main
        except asyncio.CancelledError:
            pass

    def remove_socket(self, sock: ConnectedSocket) -> None:
        self._sockets.remove(sock)

    def add_socket(self, sock: ConnectedSocket) -> None:
        self._sockets.append(sock)

    async def _connect(self, hostname: str, port: int) -> ConnectedSocket:
        """
        Called from loop.create_connection - subclasses should implement connect()
        to be called from test cases.
        """
        local_port = self._next_port
        self._next_port += 1
        sock = await network.connect((self.name, local_port), (hostname, port))
        self.add_socket(sock)
        sock.set_close_callback(lambda: self.remove_socket(sock))
        return sock

    def bind(self, port) -> ServerSocket:
        sock = network.bind((self.name, port))
        sock.on_accept(self.add_socket)
        return sock


def provision_host(cls: Type[Host], name: str) -> Host:
    host = cls(name)
    return host

