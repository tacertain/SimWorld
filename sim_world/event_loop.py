import asyncio
import socket
import time
from asyncio import AbstractEventLoop, tasks, DefaultEventLoopPolicy, Handle, events, Server, constants
from http.cookiejar import cut_port_re

from sim_world import network, context
from sim_world.log import logger
from sim_world.network import ServerSocket, SimTransport
from sim_world.world import Host


class SimWorldEventLoopPolicy(DefaultEventLoopPolicy):
    def new_event_loop(self) -> AbstractEventLoop:
        loop = SimWorldEventLoop()
        return loop

class SimWorldEventLoop(asyncio.BaseEventLoop):
    class Selector:
        def select(self, timeout: float):
            if network.deliver_one() or timeout is None:
                return .000001
            else:
                return timeout
            pass

        pass

    def __init__(self) -> None:
        super().__init__()
        self._now_s: float = time.time()
        self._tick_s: float = 0.000001  # One microsecond resolution
        self._selector = self.Selector()

    def time(self) -> float:
        now = self._now_s
        self._now_s += self._tick_s
        return now

    def _process_events(self, clock_tick_as_event_list):
        self._now_s += clock_tick_as_event_list

    def _write_to_self(self):
        pass

    async def create_server(
            self, protocol_factory, host=None, port=None,
            *, family=socket.AF_UNSPEC,
            flags=socket.AI_PASSIVE, sock=None, backlog=100,
            ssl=None, reuse_address=None, reuse_port=None,
            ssl_handshake_timeout=None,
            ssl_shutdown_timeout=None,
            start_serving=True) -> Server:

        current_host: Host = context.current_host.get()
        if current_host is None:
            raise RuntimeError("No host found. create_server() must be called inside a Host.")

        if host is not None and not (host == current_host.name or host == 'localhost'):
            raise RuntimeError(f"Host {host} does not match current host {current_host.name}")

        if port is None:
            raise RuntimeError("port must be specified explicitly")

        sock = current_host.bind(port)
        # ServerSockets aren't actually of type socket.socket, but they fulfill what we need for this call
        server = Server(self, [sock], protocol_factory, ssl_context=None, backlog=100,
                        ssl_shutdown_timeout=None, ssl_handshake_timeout=None)
        if start_serving:
            await server.start_serving()

        return server

    async def _accept_connections(self, sock: ServerSocket, protocol_factory, server: Server):
        logger.debug(f"Accepting connections on {sock}")
        while sock.is_open():
            try:
                connected_socket = await sock.accept()
            except IOError:
                break
            protocol = protocol_factory()
            SimTransport(connected_socket, protocol, self, server) # The SimTransport installs a driving coroutine



    def _start_serving(self, protocol_factory, sock,
                       sslcontext=None, server=None, backlog=100,
                       ssl_handshake_timeout=constants.SSL_HANDSHAKE_TIMEOUT,
                       ssl_shutdown_timeout=constants.SSL_SHUTDOWN_TIMEOUT):
        def callback(t):
            if t.exception():
                self.call_exception_handler({
                    'message': 'Error on accept()',
                    'exception': t.exception()
                })
        task = self.create_task(self._accept_connections(sock, protocol_factory, server))
        task.add_done_callback(callback)
        return task

    def _stop_serving(self, sock):
        sock.close()

    async def create_connection(
            self, protocol_factory, host=None, port=None,
            *, ssl=None, family=0,
            proto=0, flags=0, sock=None,
            local_addr=None, server_hostname=None,
            ssl_handshake_timeout=None,
            ssl_shutdown_timeout=None,
            happy_eyeballs_delay=None, interleave=None,
            all_errors=False):

        if host is None or port is None:
            raise RuntimeError("host and port must be specified")

        source_host = context.current_host.get()
        if source_host is None:
            raise RuntimeError("No host found. open_connection() must be called inside a Host.")
        sock = await source_host._connect(host, port)

        protocol = protocol_factory()

        transport = SimTransport(sock, protocol, self)

        if self.get_debug():
            # Get the socket from the transport because SSL transport closes
            # the old socket and creates a new SSL socket
            sock = transport.get_extra_info('socket')
            logger.debug("%r connected to %s:%r: (%r, %r)",
                         sock, host, port, transport, protocol)
        return transport, protocol

