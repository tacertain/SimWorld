"""
The core network module.

This module is responsible for running the simulated network. Using it, clients
and servers can open simulated sockets and pass simulated network traffic.

Unlike normal sockets, you don't create one and then either call `bind()` or `connect()` on the
socket. Instead you just call bind/connect directly on the network and get back either a
`ServerSocket` (which can be listened on for connections) or a `ConnectedSocket` on which you can
send/recv.

The network supplies DNS in the sense that you open network connections using strings directly.
The host-port combination is just looked up directly to form the connection.

So on the server you can write:
    `sock = network.bind(("my-server-host", 80))`
    `sock.listen()`

And on the client write:
    `sock = network.connect(("my-server-host", 80))`

You don't need to resolve addresses, etc. The network also doesn't know anything
about address families, etc. It is all just simulated TCP.

The network operates logically like TCP in that once a socket connection is established,
all data sent from one side to the other will appear in order with no gaps.

TODO:
The network consists of two areas, the "delivery" area and the "hold" area. Normally,
when an object is sent into the network, it's placed in the delivery area. When the
network event loop runs, an object in the delivery area is selected and delivered to
the appropriate `(host, port)` combination (respecting delivery order, of course).

Packets can be moved into the "hold" area with filters. When a packet is in the hold
area, it cannot be delivered. This mechanism is useful for simulating network failures
and delays.
"""
import asyncio
from abc import ABC, abstractmethod
from asyncio import Queue, Transport, Protocol, BaseEventLoop
from dataclasses import dataclass
from typing import Dict, Tuple, Any, Optional, Callable

from sim_world import context
from sim_world.log import logger

Address = Tuple[str, int]


class Socket(ABC):

    @abstractmethod
    def getsockname(self) -> Address:
        raise Exception("Not implemented")


RESET_TYPE = "RST"
SYN_TYPE = "SYN"
ACK_TYPE = "ACK"

@dataclass
class Packet:
    src: Address
    dst: Address


@dataclass
class DataPacket(Packet):
    data: bytes
    is_eof: bool = False


@dataclass
class ControlPacket(Packet):
    pass

@dataclass
class ResetPacket(ControlPacket):
    pass

@dataclass
class SynPacket(ControlPacket):
    connection_no: int
    peer_socket: 'ConnectedSocket'

@dataclass
class AckPacket(ControlPacket):
    peer_socket: 'ConnectedSocket'

class ConnectedSocket(Socket):
    """
    A connected socket.

    Every connection in the network is represented by two `ConnectedSocket`s, one for each host. 
    The `ConnectedSocket` has a queue for data ready for the reader to consume.
    """

    def __init__(self, src: Address, dst: Address, connection_no: int):
        self._connected = False
        self._local_addr = src
        self._remote_addr = dst
        self._connection_no = connection_no
        self._send_queue = asyncio.Queue()
        self._recv_queue = Queue()
        self.eof = False
        self._close_callback: Callable[[], None] | None = None
        host = context.current_host.get()
        assert host is not None
        tg = host.task_group
        if tg is None:
            self._close_task = asyncio.create_task(self.wait_closed(), name=f"{self._local_addr}-{self._remote_addr}-close")
        else:
            self._close_task = tg.create_task(self.wait_closed(), name=f"{self._local_addr}-{self._remote_addr}-close")
        self._close_task.add_done_callback(lambda _: self._on_close())
        self._closing: asyncio.Event = asyncio.Event()

    def getsockname(self) -> Address:
        return self._local_addr

    def getpeername(self) -> Address:
        return self._remote_addr

    def family(self):
        return None

    async def _try_recv(self) -> Packet:
        try:
            packet = await self._recv_queue.get()
            self._recv_queue.task_done()
            return packet
        except (asyncio.QueueShutDown, asyncio.CancelledError):
            raise InterruptedError

    async def _try_send(self, packet: Packet) -> None:
        try:
            await self._send_queue.put(packet)
        except asyncio.QueueShutDown:
            raise OSError("Socket closed")

    async def connect(self) -> None:
        assert not self._connected
        await self._try_send(SynPacket(self._local_addr, self._remote_addr, self._connection_no, self))
        response = await self._try_recv()
        assert isinstance(response, ControlPacket)
        if isinstance(response, ResetPacket):
            self.reset()
            raise ConnectionResetError
        assert isinstance(response, AckPacket)
        register_socket(self, response.peer_socket)
        self._connected = True

    async def send(self, data: bytes) -> None:
        """
        Called from the Transport to enqueue data for sending to the network
        """
        await self._try_send(DataPacket(self._local_addr, self._remote_addr, data))

    async def recv(self) -> DataPacket:
        """
        Called from the event loop to deliver data to the associated Protocol for the stream
        """
        packet = await self._try_recv()
        if isinstance(packet, ControlPacket):
            assert isinstance(packet, ResetPacket)
            self.reset()
            raise ConnectionResetError
        else:
            assert isinstance(packet, DataPacket)
            if packet.is_eof:
                assert self._recv_queue.empty()
                self._recv_queue.shutdown(immediate=True)
            return packet

    def set_close_callback(self, callback: Any) -> None:
        if self._closing.is_set():
            raise Exception("Socket is closing")
        self._close_callback = callback

    async def wait_closed(self) -> None:
        await self._closing.wait()
        await self._recv_queue.join()
        await self._send_queue.join()

    def reset(self) -> None:
        self.close(force=True)

    def close(self, force: bool = False) -> None:
        self._closing.set()
        self._send_queue.shutdown(immediate=force)
        self._recv_queue.shutdown(immediate=force)

    def _on_close(self) -> None:
        remove_connection(self)
        if self._close_callback is not None:
            self._close_callback()
            self._close_callback = None

    def deliver(self, packet: Packet) -> None:
        """
        Called from the network event loop to deliver a packet.
        """
        try:
            self._recv_queue.put_nowait(packet)
        except asyncio.QueueShutDown:
            pass

    def fetch(self) -> Packet | None:
        """
        Called from the network event loop to get packets enqueued from the application.
        """
        try:
            packet = self._send_queue.get_nowait()
            self._send_queue.task_done()
            return packet
        except (asyncio.QueueEmpty, asyncio.QueueShutDown):
            return None


class ServerSocket(Socket):
    """
    A listening server socket.

    ServerSockets are created when a server calls `listen` (indirectly via `asyncio.create_server`). 
    They create an endpoint in the simulated network. When `network.connect` is called, a new socket 
    pair is created and
    """
    def __init__(self, addr: Address):
        self._addr = addr
        self._connections: [ConnectedSocket] = []
        self._queue: Optional[Queue] = None
        self._listening = False
        self._is_open = True
        self._accept_callback = None

    async def accept(self) -> ConnectedSocket:
        if not self._listening:
            raise OSError("Socket is not listening")
        try:
            packet = await self._queue.get()
        except (asyncio.QueueEmpty, asyncio.QueueShutDown):
            raise IOError("Socket closed")
        assert isinstance(packet, SynPacket)
        sock = ConnectedSocket(packet.dst, packet.src, packet.connection_no)
        self._connections.append(sock)
        if self._accept_callback is not None:
            self._accept_callback(sock)
        register_socket(sock, packet.peer_socket)
        assert _connecting_sockets.get(packet.peer_socket._local_addr) is packet.peer_socket
        del _connecting_sockets[packet.peer_socket._local_addr]
        packet.peer_socket.deliver(AckPacket(sock._local_addr, sock._remote_addr, sock))
        return sock

    def on_accept(self, callback):
        assert self._accept_callback is None
        self._accept_callback = callback

    def listen(self, backlog: int = 100) -> None:
        if self._listening:
            raise OSError("Socket already listening")
        if backlog < 0:
            backlog = 0
        self._queue = Queue(backlog+1)
        register_listener(self)
        self._listening = True

    def deliver(self, packet: ControlPacket) -> None:
        """
        Used for delivering SYNs from clients.
        """
        try:
            self._queue.put_nowait(packet)
        except asyncio.QueueFull:
            raise RuntimeError("This needs to send a RST back to the sender")

    def is_open(self) -> bool:
        return self._is_open

    def close(self) -> None:
        self._is_open = False
        del _servers[self._addr]
        self._queue.shutdown(immediate=True)
        # Really RSTs need to be sent back to everything in the queue

    def getsockname(self) -> Address:
        return self._addr

class SimTransport(Transport):
    """
    A transport over a ConnectedSocket. The transport has a write queue that absorbs writes from the protocol
    and eventually delivers them to the socket.
    """

    def __init__(self, sock: ConnectedSocket, proto: Protocol, loop: BaseEventLoop, server=None):
        super().__init__(extra={'socket': sock, 'peername': sock.getpeername()})
        self._sock = sock
        self._protocol = proto
        self._poller = loop.create_task(self._run_protocol())
        self._writer = loop.create_task(self._run_writer())
        self._closing = False
        self._write_queue = Queue()
        self._conn_lost: bool = False
        self._loop = loop
        self._reading = asyncio.Event()
        self._reading.set()
        self._protocol.connection_made(self)
        self._server = server
        if self._server is not None:
            self._server._attach(self)


    def is_closing(self):
        return self._closing

    def close(self):
        if self._closing:
            return
        self._closing = True
        self._poller.cancel()
        self._write_queue.shutdown()

    def pause_reading(self):
        if not self.is_reading():
            return
        self._reading.clear()
        if self._loop.get_debug():
            logger.debug("%r pauses reading", self)

    def resume_reading(self):
        if self._closing or self._reading.is_set():
            return
        self._reading.set()
        if self._loop.get_debug():
            logger.debug("%r resumes reading", self)

    async def _run_protocol(self):
        try:
            while True:
                packet = None
                packet = await self._sock.recv()
                await self._reading.wait()
                self._protocol.data_received(packet.data)
                if packet.is_eof:
                    if not self._protocol.eof_received():
                        self._sock.close()
                    break
        except (BlockingIOError, InterruptedError):
            return
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            self._fatal_error(exc, 'Fatal read error on socket transport')
            return
        finally:
            self.close()

    def _fatal_error(self, exc, message='Fatal error on transport'):
        # Should be called from exception handler only.
        if isinstance(exc, OSError):
            if self._loop.get_debug():
                logger.debug("%r: %s", self, message, exc_info=True)
        else:
            self._loop.call_exception_handler({
                'message': message,
                'exception': exc,
                'transport': self,
                'protocol': self._protocol,
            })
        self._force_close(exc)

    def _call_connection_lost(self, exc):
        try:
            self._protocol.connection_lost(exc)
        finally:
            self._sock.close()
            self._sock = None
            self._protocol = None
            self._loop = None
            server = self._server
            if server is not None:
                server._detach(self)
                self._server = None

    async def _run_writer(self):
        exe = None
        try:
            while True:
                data = await self._write_queue.get()
                self._write_queue.task_done()
                await self._sock.send(data)
        except asyncio.QueueShutDown:
                pass
        except BaseException as e:
            exe = e
        finally:
            self._force_close(exe)

    def write(self, data):
        self._write_queue.put_nowait(data)

    def writelines(self, list_of_data):
        for line in list_of_data:
            self.write(line)

    def write_eof(self):
        self._sock.close()

    def can_write_eof(self):
        return True

    def abort(self):
        self._force_close(None)

    def _force_close(self, exc):
        if self._conn_lost:
            return
        self._write_queue.shutdown(immediate=True)
        if not self._closing:
            self._closing = True
            self._poller.cancel()
        self._conn_lost = True
        self._loop.call_soon(self._call_connection_lost, exc)

@dataclass
class Connection:
    socket: ConnectedSocket
    peer_socket: ConnectedSocket
    sequence_no: int


_connecting_sockets: Dict[Address, ConnectedSocket] = {}
_connected_sockets: Dict[Tuple[Address, Address], Connection] = {}
_servers: Dict[Address, ServerSocket] = {}
_sequence_no: int = 0



def deliver_one() -> bool:
    """
    This method is the core of the network delivery. Each time the main event loop runs, it calls
    this method once to move a packet from the send queue of one socket to the receive queue of
    its peer.

    The network can be set up to block or delay packets by host or connection.
    """
    for socket in _connecting_sockets.values():
        packet = socket.fetch()
        if packet is not None:
            try:
                server_socket = _servers[packet.dst]
                assert isinstance(packet, SynPacket)
                logger.trace(f"Delivering SYN to {packet.dst}")
                server_socket.deliver(packet)
            except KeyError:
                raise ConnectionRefusedError
            return True
    for connection in _connected_sockets.values():
        sock = connection.socket
        packet = sock.fetch()
        if packet is not None:
            try:
                logger.trace(f"Delivering packet to {packet.dst}")
                connection.peer_socket.deliver(packet)
            except ConnectionResetError:
                try:
                    sock.deliver(ResetPacket(sock._remote_addr, sock._local_addr))
                except ConnectionResetError:
                    pass
            return True
    return False

def remove_connection(sock: ConnectedSocket) -> None:
    connection = _connected_sockets.get((sock._local_addr, sock._remote_addr))
    if connection is not None:
        assert connection.sequence_no == sock._connection_no
        del _connected_sockets[(sock._local_addr, sock._remote_addr)]

async def connect(src: Address, dest: Address) -> ConnectedSocket:
    """
    Connects two hosts together.

    This method is called from the client to create a connection to the host. It takes the client's host and port
    and creates a connection to a listening socket at the destination host and port. Once the method 
    returns, the connection is in the server's queue, waiting for an accept to be called on the 
    listening socket.
    """
    global _sequence_no
    if (src, dest) in _connected_sockets:
        raise Exception(f"Pair ({src}, {dest}) already connected")
    assert (dest, src) not in _connected_sockets
    client_socket = ConnectedSocket(src, dest, _sequence_no)
    _sequence_no += 1
    _connecting_sockets[src] = client_socket
    await client_socket.connect()
    return client_socket


def bind(addr: Address) -> ServerSocket:
    return ServerSocket(addr)

def register_listener(sock: ServerSocket) -> None:
    """
    Creates a listening socket on a port.

    This method is called from the server to create a listening socket on the host and port. Once the method 
    returns, the server is listening for connections on the specified port. Unlike in the real world, you
    don't have to create a socket and then bind it - you just call `listen` and it does it all.
    """
    if sock._addr in _servers:
        raise Exception("Address already bound")
    _servers[sock._addr] = sock


def register_socket(socket: ConnectedSocket, peer_socket: ConnectedSocket) -> None:
    connection = Connection(socket, peer_socket, socket._connection_no)
    _connected_sockets[(socket._local_addr, socket._remote_addr)] = connection
