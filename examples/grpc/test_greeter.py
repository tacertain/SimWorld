import asyncio

from grpclib.client import Channel
from grpclib.server import Server

from helloworld_grpc import GreeterStub, GreeterBase
from helloworld_pb2 import HelloRequest, HelloReply
from sim_world.log import logger
from sim_world.world import Host
from sim_world.testutils import simworld_run


class ClientHost(Host):
    """
    A host to run the client on. It offers a single method that calls
    the HelloRequest method and returns the response.
    """
    async def say_hello(self, name, server, port):
        async def _say_hello(_name, _server, _port):
            async with Channel(_server, _port) as channel:
                greeter = GreeterStub(channel)
                return await greeter.SayHello(HelloRequest(name=_name))

        # Methods that need access to the network must use the
        # run_once method on the server so that the context gets
        # populated with the virtual host info where the code should
        # be run.
        return await self.run_once(_say_hello(name, server, port))

class ServerHost(Host):
    """
    A host to run the server on. It runs the gRPC server in main()
    and response to HelloRequests.
    """
    def __init__(self, name):
        super().__init__(name)
        self.started = asyncio.Event()
        self.port = 50051

    class Greeter(GreeterBase):
        async def SayHello(self, stream):
            request = await stream.recv_message()
            message = f'Hello, {request.name}!'
            await stream.send_message(HelloReply(message=message))

    async def main(self) -> None:
        server = Server([ServerHost.Greeter()])
        await server.start(self.name, self.port)
        self.started.set()
        logger.debug(f'Serving on {self.name}:{self.port}')
        await server.wait_closed()


@simworld_run
async def test_greeter():
    client = ClientHost("client")
    server = ServerHost("server")
    await server.start()
    await server.started.wait()
    response = await client.say_hello("Bob", server.name, server.port)
    assert response.message == "Hello, Bob!"

