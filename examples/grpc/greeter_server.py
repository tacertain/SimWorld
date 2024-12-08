import asyncio
import logging
from concurrent import futures

import grpc
from grpc import aio
import helloworld_pb2
import helloworld_pb2_grpc


# Implement the Greeter Servicer
class GreeterServicer(helloworld_pb2_grpc.GreeterServicer):
    async def SayHello(self, request, context):
        name = request.name
        message = f"Hello, {name}!"
        return helloworld_pb2.HelloReply(message=message)


async def serve():
    server = aio.server()
    helloworld_pb2_grpc.add_GreeterServicer_to_server(GreeterServicer(), server)
    listen_addr = "[::]:50051"
    server.add_insecure_port(listen_addr)
    print(f"Listening on {listen_addr}")
    await server.start()
    await server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve())