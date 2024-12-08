import asyncio

import grpc

import helloworld_pb2
import helloworld_pb2_grpc



async def run():
    # Create a gRPC channel
    channel = grpc.aio.insecure_channel('localhost:50051')
    async with channel:
        # Create the stub
        stub = helloworld_pb2_grpc.GreeterStub(channel)
        # Create a request
        request = helloworld_pb2.HelloRequest(name="World")
        # Call the SayHello method
        response = await stub.SayHello(request)
        print(f"Server responded with: {response.message}")


if __name__ == "__main__":
    asyncio.run(run())