import asyncio
from asyncio import start_server


async def handle_connection(reader, writer):
    addr = writer.get_extra_info('peername')

    print(f'Pong: Connected by {addr}')
    data = await reader.read(100)
    if data:
        print(f'Pong: Received {data.decode()}')
        print("Pong: Sending pong")
        writer.write(b'pong')
        await writer.drain()

    writer.close()


async def ponger(port):
    server = await asyncio.start_server(handle_connection, 'localhost', port, start_serving=False)

    addr = server.sockets[0].getsockname()
    print(f'Serving on {addr}')

    async with server:
        await server.serve_forever()

async def main():
    async with asyncio.TaskGroup() as tg:
        task = tg.create_task(ponger(5000))
        await asyncio.sleep(1)
        task.cancel()
        await task

if __name__ == '__main__':
    asyncio.run(main())