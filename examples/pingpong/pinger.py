# pingpong/ping_async.py
import asyncio


async def pinger(host, port):
    reader, writer = await asyncio.open_connection(host, port)

    print("Ping: Sending ping")
    writer.write(b'ping')
    await writer.drain()

    data = await reader.read(100)
    print(f'Ping: Received {data.decode()}')

    writer.close()
    await writer.wait_closed()


if __name__ == '__main__':
    asyncio.run(pinger("localhost", 5000))