import asyncio
import contextvars

from sim_world.event_loop import SimWorldEventLoopPolicy

output = contextvars.ContextVar('output', default='None')

async def foo():
    output.set('foo')
    print(f"foo @ {asyncio.get_running_loop().time()} - context {output.get()}")

    async with asyncio.TaskGroup() as tg:
        tg.create_task(asyncio.sleep(2))
        asyncio.create_task(burp())
        tg.create_task(baz())
    print(f"foo @ {asyncio.get_running_loop().time()} - context {output.get()}")

async def burp():
    await asyncio.sleep(3)
    print(f"burp @ {asyncio.get_running_loop().time()} - context {output.get()}")

async def bar():
    output.set('bar')
    await asyncio.sleep(1)
    print(f"bar @ {asyncio.get_running_loop().time()} - context {output.get()}")

async def baz():
    print(f"baz @ {asyncio.get_running_loop().time()} - context {output.get()}")
    await asyncio.sleep(2)
    print(f"baz @ {asyncio.get_running_loop().time()} - context {output.get()}")

async def run():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(foo())
        tg.create_task(bar())

async def run2():
    task = asyncio.get_running_loop().create_task(foo())
    await bar()
    await task

event_one = asyncio.Event()
event_two = asyncio.Event()

async def one():
    print(f"One before set")
    await asyncio.sleep(1)
    event_one.set()
    print(f"One after set")
    await event_two.wait()
    print(f"One after wait")

async def two():
    print(f"Two before wait")
    await event_one.wait()
    print(f"Two after wait")
    await asyncio.sleep(1)
    event_two.set()
    print(f"Two after set")

async def run3():
    async with asyncio.TaskGroup() as tg:
        tg.create_task(one())
        tg.create_task(two())

if __name__ == '__main__':
    asyncio.set_event_loop_policy(SimWorldEventLoopPolicy())
    asyncio.run(run3())
