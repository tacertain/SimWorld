import asyncio

from pinger import pinger
from ponger import ponger
from sim_world.testutils import simworld_run
from sim_world.world import Host


class Ponger(Host):
    """
    A host to run the ponger. It runs the ponger in its main()
    """
    def __init__(self, name):
        super().__init__(name)
        self.port = 5000

    async def main(self):
        await ponger(self.port)

class Pinger(Host):
    """
    A host for the pinger. It runs the pinger in its main()
    """
    async def ping(self, server, port):
        async def _ping(_server, _port):
            await pinger(_server, _port)

        await self.run_once(_ping(server, port))

@simworld_run
async def testall():
    ponger = Ponger("ponger")
    await ponger.start()
    # Give the server a chance to start up
    await asyncio.sleep(10)
    pinger = Pinger("pinger")
    await pinger.ping(ponger.name, ponger.port)
    ponger.shutdown()
    await ponger.gather()

