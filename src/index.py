from workers import WorkerEntrypoint, DurableObject, Response
import asyncpg
from urllib.parse import urlparse


class ThermAssureMQTT(DurableObject):

    def __init__(self, ctx, env):
        super().__init__(ctx, env)

    async def ping(self):
        return "ThermAssureMQTT Durable Object is working"


class Default(WorkerEntrypoint):

    async def fetch(self, request):

        url = urlparse(request.url)

        # Test Durable Object
        if url.path == "/do-test":

            stub = self.env.THERMassureMQTT.getByName("main")

            result = await stub.ping()

            return Response.json({
                "success": True,
                "message": result
            })

        # Existing Hyperdrive → Tiger Cloud test
        hd = self.env.HYPERDRIVE

        try:

            connection = await asyncpg.connect(
                host=hd.host,
                port=int(hd.port),
                user=hd.user,
                password=hd.password,
                database=hd.database,
                ssl=False,
            )

            result = await connection.fetchval("SELECT 1")

            await connection.close()

            return Response.json({
                "success": True,
                "message": "Connected to Tiger Cloud through Hyperdrive",
                "result": result
            })

        except Exception as e:

            return Response.json({
                "success": False,
                "error": str(e)
            }, status=500)
