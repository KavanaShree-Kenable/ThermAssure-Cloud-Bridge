from workers import WorkerEntrypoint, Response
import asyncpg


class Default(WorkerEntrypoint):

    async def fetch(self, request):

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
