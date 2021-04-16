import asyncio
import aioredis


async def send_event_async(stream, msg):
    fields = {b"message": msg}

    redis = await aioredis.create_redis("redis://localhost")
    result = await redis.xadd(stream, fields)

    redis.close()
    await redis.wait_closed()


def send_event(stream, msg):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_event_async(stream, msg))
