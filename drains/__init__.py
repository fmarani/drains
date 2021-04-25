"""
Drains is an ASGI middleware for Server sent events backed by Redis streams
"""
import asyncio

import aioredis

__version__ = "0.1.2"


async def send_event_async(stream, msg):
    fields = {b"message": msg}

    redis = await aioredis.create_redis("redis://localhost")
    result = await redis.xadd(stream, fields)

    redis.close()
    await redis.wait_closed()


def send_event(stream, msg):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(send_event_async(stream, msg))
