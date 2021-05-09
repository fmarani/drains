"""
Drains is an ASGI middleware for Server sent events backed by Redis streams
"""
import asyncio
import logging

import aioredis


__version__ = "0.1.2"
logger = logging.getLogger(__name__)


async def ssend_async(stream, *, event, data=None):
    logger.info("sending event to %s", stream)
    if data:
        fields = {b"event": event, b"data": data}
    else:
        fields = {b"event": event}

    redis = await aioredis.create_redis("redis://localhost")
    result = await redis.xadd(stream, fields)

    redis.close()
    await redis.wait_closed()


def ssend(stream, *, event, data=None):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ssend_async(stream, event=event, data=data))
