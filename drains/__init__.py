"""
Drains is an ASGI middleware for Server sent events backed by Redis streams
"""
import asyncio
import logging

import aioredis


__version__ = "0.1.2"
logger = logging.getLogger(__name__)


async def ssend_async(stream, *, event, data=None, include_id=None):
    logger.info("sending event to %s", stream)
    fields = {b"event": event}
    if data:
        fields[b"data"] = data
    if include_id:
        fields[b"include_id"] = True

    redis = await aioredis.create_redis("redis://localhost")
    result = await redis.xadd(stream, fields)

    redis.close()
    await redis.wait_closed()


def ssend(*args, **kwargs):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    loop.run_until_complete(ssend_async(*args, **kwargs))
