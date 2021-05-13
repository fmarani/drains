import asyncio
import logging
from urllib.parse import parse_qs

import aioredis

logger = logging.getLogger(__name__)


class LimitReached(Exception):
    pass


class SSEEndpoint:
    def __init__(self, app, *, url_prefix):
        self.app = app
        self.url_prefix = url_prefix

    async def create_redis_conn(self):
        return await aioredis.create_redis("redis://localhost/0")

    async def compose_and_send_event(self, send, stream_id, redis_value):
        logger.debug("send called with %s, %s", stream_id, redis_value)
        msg = dict(redis_value)
        body = b"id: %s\r\n" % stream_id
        body += b"event: %s\r\n" % redis_value[b"event"]

        if b"data" in redis_value:
            body += b"data: %s\r\n" % redis_value[b"data"]
        else:
            body += b"data: \r\n"

        await send(
            {"type": "http.response.body", "body": body + b"\r\n", "more_body": True}
        )

    async def redis_loop(self, channel_name, stream_from, limit_events, send):
        logger.debug("creating redis listener")
        redis = await self.create_redis_conn()

        try:
            if stream_from:
                logger.debug("streaming from %s", stream_from)
                results = await redis.xrange(channel_name, start=stream_from, stop=b"+")
                for result in results:
                    stream_id, msg = result
                    await self.compose_and_send_event(send, stream_id, msg)
            logger.debug("awaiting items from %s", channel_name)
            c = 0
            while True:
                results = await redis.xread([channel_name], timeout=1000)
                logger.debug("xread returned from %s with %s", channel_name, results)
                for result in results:
                    c += 1
                    stream_name, stream_id, msg = result
                    await self.compose_and_send_event(send, stream_id, msg)
                    if limit_events and c >= limit_events:
                        logger.debug("hitting limit")
                        raise LimitReached("returned enough events")
        except (asyncio.CancelledError, LimitReached):
            logger.debug("closing redis")
            redis.close()
            await send({"type": "http.response.body", "body": b""})

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        if scope["method"] == "GET" and scope["path"].startswith(self.url_prefix):
            channel_name = scope["path"][len(self.url_prefix) :].strip("/")
            if channel_name == "":
                raise Exception("channel name must be not empty")

            query_string = parse_qs(scope["query_string"])
            stream_from = limit_events = None

            if query_string.get(b"from"):
                stream_from = query_string.get(b"from")[0]
            if query_string.get(b"limit"):
                limit_events = int(query_string.get(b"limit")[0])

            await send(
                {
                    "type": "http.response.start",
                    "status": 200,
                    "headers": [[b"content-type", b"text/event-stream"]],
                }
            )

            looper = self.redis_loop(channel_name, stream_from, limit_events, send)
            looper_task = asyncio.create_task(looper)

            try:
                while True:
                    done, still = await asyncio.wait([receive()])
                    for d in done:
                        event = await d
                        if event["type"] == "http.disconnect":
                            logger.debug("client disconnected")
                            raise IOError
                    await asyncio.sleep(0.5)
            except IOError:
                pass
            except asyncio.CancelledError:
                pass

            logger.debug("cancelling redis looper")
            looper_task.cancel()
            try:
                await looper_task
            except asyncio.CancelledError:
                logger.debug("cancelled redis looper")
        else:
            await self.app(scope, receive, send)
