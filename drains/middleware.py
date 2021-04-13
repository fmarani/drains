import aioredis
import asyncio
from urllib.parse import parse_qs


class LimitReached(Exception):
    pass


class SSEEndpoint:
    def __init__(self, url_prefix, app):
        self.url_prefix = url_prefix
        self.app = app

    async def create_redis_conn(self):
        return await aioredis.create_redis('redis://localhost/0')

    async def redis_loop(self, channel_name, stream_from, limit_events, send):
        print("creating redis listener")
        redis = await self.create_redis_conn()

        try:
            if stream_from:
                print(stream_from)
                results = await redis.xrange(channel_name, start=stream_from, stop=b"+")
                for result in results:
                    stream_id, msg = result
                    msg = dict(msg)
                    print(stream_id, msg)
                    await send({
                        'type': 'http.response.body',
                        'body': b"event: " + stream_id + b"|" + msg[b'message'] + b"\r\n",
                        'more_body': True
                    })
            print(f"awaiting items from {channel_name}")
            c = 0
            while results := await redis.xread([channel_name]):
                print(f"got results from {channel_name}")
                for result in results:
                    c += 1
                    stream_name, stream_id, msg = result
                    msg = dict(msg)
                    print(stream_id)
                    print(msg)
                    await send({
                        'type': 'http.response.body',
                        'body': b"event: " + stream_id + b"|" + msg[b'message'] + b"\r\n",
                        'more_body': True
                    })
                    if limit_events and c >= limit_events:
                        print("hitting limit")
                        raise LimitReached("returned enough events")
        except (asyncio.CancelledError, LimitReached):
            print("closing redis")
            redis.close()
            await send({
                'type': 'http.response.body',
                'body': b'',
            })

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            await self.app(scope, receive, send)
            return

        if scope['method'] == "GET" and scope['path'].startswith(self.url_prefix):
            channel_name = scope['path'][len(self.url_prefix):].strip("/")
            if channel_name == "":
                raise Exception("channel name must be not empty")

            query_string = parse_qs(scope['query_string'])
            stream_from = limit_events = None

            if query_string.get(b'from'):
                stream_from = query_string.get(b'from')[0]
            if query_string.get(b'limit'):
                limit_events = int(query_string.get(b'limit')[0])

            await send({
                'type': 'http.response.start',
                'status': 200,
                'headers': [
                    [b'content-type', b'text/event-stream'],
                ]
            })
            print("sending headers")

            print("creating sender")

            looper = self.redis_loop(channel_name, stream_from, limit_events, send)
            looper_task = asyncio.create_task(looper)

            try:
                while True:
                    done, still = await asyncio.wait([receive()])
                    for d in done:
                        event = await d
                        print("event", event)
                        if event['type'] == "http.disconnect":
                            print("client disconnecting")
                            raise IOError
                    await asyncio.sleep(0.5)
            except IOError:
                pass
            except asyncio.CancelledError:
                print("wait cancelled")

            print("cancelling sender")
            looper_task.cancel()
            try:
                await looper_task
            except asyncio.CancelledError:
                print("cancelled sender")
        else:
            await self.app(scope, receive, send)
