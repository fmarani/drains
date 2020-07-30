import aioredis
import asyncio


class SSEEndpoint:
    def __init__(self, url_prefix, app):
        self.url_prefix = url_prefix
        self.app = app

    async def create_redis_conn(self):
        return await aioredis.create_redis('redis://localhost/0')

    async def redis_loop(self, channel_name, stream_from, send):
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
            while results := await redis.xread([channel_name]):
                print(f"got results from {channel_name}")
                for result in results:
                    stream_name, stream_id, msg = result
                    msg = dict(msg)
                    print(stream_id)
                    print(msg)
                    await send({
                        'type': 'http.response.body',
                        'body': b"event: " + stream_id + b"|" + msg[b'message'] + b"\r\n",
                        'more_body': True
                    })
        except asyncio.CancelledError:
            redis.close()
            await redis.wait_closed()
            await send({
                'type': 'http.response.body',
                'body': '',
            })

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            await self.app(scope, receive, send)
            return

        if scope['method'] == "GET" and scope['path'].startswith(self.url_prefix):
            channel_name = scope['path'][len(self.url_prefix):].strip("/")
            if channel_name == "":
                raise Exception("channel name must be not empty")
            stream_from = scope['query_string']
            await send({
                'type': 'http.response.start',
                'status': 200,
                'headers': [
                    [b'content-type', b'text/event-stream'],
                ]
            })
            print("sending headers")

            print("creating sender")

            receiver = receive()
            looper = self.redis_loop(channel_name, stream_from, send)
            looper_task = asyncio.create_task(looper)

            while True:
                done, _ = await asyncio.wait([receiver])
                if receiver in done:
                    event = await receiver
                    if event['type'] == "http.disconnect":
                        break
                await asyncio.sleep(0.5)

            print("cancelling sender")
            looper_task.cancel()
            await looper_task
        else:
            await self.app(scope, receive, send)
