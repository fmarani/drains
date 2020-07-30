from drains.middleware import SSEEndpoint

async def inner_app(scope, receive, send):
    if scope['type'] == 'lifespan':
        while True:
            message = await receive()
            if message['type'] == 'lifespan.startup':
                print("lifespan start")
                await send({'type': 'lifespan.startup.complete'})
            elif message['type'] == 'lifespan.shutdown':
                print("lifespan stop")
                await send({'type': 'lifespan.shutdown.complete'})
                return

    elif scope['type'] == 'http':
        print("inner_app")
        await send({
            'type': 'http.response.start',
            'status': 200,
            'headers': [
                [b'content-type', b'text/plain'],
            ]
        })
        await send({
            'type': 'http.response.body',
            'body': b'Hello, world!',
        })

app = SSEEndpoint("/sse/", inner_app)
