import pytest
from async_asgi_testclient import TestClient
from drains import send_event_async
import asyncio

from .asgi_app import app

@pytest.mark.asyncio
async def ztest_server_is_alive(xserver):
    with xserver() as xserver:
        client = httpx.AsyncClient()
        resp = await client.get(xserver.http_base_url + "/")
        assert resp.status_code == 200

@pytest.mark.asyncio
async def ztest_server_is_alive():
    client = httpx.AsyncClient(app=app, base_url="http://testserver")
    resp = await client.get("/")
    assert resp.status_code == 200

@pytest.mark.asyncio
async def test_sse_events_work():
    async def emitter():
        print("emitting to example")
        await send_event_async("example", "payload1")
        print("emitting to example")
        await send_event_async("example", "payload2")
        print("emitting to example")
        await send_event_async("example", "payload3")

    async def receiver():
        async with TestClient(app) as client:
            resp = await client.get("/sse/example/", stream=True)
            print("receiving")
            assert resp.status_code == 200

            line = 0
            async for i in resp:
                line += 1
                print("received", line)
                assert f"payload{line}" in i.decode("utf8")
                if line == 3:
                    break


    receiver_task = asyncio.create_task(receiver())
    await asyncio.sleep(0.3)
    await emitter()
    await receiver_task

    #await asyncio.gather(receiver_task, emitter_task)
