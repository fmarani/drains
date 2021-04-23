import asyncio
import time

import httpx
import pytest

from drains import send_event_async


def test_server_is_alive():
    r = httpx.get("http://localhost:8000/")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_sse_events_work_with_limit():
    channel_name = "limited"

    async def emitter():
        await send_event_async(channel_name, "payload1")
        await send_event_async(channel_name, "payload2")
        await send_event_async(channel_name, "payload3")

    async def receiver():
        async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
            req = client.build_request("GET", f"/sse/{channel_name}/?limit=3")
            resp = await client.send(req, stream=True)
            assert resp.status_code == 200

            line = 0
            async for i in resp.aiter_bytes():
                line += 1
                if i.startswith(b"event:"):
                    assert f"payload{line}" in i.decode("utf8")
            await resp.aclose()

    receiver_task = asyncio.create_task(receiver())
    await asyncio.sleep(0.3)
    await emitter()
    await receiver_task


@pytest.mark.asyncio
async def test_sse_events_work():
    channel_name = "unlimited"

    async def emitter():
        await send_event_async(channel_name, "payload1")
        await send_event_async(channel_name, "payload2")
        await send_event_async(channel_name, "payload3")

    async def receiver():
        async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
            req = client.build_request("GET", f"/sse/{channel_name}/")
            resp = await client.send(req, stream=True)
            assert resp.status_code == 200

            line = 0
            async for i in resp.aiter_bytes():
                line += 1
                if i.startswith(b"event:"):
                    assert f"payload{line}" in i.decode("utf8")
                if line == 3:
                    break
            await resp.aclose()

    receiver_task = asyncio.create_task(receiver())
    await asyncio.sleep(0.3)
    await emitter()
    await receiver_task
