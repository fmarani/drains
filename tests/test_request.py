import asyncio
import aioredis
import time

import httpx
import pytest

from drains import ssend_async


async def delete_stream(stream):
    redis = await aioredis.create_redis("redis://localhost")
    await redis.delete(stream)
    redis.close()
    await redis.wait_closed()


def test_server_is_alive():
    r = httpx.get("http://localhost:8000/")
    assert r.status_code == 200


@pytest.mark.asyncio
async def test_sse_events_work_with_limit():
    channel_name = "limited"

    async def emitter():
        await ssend_async(channel_name, event="notification", data="payload1")
        await ssend_async(channel_name, event="notification", data="payload2")
        await ssend_async(channel_name, event="notification", data="payload3")

    async def receiver():
        async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
            req = client.build_request("GET", f"/sse/{channel_name}/?limit=3")
            resp = await client.send(req, stream=True)
            assert resp.status_code == 200

            line = 0
            async for i in resp.aiter_bytes():
                line += 1
                if i.startswith(b"data:"):
                    assert f"payload{line}" in i.decode("utf8")
            await resp.aclose()

    await delete_stream(channel_name)
    receiver_task = asyncio.create_task(receiver())
    await asyncio.sleep(0.3)
    await emitter()
    await receiver_task


@pytest.mark.asyncio
async def test_sse_events_work():
    channel_name = "unlimited"

    async def emitter():
        await ssend_async(channel_name, event="notification", data="payload1")
        await ssend_async(channel_name, event="notification", data="payload2")
        await ssend_async(channel_name, event="notification", data="payload3")

    async def receiver():
        async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
            req = client.build_request("GET", f"/sse/{channel_name}/")
            resp = await client.send(req, stream=True)
            assert resp.status_code == 200

            line = 0
            async for i in resp.aiter_bytes():
                line += 1
                if i.startswith(b"data:"):
                    assert f"payload{line}" in i.decode("utf8")
                if line == 3:
                    break
            await resp.aclose()

    await delete_stream(channel_name)
    receiver_task = asyncio.create_task(receiver())
    await asyncio.sleep(0.3)
    await emitter()
    await receiver_task


@pytest.mark.asyncio
async def test_slow_sse_events_work():
    channel_name = "unlimited2"

    async def emitter():
        await ssend_async(channel_name, event="notification", data="payload1")
        await ssend_async(channel_name, event="notification", data="payload2")
        await ssend_async(channel_name, event="notification", data="payload3")

    async def receiver():
        async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
            req = client.build_request("GET", f"/sse/{channel_name}/")
            resp = await client.send(req, stream=True)
            assert resp.status_code == 200

            line = 0
            async for i in resp.aiter_bytes():
                line += 1
                if i.startswith(b"data:"):
                    assert f"payload{line}" in i.decode("utf8")
                if line == 3:
                    break
            await resp.aclose()

    await delete_stream(channel_name)
    receiver_task = asyncio.create_task(receiver())
    await asyncio.sleep(5)
    await emitter()
    await receiver_task


@pytest.mark.asyncio
async def test_without_data_works():
    channel_name = "unlimited3"

    async def emitter():
        await ssend_async(channel_name, event="notification")

    async def receiver():
        async with httpx.AsyncClient(base_url="http://localhost:8000") as client:
            req = client.build_request("GET", f"/sse/{channel_name}/")
            resp = await client.send(req, stream=True)
            assert resp.status_code == 200

            async for i in resp.aiter_bytes():
                assert b"event: notification\r\n" in i
                break

            await resp.aclose()

    await delete_stream(channel_name)
    receiver_task = asyncio.create_task(receiver())
    await asyncio.sleep(0.3)
    await emitter()
    await receiver_task
