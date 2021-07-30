# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0

from os2mo_dar_client import AsyncDARClient, DARClient

from warnings import catch_warnings

import asyncio
import aiohttp
import pytest
from aiohttp import web
from more_itertools import first
from ra_utils.async_to_sync import async_to_sync
from ra_utils.syncable import Syncable
from unittest.mock import MagicMock


async def darserver_mock() -> web.Application:
    async def autocomplete(request):
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get('/autocomplete', autocomplete)
    return app

@pytest.fixture
async def darserver() -> web.Application:
    return await darserver_mock()


async def darclient_mock(aiohttp_client, darserver) -> DARClient:

    class TestAsyncDARClient(AsyncDARClient):
        async def aopen(self) -> None:
            self._session = await aiohttp_client(darserver, timeout=2)

    class TestDARClient(Syncable, TestAsyncDARClient):
        pass

    darclient = TestDARClient()
    darclient._baseurl = ""
    return darclient


@pytest.fixture
async def darclient(aiohttp_client, darserver) -> DARClient:
    return await darclient_mock(aiohttp_client, darserver)


def test_healthcheck_sync(darclient: DARClient):
    assert darclient._session is None
    with pytest.raises(ValueError) as excinfo:
        darclient.healthcheck()
    assert "Session not set" in str(excinfo)

    assert darclient._session is None
    with darclient:
        assert darclient._session is not None
        result = darclient.healthcheck()
    assert result is True
    assert darclient._session is None

    assert darclient._session is None
    darclient.aopen()
    assert darclient._session is not None
    result = darclient.healthcheck()
    darclient.aclose()
    assert result is True
    assert darclient._session is None


async def test_healthcheck_async(darclient: DARClient, loop):
    assert darclient._session is None
    with pytest.raises(ValueError) as excinfo:
        await darclient.healthcheck()
    assert "Session not set" in str(excinfo)

    assert darclient._session is None
    async with darclient:
        assert darclient._session is not None
        print(darclient._get_session())
        print(type(darclient._get_session()))

        result = await darclient.healthcheck()
    assert result is True
    assert darclient._session is None

    assert darclient._session is None
    await darclient.aopen()
    assert darclient._session is not None
    result = await darclient.healthcheck()
    await darclient.aclose()
    assert result is True
    assert darclient._session is None


def test_multiple_call_warnings():
    darclient = DARClient()
    with catch_warnings(record=True) as warnings:
        darclient.aopen()
        darclient.aclose()
        assert len(warnings) == 0

    with catch_warnings(record=True) as warnings:
        darclient.aopen()
        assert len(warnings) == 0
    with catch_warnings(record=True) as warnings:
        darclient.aopen()  # This triggers a warning

        assert len(warnings) == 1
        warning = first(warnings)
        assert issubclass(warning.category, UserWarning)
        assert "aopen called with existing session" in str(warning.message)
    with catch_warnings(record=True) as warnings:
        darclient.aclose()
        assert len(warnings) == 0

    with catch_warnings(record=True) as warnings:
        darclient.aclose()

        assert len(warnings) == 1
        warning = first(warnings)
        assert issubclass(warning.category, UserWarning)
        assert "aclose called without session" in str(warning.message)


async def test_healthcheck_non_200(aiohttp_client, loop):
    # Non-200 status code
    async def autocomplete_fail(request):
        autocomplete_fail.entered = True
        raise web.HTTPInternalServerError()
    autocomplete_fail.entered = False

    app = web.Application()
    app.router.add_get('/autocomplete', autocomplete_fail)
    darclient = await darclient_mock(aiohttp_client, app)
    async with darclient:
        result = await darclient.healthcheck()
    assert result is False
    assert autocomplete_fail.entered is True

async def test_healthcheck_timeout(aiohttp_client, loop):
    # Timeout
    async def autocomplete_slow(request):
        autocomplete_slow.entered = True
        await asyncio.sleep(5)
        return web.Response(text="OK")
    autocomplete_slow.entered = False

    app = web.Application()
    app.router.add_get('/autocomplete', autocomplete_slow)
    darclient = await darclient_mock(aiohttp_client, app)
    async with darclient:
        result = await darclient.healthcheck(1)
    assert result is False
    assert autocomplete_slow.entered is True

async def test_healthcheck_client_error(darclient, loop):
    # ClientError
    async with darclient:
        get_mock = MagicMock()
        get_mock.side_effect = aiohttp.ClientError("BOOM")
        darclient._get_session().get = get_mock
        result = await darclient.healthcheck(1)
    assert result is False
