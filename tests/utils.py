# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0

from aiohttp import web
import pytest
from ra_utils.syncable import Syncable

from os2mo_dar_client import AsyncDARClient
from os2mo_dar_client import DARClient

async def darserver_mock() -> web.Application:
    async def autocomplete(request):
        return web.Response(text="OK")

    app = web.Application()
    app.router.add_get("/autocomplete", autocomplete)
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
