# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
from typing import Tuple
from uuid import UUID

import pytest
from aiohttp import web
from ra_utils.syncable import Syncable

from os2mo_dar_client import AsyncDARClient
from os2mo_dar_client import DARClient
from os2mo_dar_client.dar_client import ALL_ADDRESS_TYPES


# Actual DAR UUIDS with actual response snippets
# Note: Integration-tests will fail if these go out of sync
dar_lookup = {
    UUID("0a3f50c4-379f-32b8-e044-0003ba298018"): {
        "id": "0a3f50c4-379f-32b8-e044-0003ba298018",
        "vejnavn": "Skt. Johannes All\u00e9",
        "husnr": "2",
        "postnr": "8000",
        "postnrnavn": "Aarhus C",
        "kommunekode": "0751",
    },
    UUID("03c59320-1edd-40f4-9bbe-af135475205e"): {
        "id": "03c59320-1edd-40f4-9bbe-af135475205e",
        "vejnavn": "Julsøvej",
        "husnr": "14",
        "postnr": "8680",
        "postnrnavn": "Ry",
        "kommunekode": "0746",
    },
}
dar_parameterize = ("uuid,expected", list(dar_lookup.items()))
dar_non_existent = {
    UUID("00000000-0000-0000-0000-000000000000"),
    UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
}


def assert_dar_response(result, expected):
    for key in expected.keys():
        assert result[key] == expected[key]


async def darserver_mock() -> web.Application:
    async def autocomplete(request):
        return web.Response(text="OK")

    async def single_address_endpoint(request):
        uuid = UUID(request.path.rsplit("/")[-1])
        if uuid not in dar_lookup:
            raise web.HTTPNotFound()
        return web.json_response(dar_lookup[uuid])

    async def address_endpoint(request):
        uuids = request.query.get("id").split("|")
        uuids = map(UUID, uuids)
        uuids = filter(lambda uuid: uuid in dar_lookup, uuids)
        result = map(lambda uuid: dar_lookup[uuid], uuids)
        return web.json_response(list(result))

    app = web.Application()
    app.router.add_get("/autocomplete", autocomplete)
    for addrtype in ALL_ADDRESS_TYPES:
        app.router.add_get(f"/{addrtype.value}/" + "{uuid}", single_address_endpoint)
        app.router.add_get(f"/{addrtype.value}", address_endpoint)
    return app


@pytest.fixture
async def darserver() -> web.Application:
    return await darserver_mock()


async def darclient_mocks(
    aiohttp_client, darserver
) -> Tuple[AsyncDARClient, DARClient]:
    class TestAsyncDARClient(AsyncDARClient):
        async def aopen(self) -> None:
            self._session = await aiohttp_client(darserver, timeout=2)

    class TestDARClient(Syncable, TestAsyncDARClient):
        pass

    adarclient = TestAsyncDARClient()
    adarclient._baseurl = ""
    darclient = TestDARClient()
    darclient._baseurl = ""
    return adarclient, darclient


async def darclient_mock(aiohttp_client, darserver) -> DARClient:
    _, darclient = await darclient_mocks(aiohttp_client, darserver)
    return darclient


@pytest.fixture
async def darclient(aiohttp_client, darserver) -> DARClient:
    return await darclient_mock(aiohttp_client, darserver)


async def adarclient_mock(aiohttp_client, darserver) -> AsyncDARClient:
    adarclient, _ = await darclient_mocks(aiohttp_client, darserver)
    return adarclient


@pytest.fixture
async def adarclient(aiohttp_client, darserver) -> AsyncDARClient:
    return await adarclient_mock(aiohttp_client, darserver)
