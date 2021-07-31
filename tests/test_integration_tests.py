# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0

import pytest
from typing import Dict

from os2mo_dar_client import AsyncDARClient


@pytest.mark.integrationtest
async def test_healthcheck_async(loop):
    darclient = AsyncDARClient()
    async with darclient:
        result = await darclient.healthcheck()
    assert result == True


def assert_sja2(result: Dict) -> None:
    assert result["id"] == "0a3f50c4-379f-32b8-e044-0003ba298018"
    assert result["vejnavn"] == "Skt. Johannes All\u00e9"
    assert result["husnr"] == "2"
    assert result["postnr"] == "8000"
    assert result["postnrnavn"] == "Aarhus C"
    assert result["kommunekode"] == "0751"


@pytest.mark.integrationtest
async def test_dar_fetch_single(loop):
    darclient = AsyncDARClient()
    async with darclient:
        result = await darclient.dar_fetch_single(
            "0a3f50c4-379f-32b8-e044-0003ba298018"
        )
    assert_sja2(result)


@pytest.mark.integrationtest
async def test_dar_fetch(loop):
    darclient = AsyncDARClient()
    async with darclient:
        results, missing = await darclient.dar_fetch(
            ["0a3f50c4-379f-32b8-e044-0003ba298018"]
        )
    assert not missing
    assert len(results) == 1
    result = next(iter(results.values()))
    assert_sja2(result)
