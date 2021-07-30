# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
from functools import partial
from unittest.mock import MagicMock

import pytest
from aiohttp import ClientResponseError

from .utils import assert_dar_response
from .utils import dar_lookup
from .utils import dar_non_existent
from .utils import dar_parameterize
from os2mo_dar_client import AsyncDARClient


@pytest.mark.parametrize(*dar_parameterize)
async def test_dar_fetch_single(adarclient: AsyncDARClient, uuid, expected):
    """Test lookup of single entry passes."""
    async with adarclient:
        result = await adarclient.fetch_single(uuid)
    assert_dar_response(result, expected)


@pytest.mark.parametrize("uuid", dar_non_existent)
async def test_dar_fetch_single_non_existent(adarclient: AsyncDARClient, uuid):
    """Test lookup of a single non-existent entry fails."""
    async with adarclient:
        with pytest.raises(ValueError) as excinfo:
            await adarclient.fetch_single(uuid)
        assert "No address match found in DAR" in str(excinfo.value)


@pytest.mark.parametrize(*dar_parameterize)
async def test_dar_fetch(adarclient: AsyncDARClient, uuid, expected):
    """Test lookup of single entry using dar_fetch passes."""
    async with adarclient:
        results, missing = await adarclient.fetch({uuid})
    assert not missing
    assert len(results) == 1
    result = next(iter(results.values()))
    assert_dar_response(result, expected)


@pytest.mark.parametrize("uuid", dar_non_existent)
async def test_dar_fetch_non_existent(adarclient: AsyncDARClient, uuid, loop):
    """Test lookup of single non-existent entry using dar_fetch fails."""
    async with adarclient:
        results, missing = await adarclient.fetch({uuid})
    assert len(missing) == 1
    assert not results
    result = next(iter(missing))
    assert result == uuid


async def test_dar_fetch_zero(adarclient: AsyncDARClient):
    """Test lookup of zero entries using dar_fetch passes."""
    async with adarclient:
        results, missing = await adarclient.fetch(set())
    assert len(missing) == 0
    assert len(results) == 0


async def test_dar_fetch_multiple(adarclient: AsyncDARClient):
    """Test lookup of multiple entries using dar_fetch passes."""
    async with adarclient:
        results, missing = await adarclient.fetch(set(dar_lookup.keys()))
    assert not missing
    assert len(results) == len(dar_lookup)
    for uuid, expected in dar_lookup.items():
        result = results[uuid]
        assert_dar_response(result, expected)


async def test_dar_fetch_multiple_chunked(adarclient: AsyncDARClient):
    """Test lookup of multiple entries using dar_fetch passes."""
    async with adarclient:
        results, missing = await adarclient.fetch(set(dar_lookup.keys()), chunk_size=1)
    assert not missing
    assert len(results) == len(dar_lookup)
    for uuid, expected in dar_lookup.items():
        result = results[uuid]
        assert_dar_response(result, expected)


async def test_dar_fetch_multiple_non_existent(adarclient: AsyncDARClient):
    """Test lookup of multiple non-existent entries using dar_fetch fails."""
    async with adarclient:
        results, missing = await adarclient.fetch(dar_non_existent)
    assert len(missing) == len(dar_non_existent)
    assert not results
    for uuid in dar_non_existent:
        assert uuid in missing


async def test_dar_fetch_multiple_mixed_existence(adarclient: AsyncDARClient):
    """Test lookup of multiple mixed-existent entries using dar_fetch."""
    async with adarclient:
        results, missing = await adarclient.fetch(
            set.union(dar_non_existent, set(dar_lookup.keys()))
        )
    assert len(missing) == len(dar_non_existent)
    assert len(results) == len(dar_lookup)
    for uuid in dar_non_existent:
        assert uuid in missing
    for uuid, expected in dar_lookup.items():
        result = results[uuid]
        assert_dar_response(result, expected)


async def test_dar_fetch_single_clientresponse_error(adarclient: AsyncDARClient):
    """Test that ClientResponseErrors are propagated."""

    SeededClientResponseError = partial(
        ClientResponseError, history=None, request_info=None
    )
    uuid = next(iter(dar_non_existent))

    # 404 are retried as next type
    with pytest.raises(ValueError) as excinfo1:
        async with adarclient:
            adarclient._get_session().get = MagicMock(  # type: ignore
                side_effect=SeededClientResponseError(status=404)
            )
            await adarclient.fetch_single(uuid)
        assert "No address match found in DAR" in str(excinfo1.value)

    # All others are propagated as-is
    with pytest.raises(ClientResponseError) as excinfo2:
        async with adarclient:
            adarclient._get_session().get = MagicMock(  # type: ignore
                side_effect=SeededClientResponseError()
            )
            await adarclient.fetch_single(uuid)
        assert "BOOM" in str(excinfo2.value)
