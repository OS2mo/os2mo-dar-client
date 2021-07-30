# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import pytest

from .utils import assert_dar_response
from .utils import dar_lookup
from .utils import dar_non_existent
from .utils import dar_parameterize
from os2mo_dar_client import AsyncDARClient


@pytest.mark.integrationtest
async def test_healthcheck_async(loop):
    """Test healthcheck passes."""
    darclient = AsyncDARClient()
    async with darclient:
        result = await darclient.healthcheck()
    assert result is True


@pytest.mark.integrationtest
@pytest.mark.parametrize(*dar_parameterize)
async def test_dar_fetch_single(uuid, expected, loop):
    """Test lookup of single entry passes."""
    darclient = AsyncDARClient()
    async with darclient:
        result = await darclient.fetch_single(uuid)
    assert_dar_response(result, expected)


@pytest.mark.integrationtest
@pytest.mark.parametrize("uuid", dar_non_existent)
async def test_dar_fetch_single_non_existent(uuid, loop):
    """Test lookup of a single non-existent entry fails."""
    darclient = AsyncDARClient()
    async with darclient:
        with pytest.raises(ValueError) as excinfo:
            await darclient.fetch_single(uuid)
        assert "No address match found in DAR" in str(excinfo.value)


@pytest.mark.integrationtest
@pytest.mark.parametrize(*dar_parameterize)
async def test_dar_fetch(uuid, expected, loop):
    """Test lookup of single entry using dar_fetch passes."""
    darclient = AsyncDARClient()
    async with darclient:
        results, missing = await darclient.fetch({uuid})
    assert not missing
    assert len(results) == 1
    result = next(iter(results.values()))
    assert_dar_response(result, expected)


@pytest.mark.integrationtest
@pytest.mark.parametrize("uuid", dar_non_existent)
async def test_dar_fetch_non_existent(uuid, loop):
    """Test lookup of single non-existent entry using dar_fetch fails."""
    darclient = AsyncDARClient()
    async with darclient:
        results, missing = await darclient.fetch({uuid})
    assert len(missing) == 1
    assert not results
    result = next(iter(missing))
    assert result == uuid


@pytest.mark.integrationtest
async def test_dar_fetch_zero(loop):
    """Test lookup of zero entries using dar_fetch passes."""
    darclient = AsyncDARClient()
    async with darclient:
        results, missing = await darclient.fetch(set())
    assert len(missing) == 0
    assert len(results) == 0


@pytest.mark.integrationtest
async def test_dar_fetch_multiple(loop):
    """Test lookup of multiple entries using dar_fetch passes."""
    darclient = AsyncDARClient()
    async with darclient:
        results, missing = await darclient.fetch(set(dar_lookup.keys()))
    assert not missing
    assert len(results) == len(dar_lookup)
    for uuid, expected in dar_lookup.items():
        result = results[uuid]
        assert_dar_response(result, expected)


@pytest.mark.integrationtest
async def test_dar_fetch_multiple_chunked(loop):
    """Test lookup of multiple entries using dar_fetch passes."""
    darclient = AsyncDARClient()
    async with darclient:
        results, missing = await darclient.fetch(set(dar_lookup.keys()), chunk_size=1)
    assert not missing
    assert len(results) == len(dar_lookup)
    for uuid, expected in dar_lookup.items():
        result = results[uuid]
        assert_dar_response(result, expected)


@pytest.mark.integrationtest
async def test_dar_fetch_multiple_non_existent(loop):
    """Test lookup of multiple non-existent entries using dar_fetch fails."""
    darclient = AsyncDARClient()
    async with darclient:
        results, missing = await darclient.fetch(dar_non_existent)
    assert len(missing) == len(dar_non_existent)
    assert not results
    for uuid in dar_non_existent:
        assert uuid in missing


@pytest.mark.integrationtest
async def test_dar_fetch_multiple_mixed_existence(loop):
    """Test lookup of multiple mixed-existent entries using dar_fetch."""
    darclient = AsyncDARClient()
    async with darclient:
        results, missing = await darclient.fetch(
            set.union(dar_non_existent, set(dar_lookup.keys()))
        )
    assert len(missing) == len(dar_non_existent)
    assert len(results) == len(dar_lookup)
    for uuid in dar_non_existent:
        assert uuid in missing
    for uuid, expected in dar_lookup.items():
        result = results[uuid]
        assert_dar_response(result, expected)
