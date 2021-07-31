# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import warnings
from asyncio import gather
from asyncio.exceptions import TimeoutError
from collections import ChainMap
from enum import Enum
from functools import partial
from itertools import starmap
from operator import itemgetter
from types import TracebackType
from typing import Any
from typing import cast
from typing import ChainMap as tChainMap
from typing import Dict
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Type
from uuid import UUID

import aiohttp
from more_itertools import chunked
from more_itertools import unzip
from ra_utils.syncable import Syncable
from tenacity import Retrying
from tenacity import stop_after_attempt
from tenacity import wait_exponential


# TODO: Pydantic type
AddressReply = Dict[str, Any]


class AddressType(str, Enum):
    ADDRESS = "adresser"
    ACCESS_ADDRESS = "adgangsadresser"
    HISTORIC_ADDRESS = "historik/adresser"
    HISTORIC_ACCESS_ADDRESS = "historik/adgangsadresser"


ALL_ADDRESS_TYPES = list(AddressType)


class AsyncDARClient:
    def __init__(self, *, retrying_stop=None, retrying_wait=None) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        self._baseurl: str = "https://api.dataforsyningen.dk"
        self._retrying_stop or stop_after_attempt(5)
        self._retrying_wait or wait_exponential(multiplier=2, min=1)

    async def __aenter__(self) -> "AsyncDARClient":
        await self.aopen()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_traceback: Optional[TracebackType],
    ) -> bool:
        await self.aclose()
        return False

    async def aopen(self) -> None:
        if self._session:
            warnings.warn("aopen called with existing session", UserWarning)
            return
        connector = aiohttp.TCPConnector(limit=10)
        self._session = aiohttp.ClientSession(connector=connector)

    async def aclose(self) -> None:
        if self._session is None:
            warnings.warn("aclose called without session", UserWarning)
            return
        await self._session.close()
        self._session = None

    def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise ValueError("Session not set")
        return self._session

    async def healthcheck(self, timeout: int = 5) -> bool:
        """Check whether DAR can be reached

        Args:
            timeout: Maximum waiting time for response.

        Returns:
            `True` if reachable, `False` otherwise.
        """
        url = f"{self._baseurl}/autocomplete"
        try:
            async with self._get_session().get(url, timeout=timeout) as response:
                if response.status == 200:
                    return True
                return False
        except aiohttp.ClientError:
            return False
        except TimeoutError:
            return False

    # TODO: Caching
    async def _address_fetched(self, uuid: UUID, reply: Dict) -> None:
        pass

    # TODO: Tenancity.retrying
    async def _dar_fetch_single(
        self, uuid: UUID, addrtype: AddressType
    ) -> AddressReply:
        """Lookup uuid in DAR.

        Similar to `dar_fetch_single` but only takes a single `AddressType`.

        Args:
            uuid: DAR UUID.
            addrtypes: The address type to lookup.

        Raises:
            aiohttp.ClientResponseError: If anything goes wrong

        Returns:
            * dict: DAR Reply
        """

        url = f"{self._baseurl}/{addrtype.value}/{str(uuid)}"
        params = {"struktur": "mini", "noformat": 1}

        async with self._get_session().get(url, params=params) as response:
            response.raise_for_status()
            payload = await response.json()
            return cast(AddressReply, payload)

    # TODO: Tenancity.retrying
    async def _dar_fetch_non_chunked(
        self, uuids: Set[UUID], addrtype: AddressType
    ) -> Tuple[Dict[UUID, AddressReply], Set[UUID]]:
        """Lookup uuids in DAR (no chunking).

        Args:
            uuids: List of DAR UUIDs to lookup.
            addrtype: The address type to lookup.

        Returns:
            * dict: Map from UUID to DAR reply.
            * set: Set of UUIDs of entries which were not found.
        """
        url = f"{self._baseurl}/{addrtype.value}"
        params = {"id": "|".join(map(str, uuids)), "struktur": "mini", "noformat": 1}

        async with self._get_session().get(url, params=params) as response:
            response.raise_for_status()
            body = await response.json()

            result = {addr["id"]: addr for addr in body}

            found_uuids = set(map(itemgetter("id"), body))
            missing = set(uuids) - found_uuids

            return result, missing

    async def _dar_fetch_chunked(
        self, uuids: Set[UUID], addrtype: AddressType, chunk_size: int
    ) -> Tuple[Dict[UUID, AddressReply], Set[UUID]]:
        """Lookup uuids in DAR (chunked).

        Chunks the UUIDs before calling `_dar_fetch_non_chunked` on each chunk.

        Args:
            uuids: List of DAR UUIDs.
            addrtype: The address type to lookup.
            chunk_size: Number of UUIDs per block, sent to DAR.

        Returns:
            * dict: Map from UUID to DAR reply.
            * set: Set of UUIDs of entries which were not found.
        """

        # Chunk our UUIDs into blocks of chunk_size
        uuid_chunks = chunked(uuids, chunk_size)
        # Convert chunks into a list of asyncio.tasks
        tasks = list(
            map(partial(self._dar_fetch_non_chunked, addrtype=addrtype), uuid_chunks)
        )
        # Here 'result' is a list of tuples (dict, set) => (result, missing)
        result = await gather(*tasks)
        # First we unzip 'result' to get a list of results and a list of missing
        result_dicts, missing_sets = unzip(result)
        # Then we union the dicts and sets before returning
        combined_result = dict(ChainMap(*result_dicts))
        combined_missing = set.union(*missing_sets)
        return combined_result, combined_missing

    async def _dar_fetch(
        self, uuids: Set[UUID], addrtype: AddressType, chunk_size: int = 150
    ) -> Tuple[Dict[UUID, AddressReply], Set[UUID]]:
        """Lookup uuids in DAR (chunked if required).

        Similar to `dar_fetch` but only takes a single `AddressType`.

        Args:
            uuids: List of DAR UUIDs.
            addrtype: The address type to lookup.
            chunk_size: Number of UUIDs per block, sent to DAR.

        Returns:
            * dict: Map from UUID to DAR reply.
            * set: Set of UUIDs of entries which were not found.
        """
        num_uuids = len(uuids)
        # Short-circuit if possible, chunk if required
        if num_uuids == 0:
            return dict(), set()
        if num_uuids <= chunk_size:
            return await self._dar_fetch_non_chunked(uuids, addrtype)
        return await self._dar_fetch_chunked(uuids, addrtype, chunk_size)

    async def dar_fetch(
        self,
        uuids: Set[UUID],
        addrtypes: Optional[List[AddressType]] = None,
        chunk_size: int = 150,
    ) -> Tuple[Dict[UUID, AddressReply], Set[UUID]]:
        """Lookup uuids in DAR (chunked if required).

        Args:
            uuids: List of DAR UUIDs.
            addrtypes: The address type(s) to lookup. If `None` all 4 types are checked.
            chunk_size: Number of UUIDs per block, sent to DAR.

        Returns:
            * dict: Map from UUID to DAR reply.
            * set: Set of UUIDs of entries which were not found.
        """
        addrtypes = addrtypes or ALL_ADDRESS_TYPES
        combined_result: tChainMap[UUID, AddressReply] = ChainMap({})
        # TODO: Do all 4 in parallel?
        for addrtype in addrtypes:
            result, missing = await self._dar_fetch(
                uuids, addrtype, chunk_size=chunk_size
            )
            combined_result = ChainMap(combined_result, result)
            # If we managed to find everything, there is no need to check the remaining
            # address types, we can simply return our findings
            if not missing:
                break
            # We only need to check the missing UUIDs in the remaining address types
            uuids = missing
        final_result = dict(combined_result)
        await gather(*starmap(self._address_fetched, final_result.items()))
        return final_result, missing

    async def dar_fetch_single(
        self, uuid: UUID, addrtypes: Optional[List[AddressType]] = None
    ) -> AddressReply:
        """Lookup uuid in DAR.

        Args:
            uuid: DAR UUID.
            addrtypes: The address type(s) to lookup. If `None` all 4 types are checked.

        Raises:
            ValueError: If no match could be found

        Returns:
            * dict: DAR Reply
        """
        addrtypes = addrtypes or ALL_ADDRESS_TYPES
        # TODO: Do all 4 in parallel?
        for addrtype in addrtypes:
            try:
                payload = await self._dar_fetch_single(uuid, addrtype)
                # If we get here everything went well
                await self._address_fetched(uuid, payload)
                return payload
            except aiohttp.ClientResponseError as exc:
                # If not found, try the next address type
                if exc.status == 404:
                    continue
                raise exc
        raise ValueError("Not found")

    # TODO: query endpoints ala dawa_helper.py

    # TODO: Autocomplete endpoints


#    addrs = collections.OrderedDict(
#        (addr['tekst'], addr['adgangsadresse']['id'])
#        for addr in session.get(
#            'https://dawa.aws.dk/adgangsadresser/autocomplete',
#            # use a list to work around unordered dicts in Python < 3.6
#            params=[
#                ('per_side', 5),
#                ('noformat', '1'),
#                ('kommunekode', code),
#                ('q', q),
#            ],
#        ).json()
#    )
#
#    for addr in session.get(
#        'https://dawa.aws.dk/adresser/autocomplete',
#        # use a list to work around unordered dicts in Python < 3.6
#        params=[
#            ('per_side', 10),
#            ('noformat', '1'),
#            ('kommunekode', code),
#            ('q', q),
#        ],
#    ).json():
#        addrs.setdefault(addr['tekst'], addr['adresse']['id'])


class DARClient(Syncable, AsyncDARClient):
    pass


if __name__ == "__main__":
    import json

    async def main() -> None:
        darclient = AsyncDARClient()
        async with darclient:
            assert await darclient.healthcheck() is True
            result1 = await darclient.dar_fetch_single(
                UUID("0a3f50c4-379f-32b8-e044-0003ba298018"),
            )
            print(json.dumps(result1, indent=4))

            result2, missing = await darclient.dar_fetch(
                {UUID("0a3f50c4-379f-32b8-e044-0003ba298018")},
            )
            print(json.dumps(result2, indent=4))

    from asyncio import run

    run(main())
