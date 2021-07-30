# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import warnings
from asyncio.exceptions import TimeoutError
from asyncio import create_task
from asyncio import gather
from types import TracebackType
from typing import Any
from typing import Dict
from typing import Optional
from typing import Set
from typing import Type
from typing import Tuple
from typing import List
from uuid import UUID

import aiohttp
from ra_utils.syncable import Syncable
from collections import ChainMap
from functools import partial
from operator import itemgetter

from more_itertools import chunked, unzip

class AsyncDARClient:
    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        self._baseurl: str = "https://api.dataforsyningen.dk"

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

    # TODO: Tenancity.retrying 
    # TODO: Caching
    async def _dar_fetch_non_chunked(self, uuids: List[UUID], addrtype: str) -> Tuple[Dict[UUID, Any], Set[UUID]]:
        """Lookup uuids in DAR (without chunking).

        Args:
            uuids: List of DAR UUIDs to lookup.
            addrtype: The address type to lookup.

        Returns:
            * dict: Map from UUID to DAR reply.
            * set: Set of UUIDs of entries which were not found.
        """
        url = f"{self._baseurl}/{addrtype}"
        params = {"id": "|".join(map(str, uuids)), "struktur": "mini", "noformat": 1}

        async with self._get_session().get(url, params=params) as response:
            response.raise_for_status()
            body = await response.json()

            result = {addr['id']: addr for addr in body}

            found_uuids = set(map(itemgetter("id"), body))
            missing = set(uuids) - found_uuids

            return result, missing

    async def _dar_fetch_chunked(self, uuids: List[UUID], addrtype: str, chunk_size: int) -> Tuple[Dict[UUID, Any], Set[UUID]]:
        """Lookup uuids in DAR (chunked).

        Args:
            uuids: List of DAR UUIDs.
            addrtype: The address type to lookup.
            chunk_size: Number of UUIDs per block, sent to DAR.

        Returns:
            * dict: Map from UUID to DAR reply.
            * set: Set of UUIDs of entries which were not found.
        """

        def create_task(uuid_chunk):
            return create_task(
                self._dar_fetch_non_chunked(uuid_chunk, addrtype=addrtype, client=client)
            )

        # Chunk our UUIDs into blocks of chunk_size
        uuid_chunks = chunked(uuids, chunk_size)
        # Convert chunks into a list of asyncio.tasks
        tasks = list(map(create_task, uuid_chunks))
        # Here 'result' is a list of tuples (dict, set) => (result, missing)
        result = await gather(*tasks)
        # First we unzip 'result' to get a list of results and a list of missing
        result_dicts, missing_sets = unzip(result)
        # Then we union the dicts and sets before returning
        combined_result = dict(ChainMap(*result_dicts))
        combined_missing = set.union(*missing_sets)
        return combined_result, combined_missing

    # TODO: Lookup through 'adresser', 'adgangsadresser', 'historik/adresser', 'historik/adgangsadresser', to ensure chance of matches
    async def dar_fetch(self, uuids: List[UUID], addrtype: str="adresser", chunk_size:int =150) -> Tuple[Dict[UUID, Any], Set[UUID]]:
        """Lookup uuids in DAR (chunked if required).

        Args:
            uuids: List of DAR UUIDs.
            addrtype: The address type to lookup.
            chunk_size: Number of UUIDs per block, sent to DAR.

        Returns:
            * dict: Map from UUID to DAR reply.
            * set: Set of UUIDs of entries which were not found.
        """
        num_uuids = len(uuids)
        if num_uuids == 0:
            return dict(), set()
        if num_uuids <= chunk_size:
            return await self._dar_fetch_non_chunked(uuids, addrtype)
        return await self._dar_fetch_chunked(uuids, addrtype, chunk_size)

    # TODO: Lookup through 'adresser', 'adgangsadresser', 'historik/adresser', 'historik/adgangsadresser', return first match
    async def dar_fetch_single(self, uuid: UUID, addrtype: str="adresser") -> Tuple[Dict[UUID, Any], Set[UUID]]:
        """Lookup uuid in DAR.

        Args:
            uuid: DAR UUID.
            addrtype: The address type to lookup.

        Raises:
            ValueError: If UUID is not found.

        Returns:
            * uuid: DAR UUID.
            * dict: DAR Reply
        """
        result, missing = await self._dar_fetch_non_chunked([uuid], addrtype)
        if missing:
            raise ValueError("Address not found!")
        return uuid, result[uuid]

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
    darclient = DARClient()
    with darclient:
        assert darclient.healthcheck() is True
        uuid, result = darclient.dar_fetch_single(
            "0a3f50c4-379f-32b8-e044-0003ba298018"
        )
        print(json.dumps(result, indent=4))
