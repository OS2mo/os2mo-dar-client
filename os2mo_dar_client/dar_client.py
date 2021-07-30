# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import warnings
from asyncio.exceptions import TimeoutError
from types import TracebackType
from typing import Optional
from typing import Type

import aiohttp
from ra_utils.syncable import Syncable


class AsyncDARClient:
    def __init__(self) -> None:
        self._session: Optional[aiohttp.ClientSession] = None
        self._baseurl: str = "https://dawa.aws.dk"

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


class DARClient(Syncable, AsyncDARClient):
    pass
