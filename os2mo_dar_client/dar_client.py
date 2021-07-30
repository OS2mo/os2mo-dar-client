import aiohttp
import asyncio
import warnings
from typing import Any
from functools import wraps
from functools import partial

from ra_utils.syncable import Syncable


def check_session(func):
    @wraps(func)
    async def wrapped(self, *args, **kwargs):
        if self._session is None:
            raise ValueError("Session not set")
        return await func(self, *args, **kwargs)
    return wrapped


class AsyncDARClient:
    def __init__(self):
        self._session = None
        self._baseurl = "https://dawa.aws.dk"

    async def __aenter__(self):
        await self.aopen()
        return self

    async def __aexit__(self, *err):
        await self.aclose()

    async def aopen(self):
        if self._session:
            warnings.warn("aopen called with existing session", UserWarning)
            return
        connector = aiohttp.TCPConnector(limit=10)
        self._session = aiohttp.ClientSession(connector=connector)

    async def aclose(self):
        if self._session is None:
            warnings.warn("aclose called without session", UserWarning)
            return
        await self._session.close()
        self._session = None

    @check_session
    async def healthcheck(self, timeout: int = 5) -> bool:
        """Check whether DAR can be reached

        Args:
            timeout: Maximum waiting time for response.

        Returns:
            `True` if reachable, `False` otherwise.
        """
        url = f"{self._baseurl}/autocomplete"
        try:
            async with self._session.get(url, timeout=timeout) as response:
                if response.status == 200:
                    return True
                return False
        except aiohttp.ClientError:
            return False


class DARClient(Syncable, AsyncDARClient):
    pass


if __name__ == "__main__":
    from ra_utils.async_to_sync import async_to_sync

    @async_to_sync
    async def amain():
        darclient = DARClient()
        async with darclient:
            print(await darclient.healthcheck())

        await darclient.aopen()
        await darclient.aopen()
        print(await darclient.healthcheck())
        await darclient.aclose()
        await darclient.aclose()

        # ValueError: Session not set
        # await darclient.healthcheck()

    def main():
        darclient = DARClient()
        with darclient:
            print(darclient.healthcheck())

        darclient.aopen()
        darclient.aopen()
        print(darclient.healthcheck())
        darclient.aclose()
        darclient.aclose()

        # ValueError: Session not set
        # darclient.healthcheck()

    amain()
    main()
