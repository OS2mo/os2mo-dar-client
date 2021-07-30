# SPDX-FileCopyrightText: Magenta ApS
#
# SPDX-License-Identifier: MPL-2.0
import json
from typing import Tuple
from uuid import UUID

import click
from ra_utils.async_to_sync import async_to_sync

from os2mo_dar_client import AsyncDARClient


@click.command()
@click.option(
    "uuids",
    "--uuid",
    type=click.UUID,
    multiple=True,
    required=True,
    help="DAR UUIDs to lookup",
)
@async_to_sync
async def cli(uuids: Tuple[UUID]) -> None:
    darclient = AsyncDARClient()
    async with darclient:
        if not await darclient.healthcheck():
            raise click.ClickException("Unable to establish connection to DAR")

        results, missing = await darclient.fetch(set(uuids))
        print("Found:", json.dumps(list(results.values()), indent=4))
        print("Missing:", json.dumps(list(map(str, missing)), indent=4))


if __name__ == "__main__":
    cli()
