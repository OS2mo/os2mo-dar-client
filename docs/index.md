<!--
SPDX-FileCopyrightText: 2021 Magenta ApS <https://magenta.dk>
SPDX-License-Identifier: MPL-2.0
-->

# OS2mo DAR Client

OS2mo DAR Client is a client for [DAWA / DAR](https://dawadocs.dataforsyningen.dk/).

## Requirements

Python 3.8+

Dependencies:

* <a href="https://more-itertools.readthedocs.io/" class="external-link" target="_blank">More Itertools</a>
* <a href="https://docs.aiohttp.org/en/stable/" class="external-link" target="_blank">AIOHTTP</a>
* <a href="https://rammearkitektur.docs.magenta.dk/ra-utils/index.html" class="external-link" target="_blank">RA Utils</a>

## Installation

```console
$ pip install os2mo-dar-client
```

## Usage
```Python
from os2mo_dar_client import DARClient

darclient = DARClient()
with darclient:
    print(darclient.healthcheck())
```

## License

This project is licensed under the terms of the MPL-2.0 license.
