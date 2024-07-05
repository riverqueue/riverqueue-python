#
# Run with:
#
#     rye run python3 -m examples.all
#
# It'd be nice if this could pick up every example automatically, but after
# spending an hour just trying to find a way of making a relative import work,
# I'm not going anywhere near that problem right now.
#

import asyncio

from examples import async_client_insert_example
from examples import async_client_insert_many_example
from examples import async_client_insert_tx_example
from examples import client_insert_example
from examples import client_insert_many_example
from examples import client_insert_many_insert_opts_example
from examples import client_insert_tx_example

if __name__ == "__main__":
    asyncio.set_event_loop(asyncio.new_event_loop())

    asyncio.run(async_client_insert_example.example())
    asyncio.run(async_client_insert_many_example.example())
    asyncio.run(async_client_insert_tx_example.example())

    client_insert_example.example()
    client_insert_many_example.example()
    client_insert_many_insert_opts_example.example()
    client_insert_tx_example.example()
