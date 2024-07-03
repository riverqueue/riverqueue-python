#
# Run with:
#
#     rye run python3 -m examples.async_client_insert_tx_example
#

import asyncio
from dataclasses import dataclass
import json
import riverqueue
import sqlalchemy

from examples.helpers import dev_database_url
from riverqueue.driver import riversqlalchemy


@dataclass
class SortArgs:
    strings: list[str]

    kind: str = "sort"

    def to_json(self) -> str:
        return json.dumps({"strings": self.strings})


async def example():
    engine = sqlalchemy.ext.asyncio.create_async_engine(dev_database_url(is_async=True))
    client = riverqueue.AsyncClient(riversqlalchemy.AsyncDriver(engine))

    async with engine.begin() as session:
        insert_res = await client.insert_tx(
            session,
            SortArgs(strings=["whale", "tiger", "bear"]),
            insert_opts=riverqueue.InsertOpts(
                unique_opts=riverqueue.UniqueOpts(by_period=900)
            ),
        )
        print(insert_res)


if __name__ == "__main__":
    asyncio.set_event_loop(asyncio.new_event_loop())
    asyncio.run(example())
