#
# Run with:
#
#     rye run python3 -m examples.client_insert_many_example
#

import asyncio
from dataclasses import dataclass
import json
import riverqueue
import sqlalchemy

from examples.helpers import dev_database_url
from riverqueue.driver import riversqlalchemy


@dataclass
class CountArgs:
    count: int

    kind: str = "sort"

    def to_json(self) -> str:
        return json.dumps({"count": self.count})


async def example():
    engine = sqlalchemy.ext.asyncio.create_async_engine(dev_database_url(is_async=True))
    client = riverqueue.AsyncClient(riversqlalchemy.AsyncDriver(engine))

    num_inserted = await client.insert_many(
        [
            CountArgs(count=1),
            CountArgs(count=2),
        ]
    )
    print(num_inserted)


if __name__ == "__main__":
    asyncio.set_event_loop(asyncio.new_event_loop())
    asyncio.run(example())
