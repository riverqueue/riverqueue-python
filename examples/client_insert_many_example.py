#
# Run with:
#
#     rye run python3 -m examples.client_insert_many_example
#

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


def example():
    engine = sqlalchemy.create_engine(dev_database_url())
    client = riverqueue.Client(riversqlalchemy.Driver(engine))

    num_inserted = client.insert_many(
        [
            CountArgs(count=1),
            CountArgs(count=2),
        ]
    )
    print(num_inserted)


if __name__ == "__main__":
    example()
