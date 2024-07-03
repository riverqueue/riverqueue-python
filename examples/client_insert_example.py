#
# Run with:
#
#     rye run python3 -m examples.client_insert_example
#

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


def example():
    engine = sqlalchemy.create_engine(dev_database_url())
    client = riverqueue.Client(riversqlalchemy.Driver(engine))

    insert_res = client.insert(
        SortArgs(strings=["whale", "tiger", "bear"]),
        insert_opts=riverqueue.InsertOpts(
            unique_opts=riverqueue.UniqueOpts(by_period=900)
        ),
    )
    print(insert_res)


if __name__ == "__main__":
    example()
