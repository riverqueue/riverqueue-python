import json
from dataclasses import dataclass


@dataclass
class SimpleArgs:
    kind: str = "simple"

    @staticmethod
    def to_json() -> str:
        return json.dumps({"job_num": 1})
