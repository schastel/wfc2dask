from dask_client import build_dask_client
from run_workflow import run_workflow
import json


def to_json(obj):
    return json.dumps(obj, default=lambda o: o.__dict__)


if __name__ == '__main__':
    with build_dask_client() as client:
        tasks = run_workflow(client)
    print(to_json(tasks))
