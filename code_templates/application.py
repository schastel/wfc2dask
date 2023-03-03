from dask_client import build_dask_client
from run_workflow import run_workflow
import json


def to_json(obj):
    return json.dumps(obj, indent=2, default=lambda o: o.__dict__)


def process_arguments():
    import argparse
    import sys
    parser = argparse.ArgumentParser(prog=sys.argv[0],
                                     description='Runs a workflow through dask TODO')  # TODO
    parser.add_argument("-dns", "--do-not-simulate", help="Do not simulate all tasks (default: do simulate all tasks)", action="store_false")
    parser.add_argument("-s", "--seed", help="Randomizer seed (used when simulating)")
    return parser.parse_args()


if __name__ == '__main__':
    args = process_arguments()
    with build_dask_client() as client:
        tasks = run_workflow(client, args.do_not_simulate, seed=int(args.seed))
    with open("run.json", "w") as fp:
        fp.write(to_json(tasks))
