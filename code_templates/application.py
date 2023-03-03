from dask_client import build_dask_client
from run_workflow import run_workflow


if __name__ == '__main__':
    with build_dask_client() as client:
        run_workflow(client)
