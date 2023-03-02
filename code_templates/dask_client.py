#
# Modify this according to your configuration
#
from dask.distributed import Client


def build_dask_client():
    return Client()
