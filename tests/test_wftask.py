import unittest

from wfc2dask.wftask import WFTask


class TestWFTask(unittest.TestCase):
    def test_sequence(self):
        in_fn = "samples/unittests/hello-world-sequence.json"
        tasks = WFTask.load(in_fn)
        pass

    def test_join(self):
        in_fn = "samples/unittests/hello-world-join.json"
        tasks = WFTask.load(in_fn)
        pass


if __name__ == '__main__':
    unittest.main()
