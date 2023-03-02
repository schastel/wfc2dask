# Those imports are required when pretending to run the commands
import os
import pathlib
import time
import random
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def wait_a_bit(minimum_wait: float = 0.2, debug: bool = True) -> None:
    if debug:
        time.sleep(minimum_wait + random.random())  # wait for 3 to 4 seconds


def execute_task(taskname:str, command: str, inputs: list[str] = None , outputs: list[str] = None, simulate: bool = False) -> tuple[str]:
    logger.info("Executing task %s: %s / in=%s / out=%s" % (taskname, command, inputs, outputs))
    if simulate:
        logger.info("Simulating execution of task %s" % taskname)
        wait_a_bit()
        for output in outputs:
            print(command, " => ", output)
            pathlib.Path(output).touch()
    else:
        os.system(command)  # TODO Using subprocess etc. could be more appropriate
    return outputs


