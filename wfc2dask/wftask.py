from __future__ import annotations
import logging


# Logging setup
logger = logging.getLogger(__name__)


class WFTask:
    """
    A minimal Task derived from https://wfcommons.org/format
    We don't care about the task execution features here, just the minimum minimorum to build up
    a consistent DAG representing the workflow and the task execution if it's not simulated.
    Namely, that means that we only support the following elements in the JSON document:
    - name
    - command.arguments
    - parents
    - childrens (sic!)
    - tasks.[link,name]
    Other elements can be present but will be ignored
    """
    def __init__(self):
        self.name = None  # Ignoring type
        self.command = None
        self.parents = set()
        self.children = set()
        self.inputs = set()
        self.outputs = set()
        pass

    @staticmethod
    def from_json(o_task: dict):
        """
        :param o_task: a task represented by a JSON object
        :return: a Task instance
        """
        logger.debug("'%s'" % o_task)
        task = WFTask()
        task.name = o_task["name"]
        try:
            for parent in o_task["parents"]:
                task.parents.add(parent)
        except KeyError:
            pass  # No parents element for the task. Ignore
        try:
            for child in o_task["childrens"]:  # There is a typo in the format name
                task.children.add(child)
        except KeyError:
            pass  # No childrens element for the task. Ignore
        if "files" in o_task:  # From the sepc, files does not have to be present
            for file in o_task["files"]:
                if file["link"] == "input":
                    task.inputs.add(file["name"])
                elif file["link"] == "output":
                    task.outputs.add(file["name"])
                else:
                    logger.debug(f"{file['link']} not supported: must be either input or output")
        try:
            task.command = o_task["command"]["arguments"]
        except KeyError:
            pass  # If the command is not specified, we will fake it by creating empty output files
        return task

    @staticmethod
    def load(filename: str) -> tuple[list[WFTask], str]:
        """
        :param filename: the name of a file containing a representation of a workflow TODO add link to format
        :return: the list of tasks in filename
        """
        import json
        with open(filename) as fp:
            return WFTask.loads(json.load(fp))

    @staticmethod
    def loads(json_workflow: dict) -> tuple[list[WFTask], str]:
        """
        :param json_workflow: A JSON document describing a workflow
        :return: the list of tasks in the JSON document
        """
        tasks = []
        for o_task in json_workflow["workflow"]["tasks"]:
            tasks.append(WFTask.from_json(o_task))
        return tasks, json_workflow["name"]
