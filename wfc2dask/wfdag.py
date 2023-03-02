from __future__ import annotations
import logging

from wfc2dask.wftask import WFTask

# Logging setup
logger = logging.getLogger(__name__)


class WFDAG:
    """
    The DAG corresponding to a workflow
    """

    class WFDAGTask:
        def __init__(self, wftask: WFTask, index: int):
            self.wftask = wftask
            self.dag_id = 'dv_%d' % index
            self.dag_parents = set()
            self.dag_children = set()

    def __init__(self, workflow_name):
        # This is just a placeholder to store each wftask
        self.workflow_name = workflow_name
        self.wftasks = {}
        # The vertices in the DAG, i.e. WFDAGTask instances
        self.dag_tasks = {}  # Indexed by the WFDAGTask id
        self.ordered_tasks = []

    def add_task(self, task: WFTask) -> None:
        if task.name in self.wftasks:
            raise Exception("Duplicate task named '%s'" % task.name)
        self.wftasks[task.name] = task

    @staticmethod
    def from_tasks(tasks: list[WFTask], wfname: str) -> WFDAG:
        wfdag = WFDAG(wfname)
        for wftask in tasks:
            wfdag.add_task(wftask)
        return wfdag

    @staticmethod
    def _resolve_reference(references, key):
        # Note that this is expected to raise a KeyError exception
        # if the key is unknown, i.e. in the case of "external" inputs
        _key = key
        already_seen = set()
        while references[_key] != _key:
            if _key in already_seen:
                # Let's prevent cycles (shouldn't happen if the wf is correctly specified)
                raise Exception("Cycle detected for key '%s': %s", key, already_seen)
            already_seen.add(_key)
            _key = references[_key]
        return _key

    def _build_dag_first_pass(self):
        # First pass: Collect all dependencies
        # Note: The dependencies between two tasks can be defined in parents, in childrens, as a files.input,
        # and/or as a files.output. However, there is no guaranteed completeness, e.g. if task1 is in the parents
        # of task2, task1 children do not have to mention task2
        # The purpose of the references dictionary is to collect the dependencies
        # + task.id -> task.id (this is our stop condition when resolving a dependency)
        # + task.name -> task.id
        # +
        references = {}
        for task_index, wftask in enumerate(self.wftasks.values()):
            task = WFDAG.WFDAGTask(wftask, task_index)
            self.dag_tasks[task.dag_id] = task
            references[task.dag_id] = task.dag_id  # Stop condition when resolving a dependency/reference
            references[task.wftask.name] = task.dag_id
            # wftask.children is a list of task.names (str)
            for child in task.wftask.children:
                task.dag_children.add(child)
            # wftask.parents is a list of task.names (str)
            for parent in task.wftask.parents:
                task.dag_parents.add(parent)
            # task outputs
            for out in task.wftask.outputs:
                if out in references:
                    # TODO Make this a specific Exception
                    raise Exception(
                        "'%s' in '%s' is also an output of '%s?!" % (out, task.wftask.name, references[out]))
                references[out] = task.dag_id  # This output is a direct dependendy on this task
            for inp in task.wftask.inputs:
                # It is not known (yet) if inp is the output of another task or an "external" input
                # For the moment, we just add it to the parents. We will resolve it in the second pass
                task.dag_parents.add(inp)
            pass
        return references

    def _build_dag_second_pass(self, references: dict):
        # The parents and children of each WFDAGTask has been built in the first pass
        # Now we need to consolidate them, that is, when a parent or child is encountered, we need to replace it with
        # a dag_id (if the reference can be resolved), remove it (if the reference cannot be resolved, i.e. in the case
        # of "external" inputs), and then ensure that the resulting set contains each dag_id only once (this will be
        # ensured by the use of a Python set)
        for task in self.dag_tasks.values():
            consolidated_parents = set()
            for parent in task.dag_parents:
                try:
                    consolidated_parents.add(WFDAG._resolve_reference(references, parent))
                except KeyError:
                    logger.info("In task '%s', '%s' seems to be an external input" % (task.wftask.name, parent))
            task.dag_parents = consolidated_parents
            consolidated_children = set()
            for child in task.dag_children:
                try:
                    consolidated_children.add(WFDAG._resolve_reference(references, child))
                except KeyError:
                    logger.info("'%s' is a terminal output" % child)
            task.dag_children = consolidated_children
        pass

    def _build_dag(self):
        # This method is called once all tasks have been added through add_task
        references = self._build_dag_first_pass()
        self._build_dag_second_pass(references)
        self.order_tasks()

    def order_tasks(self):
        # The tasks just need to be ordered in a logical sense:
        # + Tasks without parents first,
        # + Then tasks that are children of those
        # + The children of those children,
        # + ... and so on
        tasks_to_order = list(self.dag_tasks.keys())
        for task in self.dag_tasks.values():
            # Backup the parents list
            task.dag_parents_backup = set(task.dag_parents)
        while len(tasks_to_order) != 0:
            logger.debug("Bef: %d tasks with ordered size %d" % (len(tasks_to_order), len(self.ordered_tasks)))
            tasks_without_parents = []
            tasks_with_parents = []
            for task_id in tasks_to_order:
                if len(self.dag_tasks[task_id].dag_parents) == 0:
                    tasks_without_parents.append(task_id)
                else:
                    tasks_with_parents.append(task_id)
            self.ordered_tasks.append(tasks_without_parents)
            for task_id in tasks_with_parents:
                for id in tasks_without_parents:
                    if id in self.dag_tasks[task_id].dag_parents:
                        self.dag_tasks[task_id].dag_parents.remove(id)
            tasks_to_order = tasks_with_parents
            logger.debug("Aft: %d tasks with ordered size %d" % (len(tasks_to_order), len(self.ordered_tasks)))
        # Restore the deleted parents and delete
        for task in self.dag_tasks.values():
            task.dag_parents = set(task.dag_parents_backup)
            del task.dag_parents_backup

    def __repr__(self):
        rep = '\n'
        for level, tasks in enumerate(self.ordered_tasks):
            rep += "Level %d: " % level
            rep += "; ".join(['%s (%s)' % (_id, self.dag_tasks[_id].wftask.name) for _id in tasks])
            rep += "\n"
        return rep

    def dask_codelines(self, simulation_configuration: dict = dict()) -> list[str]:
        self._build_dag()
        logger.debug('%s' % self)
        noindent_python_codelines = []
        #
        for level, task_ids in enumerate(self.ordered_tasks):
            noindent_python_codelines.append("# Level %d (%d tasks)" % (level + 1, len(task_ids)))
            for task_id in task_ids:
                task = self.dag_tasks[task_id]
                noindent_python_codelines.append("# Task %s (%s)" % (task_id, task.wftask.name))
                fut_task_varname = "fut_%s" % task_id
                command = "'%s'" % task.wftask.command if task.wftask.command is not None else "None"
                fut_inputs_list = ", ".join(["fut_%s" % parent_id for parent_id in task.dag_parents])
                outputs_list = list(task.wftask.outputs)
                # simulate_flag = 'simulate = True' if task.wftask.name in simulation_configuration else 'simulate = False'
                simulate_flag = "simulate = True"
                codeline = "%s = client.submit(execute_task, '%s', %s, [%s], %s, %s)" % (fut_task_varname,
                                                                                         task.wftask.name,
                                                                                         command,
                                                                                         fut_inputs_list,
                                                                                         outputs_list,
                                                                                         simulate_flag)
                noindent_python_codelines.append(codeline)
        #
        for level, task_ids in enumerate(reversed(self.ordered_tasks)):
            noindent_python_codelines.append("# Level %d (%d tasks)" % (level + 1, len(task_ids)))
            for task_id in reversed(task_ids):
                task = self.dag_tasks[task_id]
                noindent_python_codelines.append("# Task %s (%s)" % (task_id, task.wftask.name))
                fut_task_varname = "fut_%s" % task_id
                codeline = "%s.result()" % fut_task_varname
                noindent_python_codelines.append(codeline)
        return noindent_python_codelines
