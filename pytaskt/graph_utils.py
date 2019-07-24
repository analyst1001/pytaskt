"""
    Provide access to graph related utility functions in pytaskt
"""

import logging
from typing import Iterable, Callable
import networkx
import pytaskt.task_utils as task_utils
from pytaskt.exceptions import DuplicateTaskException

logging.basicConfig()
LOGGER = logging.getLogger("pytaskt::graph_utils")

def create_graph(task_functions: Iterable[Callable[[], None]]) -> networkx.DiGraph:
    """
        Create a graph representing dependencies amongst all discovered task functions

        Args:
            task_functions : Iterable over functions in which task related data has been populated

        Returns:
            A directed graph representing dependencies amongst all tasks

        Raises:
            DuplicateTaskException : If two or more tasks exist with same task name
    """
    LOGGER.debug("Creating dependency graph for all task function")
    graph: networkx.DiGraph = networkx.DiGraph()
    for func in task_functions:
        task_name: str = task_utils.get_task_name(func)
        LOGGER.debug("Trying to add task %s", task_name)
        if task_name in graph.nodes:
            LOGGER.error("Duplicate task %s found", task_name)
            raise DuplicateTaskException("Duplicate task {task_name} found"
                                         .format(task_name=task_name))
        graph.add_node(task_name, func=func)
    for (task_name, task_data) in graph.nodes.items():
        LOGGER.debug("Adding successors for task %s", task_name)
        func: Callable[[], None] = task_data['func']
        for successor_task_name in task_utils.get_successor_task_names(func):
            LOGGER.debug("Trying to add %s as successor to %s", successor_task_name, task_name)
            if successor_task_name not in graph.nodes:
                LOGGER.warning("Task %s dependent on task %s is missing. This dependency will be "
                               "ignored", successor_task_name, task_name)
            else:
                graph.add_edge(task_name, successor_task_name)
        LOGGER.debug("Adding predecessors for task: %s", task_name)
        for predecessor_task_name in task_utils.get_predecessor_task_names(func):
            LOGGER.debug("Trying to add %s as predecessor to %s", predecessor_task_name, task_name)
            if predecessor_task_name not in graph.nodes:
                LOGGER.warning("Task %s on which task %s depends is missing. This dependency will "
                               "be ignored", predecessor_task_name, task_name)
            else:
                graph.add_edge(predecessor_task_name, task_name)
    return graph
