"""
    Module to provide access to @task decorator
"""
from functools import wraps
from collections import deque
import logging
from concurrent.futures import ThreadPoolExecutor, Future, wait, FIRST_COMPLETED
from typing import Iterable, Callable, Set, Union, Dict, types, Iterator
import networkx
from pytaskt.task_utils import get_task_functions
from pytaskt.graph_utils import create_graph
from pytaskt.exceptions import (StartTaskMissingException,
                                IncorrectTaskDependencies)
from pytaskt.task_constants import (FIRST_TASK,
                                    LAST_TASK,
                                    BEFORE_KEY,
                                    AFTER_KEY,
                                    NAME_KEY)

logging.basicConfig()
LOGGER = logging.getLogger("pytaskt::task")

def task(name: str,
         before: Union[None, Iterable[str]] = None,
         after: Union[None, Iterable[str]] = None) -> Callable[[Callable[[], None]], Callable[[], None]]:
    """
        Decorator wrapper to take custom task arguments, and return task decorator

        Args:
            name : Name to be assigned to the task
            before : Iterable over task names to be performed before this task
            after : Iterable over task names to be performed after this task

        Returns:
            Wrapped function populated with task related data
    """
    LOGGER.debug("Creating task %s to run before %s and after %s", name, before, after)
    before = before or []
    after = after or []
    before_tasks: Set[str] = set(before)
    if name != LAST_TASK:
        before_tasks.add(LAST_TASK)
    after_tasks: Set[str] = set(after)
    if name != FIRST_TASK:
        after_tasks.add(FIRST_TASK)
    def fn_wrapper(func: Callable[[], None]) -> Callable[[], None]:
        """
            Task decorator to set custom arguments in functions to be marked as tasks

            Args:
                func : Function to decorate for a task

            Returns:
                Decorated function
        """
        func.__task__: Dict[str, Union[str, Set[str]]] = {}
        func.__task__[NAME_KEY] = name
        func.__task__[BEFORE_KEY] = before_tasks
        func.__task__[AFTER_KEY] = after_tasks
        @wraps(func)
        def fn_orig() -> None:
            """
                Wrapper around function decorated as task. Handles exceptions properly.
            """
            try:
                LOGGER.debug("Executing original function %s decorated as task", func.__name__)
                func()
            except Exception as ex:
                LOGGER.error("Exception occurred inside a task. Message: %s", ex)
        return fn_orig
    return fn_wrapper


def _run_task_funcs(task_graph: networkx.DiGraph, max_threads: int) -> None:
    """
        Run functions associated with tasks, whilst satisfying their dependencies.

        Args:
            task_graph: Directed graph representing dependencies amongst tasks
            max_threads: Maximum number of threads to use at a given time to run tasks

        Raises:
            StartTaskMissingException: First task is missing from set of tasks to run
    """
    LOGGER.debug("Trying to run all tasks")
    if FIRST_TASK not in task_graph.nodes:
        LOGGER.error("Start task should be present in task dependencies graph")
        raise StartTaskMissingException("Start task should always be present")
    indegree_map: Dict[str, int] = {v: d for v, d in task_graph.in_degree() if d > 0}
    start_node: str = FIRST_TASK
    task_queue: deque = deque()
    task_queue.append(start_node)
    futures_task_names: Dict[Future, str] = {}
    with ThreadPoolExecutor(max_workers=max_threads) as thread_pool_executor:
        while task_queue or futures_task_names:
            new_future_tasks = {
                thread_pool_executor.submit(task_graph.nodes()[task_name]['func']): task_name \
                        for task_name in task_queue}
            task_queue.clear()
            futures_task_names.update(new_future_tasks)
            (completed_future, _) = wait(futures_task_names, return_when=FIRST_COMPLETED)
            completed_task_name: str = futures_task_names.pop(completed_future.pop())
            for _, successor in task_graph.edges(completed_task_name):
                indegree_map[successor] -= 1
                if indegree_map[successor] is 0:
                    LOGGER.debug("Task %s is now available for execution", successor)
                    task_queue.append(successor)
                    del indegree_map[successor]
        LOGGER.debug("Completed all tasks")

def check_tasks_dependencies(task_graph: networkx.DiGraph) -> bool:
    """
        Check if there exists a cycle in dependencies amongst tasks

        Args:
            task_graph: Directed graph representing dependencies amongst tasks

        Returns:
            True, if there is any cyclic dependency amongst tasks. False otherwise
    """
    LOGGER.debug("Checking task graph for any dependency issues")
    cycle_generator = networkx.algorithms.cycles.simple_cycles(task_graph)
    return next(cycle_generator, None) is None

def run_tasks(module: types.ModuleType, max_threads: int = 1):
    """
        Run all tasks inside a module, and all its submodules, whilst maintaining dependencies

        Args:
            module: Module under which all tasks to run are present
            max_threads: Maximum number of threads allowed to run tasks at a given time

        Raises:
            IncorrectTaskDependencies: Raised if any issue is found in dependencies amongst tasks
    """
    LOGGER.debug("Trying to collect all tasks and run them")
    task_functions: Iterator[Callable[[], None]] = get_task_functions(module)
    task_graph: networkx.DiGraph = create_graph(task_functions)
    if check_tasks_dependencies(task_graph):
        _run_task_funcs(task_graph, max_threads)
    else:
        LOGGER.error("Issues found in dependencies amongst tasks")
        raise IncorrectTaskDependencies("Issues found in dependencies amongst tasks")
