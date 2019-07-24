"""
   Provide utility functions related to task functions in pytaskt
"""

import logging
from typing import Iterator, Callable, types
from pytaskt.pymodule_utils import get_all_functions
from pytaskt.task_constants import (TASK_MARKER_ATTRNAME,
                                    NAME_KEY,
                                    BEFORE_KEY,
                                    AFTER_KEY)

logging.basicConfig()
LOGGER = logging.getLogger("pytaskt::task_utils")

def get_task_functions(module: types.ModuleType) -> Iterator[Callable[[], None]]:
    """
        Retrieve all functions decorated as tasks from a module and it submodules

        Args:
            module: Module inside which task function are searched, including its submodules

        Retuns:
            Iterator over task functions decorated as tasks
    """
    LOGGER.debug("Trying to retrieve task functions from module %s and its submodules",
                 module.__name__)
    yield from filter(lambda f: hasattr(f, TASK_MARKER_ATTRNAME), get_all_functions(module))

def get_task_name(task_function: Callable[[], None]) -> str:
    """
        Retrieve the name associated with a function which has been decorated as a task

        Args:
            task_function: Function decorated as a task

        Returns:
            Name of the task associated with the function
    """
    LOGGER.debug("Trying to retrieve name of task from task decorated function %s",
                 task_function.__name__)
    return task_function.__task__[NAME_KEY]

def get_successor_task_names(task_function: Callable[[], None]) -> Iterator[str]:
    """
        Retrieve names of tasks to run after a given function which has been decorated as a task

        Args:
            task_function: Function decorated as a task

        Returns:
            Iterator over name of tasks to run after task associated with the given function is
                complet:ed
    """
    LOGGER.debug("Trying to retrieve names of tasks to run after task decorated function %s",
                 task_function.__name__)
    return task_function.__task__[BEFORE_KEY]

def get_predecessor_task_names(task_function: Callable[[], None]) -> Iterator[str]:
    """
        Retrieve names of tasks to run before a given function which as been decorated as a task

        Args:
            task_function: Function decorated as a task

        Returns:
            Iterator over name of tasks to run before task associated with the given function is
                started
    """
    LOGGER.debug("Trying to retrieve names of tasks to run before task decorated function %s",
                 task_function.__name__)
    return task_function.__task__[AFTER_KEY]
