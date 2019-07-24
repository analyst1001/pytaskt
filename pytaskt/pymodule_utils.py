"""
    Provide python objects specific functionality for pytaskt
"""

import importlib
import pkgutil
import inspect
import itertools
import logging
from typing import Iterator, List, Callable, Tuple, types

logging.basicConfig()
LOGGER = logging.getLogger("pytaskt::pymodule_utils")

def get_submodules(module: types.ModuleType) -> Iterator[types.ModuleType]:
    """
        Recursively retrieve all submodules including and inside a given module

        Args:
            module: Module under which submodules are to be searched

        Returns:
            Iterator over all submodules inside the module found by recursive traversal,
                and the module itself
    """
    LOGGER.debug("Trying to get all submodules for module %s", module.__name__)
    yield from map(lambda mod_info: importlib.import_module(mod_info.name),
                   pkgutil.walk_packages(module.__path__, prefix="{}.".format(module.__name__)))

def get_all_functions(module: types.ModuleType) -> Iterator[Callable]:
    """
        Retrieve all functions defined inside a module, including all it submodules

        Args:
            module: Module inside which functions are to be searched

        Returns:
            Iterator over all functions inside the module, and all its submodules
    """
    LOGGER.debug("Trying to get all functions inside module %s and it submodules", module.__name__)
    all_members: Iterator[List[Tuple[str, Callable]]] = map(
        lambda mod: inspect.getmembers(mod, inspect.isfunction),
        get_submodules(module))
    all_functions: Iterator[List[Tuple[str, Callable]]] = filter(lambda x: len(x) > 1, all_members)
    yield from map(lambda func_info: func_info[1], itertools.chain(*all_functions))
