"""
    Provides exception classes for pytaskt
"""

class PyTaskTException(Exception):
    """
        Base class for all custom pytaskt exceptions
    """
    pass

class DuplicateTaskException(PyTaskTException):
    """
        Exception raised when a duplicate task is found
    """
    pass

class StartTaskMissingException(PyTaskTException):
    """
        Exception raised when a first task is missing from set of tasks to run
    """
    pass

class IncorrectTaskDependencies(PyTaskTException):
    """
        Exception raised when there is some issue in dependencies amongst tasks
    """
    pass
