"""
    Simple threadpool (like) implementation for runnigs tasks as threads
"""
import queue
import logging
from threading import Thread
from typing import Callable

logging.basicConfig()
LOGGER = logging.getLogger("pytaskt::threadpool")

class TaskThread(Thread):
    """
        Derived thread class to allow running function inside threads
    """

    def __init__(self, name: str, pool_thread_queue: queue.Queue) -> None:
        """
            Initialize a special thread child object for purpose of running tasks

            Args:
                name: Name to be given to the underlying thread object
                pool_thread_queue: Queue to which this thread belongs to
        """
        LOGGER.debug("Initializing thread %s", name)
        super(TaskThread, self).__init__(name=name)
        self._task_func: Callable[[], None] = lambda: None
        self._pool_thread_queue: queue.Queue = pool_thread_queue

    def set_task_data(self, task_func: Callable[[], None]) -> None:
        """
            Assign a function to execute when the thread object runs

            Args:
                task_func: Function decorated as task to execute when this thread runs
        """
        LOGGER.debug("Assigning task function %s to thread %s", task_func.__name__, self.getName())
        self._task_func = task_func

    def run(self) -> None:
        """
            Run the underlying thread associated with this object by executing a task function
            inside it

        """
        LOGGER.debug("Running task function %s inside thread %s", self._task_func.__name__,
                     self.getName())
        print("Running task %s inside thread %s" % (self._task_func.__name__, self.getName()))
        try:
            self._task_func()
            print("Done running task thread %s" % self.getName())
        finally:
            self._pool_thread_queue.put(TaskThread(self.name, self._pool_thread_queue))
            self._pool_thread_queue.task_done()

class ThreadPool(object):
    """
        Provides a pool of threads to use for running functions
    """

    def __init__(self, max_threads: int) -> None:
        """
            Initialize a threadpool with a maximum limit of number of threads

            Args:
                max_threads: Maximum number of threads allowed to run at a given time
        """
        LOGGER.debug("Initializing a threadpool with maximum %s threads", max_threads)
        self._thread_queue: queue.Queue = queue.Queue(maxsize=max_threads)
        for i in range(max_threads):
            thread: TaskThread = TaskThread("Thread {index}".format(index=i), self._thread_queue)
            self._thread_queue.put(thread)

    def execute_task(self, task_func: Callable[[], None]) -> None:
        """
            Run a task function inside a thread

            Args:
                task_func: Function decorated as a task, to run inside a thread
        """
        LOGGER.debug("Trying to execute task function %s inside a thread", task_func.__name__)
        task_thread: TaskThread = self._get_thread()
        task_thread.set_task_data(task_func)
        task_thread.start()

    def _get_thread(self) -> TaskThread:
        """
            Get a thread from threadpool, if available. Otherwise wait until one is available
        """
        LOGGER.debug("Trying to retrieve an available thread from threadpool")
        return self._thread_queue.get()
