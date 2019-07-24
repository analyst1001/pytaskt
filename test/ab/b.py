from pytaskt.task import task
import time

@task(name="a")
def a():
    time.sleep(5)
    print("Executing a at {}".format(time.time()))

@task(name="b")
def b():
    time.sleep(10)
    print("Executing b at {}".format(time.time()))

@task(name="c")
def c():
    time.sleep(20)
    print("Executing c at {}".format(time.time()))

@task(name="d")
def d():
    time.sleep(10)
    print("Executing d at {}".format(time.time()))

@task(name="e")
def e():
    time.sleep(10)
    print("Executing e at {}".format(time.time()))

@task(name="f")
def f():
    time.sleep(20)
    print("Executing f at {}".format(time.time()))

@task(name="g")
def g():
    time.sleep(20)
    print("Executing g at {}".format(time.time()))

@task(name="h")
def h():
    time.sleep(10)
    print("Executing h at {}".format(time.time()))

@task(name="i")
def i():
    time.sleep(20)
    print("Executing i at {}".format(time.time()))
