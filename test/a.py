from pytaskt.task import task

@task(name='first_task')
def g():
    print("First task")

@task(name="hello", before=["a"], after=["b"])
def f() -> None:
    print("Hello World")

@task(name="last_task")
def h():
    print("Last task")
