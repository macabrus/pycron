from throttle import throttle
from db import get_watchers
from queue import PriorityQueue
from db import get_tasks
from fswatch import Monitor


tasks = []

monitor = Monitor()
monitor.add_path("./test/")


def callback(path, evt_time, flags, flags_num, event_num):
    print(path.decode())
    print(flags.decode())


monitor.set_callback(callback)
monitor.start()



def add_task(id):
    ...


def clear_task(id):
    ...

def reload():
    map(lambda t: (t['scheduled_at']), get_tasks())
    tasks = PriorityQueue()
    for task in tasks:
        ...
    return 