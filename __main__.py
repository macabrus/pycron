import os
import yaml
import re
import itertools
import json
import asyncio as aio

from dateutil import parser as date_parser
from dataclasses import dataclass
from functools import partial
from croniter import croniter
from datetime import datetime, timedelta

# policies for anomalies
# (written in first column of config)
# e.g. @policy[retry=always,overlap=skip,missed=4]
# missed=n => runs up to n missed runs
# retry=[once|always]
# overlap=[skip|after|ignore]
# missed=[all|once|n|ignore]


def now():
    return datetime.now() #+ timedelta(minutes=5)

async def run(cmd):
    '''
    runs command
    '''
    proc = await aio.create_subprocess_shell(
        cmd,
        stdout=aio.subprocess.PIPE,
        stderr=aio.subprocess.PIPE
    )
    print(f'[START {proc.pid}] CMD {cmd}')
    stdout, stderr = await proc.communicate()
    if stdout:
        print(f'[STDOUT {proc.pid}]\n{stdout.decode()}')
    if stderr:
        print(f'[STDERR {proc.pid}]\n{stderr.decode()}')
    print(
        f'[FINISH {proc.pid}] Process exited with return code {proc.returncode}')
    return proc.returncode


def delta(start, end):
    '''
    computes seconds between two datetimes
    '''
    return (end - start).total_seconds()


def next_task(timers, from_timestamp):
    '''
    computes schedule order
    '''
    schedule = []
    for timer in timers:
        then = croniter(timer[0], from_timestamp).get_next(datetime)
        schedule.append((delta(from_timestamp, then), *timer))
    return map(lambda x: x[1:], sorted(schedule))


def set_checkpoint(id, dt=None):
    '''
    ensures checkpoint on disk
    '''
    with open(f'checkpoint/{id}', 'w') as f:
        dt = dt or now()
        f.write(dt.isoformat())
        os.fsync(f.fileno())
    return dt


def get_checkpoint(id, fallback=None):
    '''
    reads last written checkpoint,
    if none exists, falls back to provided
    or current time
    '''
    path = f'checkpoint/{id}'
    if not os.path.isfile(path):
        dt = fallback or now()
        set_checkpoint(id, dt)
        return dt
    with open(path, 'r') as f:
        return date_parser.parse(f.read().strip())


def resolve_diff(id, cron):
    '''
    computes diff in seconds
    '''
    checkpoint = get_checkpoint(id)
    it = croniter(cron, checkpoint)
    next_run = it.get_next(datetime)
    return delta(checkpoint, next_run)


def generate_missed(checkpoint, from_timestamp, cron):
    '''
    generates all missed runs up until "now"
    '''
    it = croniter(cron, checkpoint)
    next_run = it.get_next(datetime)
    while next_run < from_timestamp:
        if checkpoint < next_run:
            yield next_run
        next_run = it.get_next(datetime)


@dataclass
class Task:
    id: int
    cron: str
    command: str
    retry: str = 0
    overlap: str = 'skip'
    missed: int = 'ignore'

def val_map_rules(pair):
    rules = {
        'retry': int
    }
    return (pair[0], rules[pair[0]](pair[1])) if pair[0] in rules else tuple(pair)

def parse_policy(line: str) -> dict:
    # default policies
    policies = {
        'retry': 0,
        'overlap': 'skip',
        'missed': 'ignore'
    }
    pairs = re.search('@policy\\((.*)\\)', line).groups()[0]
    filtered_pairs = filter(lambda x: x, pairs.split(','))
    mapped_pairs = map(lambda x: x.split('='), filtered_pairs)
    typed_tuples = map(val_map_rules, mapped_pairs)
    return policies | {x[0]: x[1] for x in typed_tuples}


def parse_task(index: int, line: str) -> Task:
    regex = '(\s*[^\s]+\s+[^\s]+\s+[^\s]+\s+[^\s]+\s+[^\s]+)\s+([^\s]+)\s+(.*)'
    res = re.search(regex, line)
    cron, policy_part, command = res.groups()
    policies = parse_policy(policy_part)
    return Task(
        index,
        cron,
        command,
        policies['retry'],
        policies['overlap'],
        policies['missed']
    )


async def handle_execution(task: Task):
    '''
    handles retries for a task
    '''
    result = -1
    for retry in range(task.retry + 1):
        result = await run(task.command)
        if result == 0:
            break
        else:
            print(f'[ERROR] CMD {task.command!r} exited with status {result}.')
    if result != 0:
        print(
            f'[ERROR] CMD {task.command!r} canceled after {task.retry + 1} unsuccessful attempts.')
    return result


async def handle_task(task: Task):
    '''
    handles single recurring task (keeping its checkpoint)
    '''
    checkpoint = get_checkpoint(task.id)
    misses = [miss for miss in generate_missed(checkpoint, now(), task.cron)]
    if misses:
        print(f"[INFO] Missed CMD {len(misses)} times.")
    for miss in misses:
        # if fire & forget then dont await
        set_checkpoint(task.id, now())
        await handle_execution(task)
        # if policy is to run missed scripts only once
        break # TODO: honor other missed policies
    while True:
        checkpoint = get_checkpoint(task.id)
        it = croniter(task.cron, checkpoint)
        next_run = it.get_next(datetime)
        diff = delta(checkpoint, next_run)
        print(f'[INFO] CMD {task.command!r} is scheduled to run at {next_run} (in {diff} seconds)')
        if diff > 0:
            await aio.sleep(diff)
        set_checkpoint(task.id, next_run)
        # if we should fire and forget, dont await... just run another tasks
        await handle_execution(task)


async def gather(tasks):
    '''
    gathers async concurrent tasks
    '''
    for future in aio.as_completed(tasks):
        res = await future
        print(res)


def main():
    '''
    parse main file and run loop
    '''
    tasks = []
    config = read_config('schedule.yaml')
    for index, line in enumerate(conf):
        if not line.strip():
            continue
        task = parse_task(index, line)
        #print(task)
        tasks.append(handle_task(task))
    aio.run(gather(tasks))

from fswatch import Monitor
def register_config_listener(path):
    monitor = Monitor()
    monitor.add_path(path)
    def callback(path, evt_time, flags, flags_num, event_num):
        print(path.decode())
        print(flags.decode())
    monitor.set_callback(callback)
    monitor.start()


# reading config file
def read_config():
    with open("schedule.yaml", "r") as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)

if __name__ == '__main__':
    #register_config_listener('./test/')
    #print(json.dumps(read_config(), indent=4))
    # exit(0)
    main()

