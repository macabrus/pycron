import os
import re
import asyncio as aio

from dateutil import parser as date_parser
from dataclasses import dataclass
from functools import partial
from croniter import croniter
from datetime import datetime

# policies for anomalies
# (written in first column of config)
# e.g. @policy[retry=always,overlap=skip,missed=4]
# missed=n => runs up to n missed runs 
# retry=[once|always]
# overlap=[skip|after|ignore]
# missed=[all|once|n|ignore]

# runs command
async def run(cmd):
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
    print(f'[FINISH {proc.pid}] Process exited with return code {proc.returncode}')
    return proc.returncode

# computes seconds between two datetimes
def delta(start, end):
    return (end - start).total_seconds()

# computes schedule order 
def next_task(timers, now):
    schedule = []
    for timer in timers:
        then = croniter(timer[0], now).get_next(datetime)
        schedule.append((delta(now, then), *timer))
    return map(lambda x: x[1:], sorted(schedule))

# ensures checkpoint on disk
def set_checkpoint(id, dt=None):
    with open(f'checkpoint/{id}', 'w') as f:
        dt = dt or datetime.now()
        f.write(dt.isoformat())
        os.fsync(f.fileno())
    return dt

# reads last written checkpoint,
# if none exists, falls back to provided
# or current time
def get_checkpoint(id, fallback=None):
    path = f'checkpoint/{id}'
    if not os.path.isfile(path):
        dt = fallback or datetime.now()
        set_checkpoint(id, dt)
        return dt
    with open(path, 'r') as f:
        return date_parser.parse(f.read().strip())

# computes diff in seconds
def resolve_diff(id, cron):
    checkpoint = get_checkpoint(id)
    it = croniter(cron, checkpoint)
    next_run = it.get_next(datetime)
    return delta(checkpoint, next_run)

# generates all missed runs up until "now"
def generate_missed(checkpoint, now, cron):
    it = croniter(cron, checkpoint)
    next_run = it.get_next(datetime)
    while next_run < now:
        if checkpoint < next_run:
            yield next_run
        next_run = it.get_next(datetime)

@dataclass
class Task:
    id: int
    cron: str
    command: str
    retry: str = 'never'
    overlap: str = 'ignore'
    max_retries: int = 2

# handles retries for a task
async def handle_execution(command, retry=0):
    max_retries = 3
    result = 1
    while retry <= max_retries:
        retry += 1
        result = await run(command)
        if result == 0:
            break
    if result != 0:
        print(f'[ERROR] Missed CMD {command!r} canceled after {max_retries} unsuccessful retries.')
    return result

# handles single recurring task (keeping its checkpoint)
async def handle_task(task):
    id, cron, command = task
    checkpoint = get_checkpoint(id)
    now = datetime.now()
    for miss in generate_missed(checkpoint, now, cron):
        print(f"[INFO] Missed CMD {command!r} at {miss}")
        # if fire & forget then dont await
        await handle_execution(command)
        set_checkpoint(id, miss)
        # if policy is to run missed scripts only once
    while True:
        checkpoint = get_checkpoint(id)
        it = croniter(cron, checkpoint)
        next_run = it.get_next(datetime)
        diff = delta(checkpoint, next_run)
        if diff > 0:
            await aio.sleep(diff)
        set_checkpoint(id, next_run)
        # if we should fire and forget, dont await... just run another tasks
        await handle_execution(command)

# gathers async concurrent tasks
async def gather(tasks):
    for future in aio.as_completed(tasks):
        res = await future
        print(res)

# parse main file and run loop
def main():
    tasks = []
    with open('schedule', 'r') as conf:
        for id, line in enumerate(conf.readlines()):
            if not line.strip():
                continue
            res = re.search('(\s*[^\s]+\s+[^\s]+\s+[^\s]+\s+[^\s]+\s+[^\s]+\s+)(.*)', line)
            cron, command = res.groups()
            tasks.append(handle_task((id, cron, command)))
    aio.run(gather(tasks))

if __name__ == '__main__':
    main()

