import sqlite3

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

# connection factory
def conn():
    c = sqlite3.connect('app.db')
    c.row_factory = dict_factory
    return c

# init db if not initialized
def init_db():
    with conn() as db:
        db.execute('''
        CREATE TABLE IF NOT EXISTS watcher (
            id INTEGER PRIMARY KEY,
            name DATETIME,
            path INTEGER,
            fs_timeout VARCHAR(64)
        )''')
        db.execute('''
        CREATE TABLE IF NOT EXISTS log (
            watcher_id INTEGER,
            executed_at INTEGER,
            duration REAL,
            successful BOOLEAN,
            FOREIGN KEY (watcher_id) REFERENCES watcher(id)
        )''')
        db.execute('''
        CREATE TABLE IF NOT EXISTS task (
            watcher_id INTEGER,
            scheduled_for DATETIME,
            FOREIGN KEY (watcher_id) REFERENCES watcher(id)
        )''')


# backup path funcs

def get_watchers():
    with conn() as db:
        return list(db.execute('SELECT * FROM watcher'))

def add_watcher(name, path, fs_timeout):
    with conn() as db:
        row = (name, path, fs_timeout)
        db.execute(
            'INSERT INTO watcher(name, path, fs_timeout) '
            'VALUES (?, ?, ?)', row
        )

def delete_watcher(id):
    with conn() as db:
        return db.execute('DELETE FROM watcher WHERE id = ?', (id,))

# getting logs
def get_logs():
    with conn() as db:
        return db.execute('SELECT * FROM log ORDER BY executed_at DESC')

def log_execution(schedule_id, executed_at, duration, successful):
    with conn() as db:
        return db.execute('INSERT INTO log(watcher_id, executed_at) VALUES (?, ?)')


# add next run
# when change already occured and was detected

def add_task(schedule):
    with conn() as db:
        row = (schedule['id'], schedule['fs_timeout'])
        db.execute('INSERT INTO task(schedule_id, run_at) VALUES (?, ?)', )

def update_task(task):
    ...

def remove_task():
    ...