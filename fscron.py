from fswatch import Monitor

# this works...
# combined with python cron manager
# we could make a beautiful backup solution that does backups every few minutes if files were actually changed
# and then, eventually, be it daily or weekly, does a full backup to pick up missed files (if any)


monitor = Monitor()
monitor.add_path("./test/")


def callback(path, evt_time, flags, flags_num, event_num):
    print(path.decode())
    print(flags.decode())


monitor.set_callback(callback)
monitor.start()
