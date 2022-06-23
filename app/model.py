
def nav(active=None):
    model = {
        'title': 'Backup Scheduler',
        'routes': [
            {
                'title': 'Watchers',
                'url': '/watchers',
            },
            {
                'title': 'Log',
                'url': '/log',
            },
        ]
    }
    for route in model['routes']:
        route['active'] = route['url'] == active
    return model

def backups():
    return [
        {
            'path': '/test/abc/1',
            'triggered_by': 'idle_timeout',
            'scheduled_at': '',
        }
    ]