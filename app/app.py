from db import get_watchers, add_watcher, delete_watcher, init_db
from flask import Flask, render_template, redirect, request
from model import nav

app = Flask(
    __name__,
    static_url_path='',
    static_folder='static',
    template_folder='template'
)

init_db()

@app.route("/watchers")
def watchers():
    return render_template(
        'backups.html',
        **nav(active='/watchers'),
        schedules=get_watchers()
    )

@app.route("/log")
def log():
    return render_template('log.html', **nav(active='/log'))

@app.route("/watcher_form")
def watcher_form():
    return render_template('backup_form.html', **nav())
    
@app.route('/create_watcher', methods=['POST'])
def create_watcher():
    form = request.form
    name = form['title']
    path = form['path']
    fs_timeout = form['fs_timeout']
    add_watcher(name, path, fs_timeout)
    return redirect('/watchers')

@app.route('/remove/<int:id>', methods=['POST'])
def remove_schedule(id):
    delete_watcher(id)
    return redirect('/watchers')

@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def match_all(path):
    return redirect('/watchers')
    
