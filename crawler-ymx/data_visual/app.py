# -*- coding: utf-8 -*-
from flask import Flask, render_template
from data import *

app = Flask(__name__)


@app.route('/')
def index():
    data = KindleData()
    return render_template('index.html', form=data, title=data.title)


@app.route('/kindle')
def kindle():
    data = KindleData()
    return render_template('index.html', form=data, title=data.title)

if __name__ == "__main__":
    app.run(host='127.0.0.1', debug=False)
