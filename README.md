# Prestornado

Asynchronous [PrestoDB](https://prestodb.io/) DB-API for [Tornado Web Server](http://tornadoweb.org/)

This is a port based on dropbox's [PyHive](https://github.com/dropbox/PyHive) with `requests` been replaced with tornado's `AsyncHTTPClient`

# Caveat

* Optional PEP-0249 API `next()` has not been implemented yet :(

# Example
```python
from tornado.gen import coroutine
from prestornado import presto
import tornado.ioloop


@coroutine
def run_once():
    cursor = presto.connect('prestodb').cursor()
    yield cursor.execute('SELECT 1 + 1')
    while True:
        ret = yield cursor.poll()
        if not ret:
            break
        print ret['stats']['state']
    ret = yield cursor.fetchall()
    print 'RESULT:', ret

io_loop = tornado.ioloop.IOLoop.instance()
io_loop.run_sync(run_once)
```
