"""Presto integration tests.
These rely on having a Presto+Hadoop cluster set up.
They also require a tables created by make_test_tables.sh.
"""
from __future__ import absolute_import
from __future__ import unicode_literals
from tornado import gen
from tornado.concurrent import Future
from tornado.httpclient import HTTPResponse, HTTPRequest, AsyncHTTPClient
from tornado.testing import AsyncTestCase, gen_test
from prestornado.tests.dbapi_test_case import with_cursor, DBAPITestCase
from prestornado import exc
from prestornado import presto
from StringIO import StringIO
import mock

_HOST = 'prestodb'


class TestPresto(AsyncTestCase, DBAPITestCase):

    def connect(self):
        return presto.connect(host=_HOST, source=self.id())

    def run_gen(self, f):
        f()
        return self.wait()

    def setup_fetch(self, fetch_mock, status_code, body=None):
        """Copied from https://groups.google.com/forum/#!topic/python-tornado/LrXqiL6InTM"""
        def side_effect(request, **kwargs):
            if request is not HTTPRequest:
                request = HTTPRequest(request)
            buffer = StringIO(body)
            response = HTTPResponse(request, status_code, None, buffer)
            future = Future()
            future.set_result(response)
            return future
        fetch_mock.side_effect = side_effect

    @with_cursor
    @gen_test
    def test_description(self, cursor):
        yield cursor.execute('SELECT 1 AS foobar FROM one_row')
        # wait to finish
        while (yield cursor.poll()):
            pass
        self.assertEqual(cursor.description, [('foobar', 'bigint', None, None, None, None, True)])

    @with_cursor
    @gen_test
    def test_complex(self, cursor):
        yield cursor.execute('SELECT * FROM one_row_complex')
        # wait to finish
        while (yield cursor.poll()):
            pass
        # TODO Presto drops the union and decimal fields
        self.assertEqual(cursor.description, [
            ('boolean', 'boolean', None, None, None, None, True),
            ('tinyint', 'bigint', None, None, None, None, True),
            ('smallint', 'bigint', None, None, None, None, True),
            ('int', 'bigint', None, None, None, None, True),
            ('bigint', 'bigint', None, None, None, None, True),
            ('float', 'double', None, None, None, None, True),
            ('double', 'double', None, None, None, None, True),
            ('string', 'varchar', None, None, None, None, True),
            ('timestamp', 'timestamp', None, None, None, None, True),
            ('binary', 'varbinary', None, None, None, None, True),
            ('array', 'array<bigint>', None, None, None, None, True),
            ('map', 'map<bigint,bigint>', None, None, None, None, True),
            ('struct', "row<bigint,bigint>('a','b')", None, None, None, None, True),
            #('union', 'varchar', None, None, None, None, True),
            #('decimal', 'double', None, None, None, None, True),
        ])
        data = yield cursor.fetchall()
        self.assertEqual(data, [[
            True,
            127,
            32767,
            2147483647,
            9223372036854775807,
            0.5,
            0.25,
            'a string',
            '1970-01-01 08:00:00.000',
            '123',
            [1, 2],
            {"1": 2, "3": 4},  # Presto converts all keys to strings so that they're valid JSON
            [1, 2],  # struct is returned as a list of elements
            #'{0:1}',
            #0.1,
        ]])

    def test_noops(self):
        """The DB-API specification requires that certain actions exist, even though they might not
        be applicable."""
        # Wohoo inflating coverage stats!
        connection = self.connect()
        cursor = connection.cursor()
        self.assertEqual(cursor.rowcount, -1)
        cursor.setinputsizes([])
        cursor.setoutputsize(1, 'blah')
        connection.commit()

    @mock.patch.object(AsyncHTTPClient, 'fetch')
    def test_non_200(self, fetch):
        self.setup_fetch(fetch, 404, '')
        cursor = self.connect().cursor()

        @gen.engine
        def f():
            yield cursor.execute('show tables')
            self.stop()

        self.assertRaises(exc.OperationalError, self.run_gen, f)

    @with_cursor
    @gen_test
    def test_poll(self, cursor):

        @gen.engine
        def f():
            yield cursor.poll()
            self.stop()

        self.assertRaises(presto.ProgrammingError, self.run_gen, f)

        yield cursor.execute('SELECT * FROM one_row')
        while True:
            status = yield cursor.poll()
            if status is None:
                break
            self.assertIn('stats', status)

        def fail(*args, **kwargs):
            self.fail("Should not need requests.get after done polling")  # pragma: no cover

        with mock.patch.object(AsyncHTTPClient, 'fetch') as fetch:
            fetch.side_effect = fail
            self.assertEqual((yield cursor.fetchall()), [[1]])
