# encoding: utf-8
"""Shared DB-API test cases"""

from __future__ import absolute_import
from __future__ import unicode_literals
from prestornado import exc
from tornado.testing import gen_test
from tornado import gen
import abc
import contextlib
import functools


def with_cursor(fn):
    """Pass a cursor to the given function and handle cleanup.
    The cursor is taken from ``self.connect()``.
    """
    @functools.wraps(fn)
    def wrapped_fn(self, *args, **kwargs):
        with contextlib.closing(self.connect()) as connection:
            with contextlib.closing(connection.cursor()) as cursor:
                fn(self, cursor, *args, **kwargs)
    return wrapped_fn


class DBAPITestCase(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def connect(self):
        raise NotImplementedError  # pragma: no cover

    @with_cursor
    @gen_test
    def test_fetchone(self, cursor):
        yield cursor.execute('SELECT * FROM one_row')
        self.assertEqual(cursor.rownumber, 0)
        self.assertEqual((yield cursor.fetchone()), [1])
        self.assertEqual(cursor.rownumber, 1)
        self.assertIsNone((yield cursor.fetchone()))

    @with_cursor
    @gen_test
    def test_fetchall(self, cursor):
        yield cursor.execute('SELECT * FROM one_row')
        self.assertEqual((yield cursor.fetchall()), [[1]])
        yield cursor.execute('SELECT a FROM many_rows ORDER BY a')
        self.assertEqual((yield cursor.fetchall()), [[i] for i in xrange(10000)])

    @with_cursor
    def test_description_initial(self, cursor):
        self.assertIsNone(cursor.description)

    @with_cursor
    @gen_test
    def test_description_failed(self, cursor):
        try:
            yield cursor.execute('blah_blah')
        except exc.DatabaseError:
            pass
        self.assertIsNone(cursor.description)

    @with_cursor
    def test_bad_query(self, cursor):
        @gen.engine
        def run():
            yield cursor.execute('SELECT does_not_exist FROM this_really_does_not_exist')
            yield cursor.fetchone()
        self.assertRaises(exc.DatabaseError, self.run_gen, run)

    @with_cursor
    @gen_test
    def test_concurrent_execution(self, cursor):
        yield cursor.execute('SELECT * FROM one_row')
        yield cursor.execute('SELECT * FROM one_row')
        self.assertEqual((yield cursor.fetchall()), [[1]])

    @with_cursor
    @gen_test
    def test_executemany(self, cursor):
        for length in 1, 2:
            yield cursor.executemany(
                'SELECT %(x)d FROM one_row',
                [{'x': i} for i in xrange(1, length + 1)]
            )
            self.assertEqual((yield cursor.fetchall()), [[length]])

    @with_cursor
    @gen_test
    def test_executemany_none(self, cursor):

        @gen.engine
        def g():
            yield cursor.fetchone()

        yield cursor.executemany('should_never_get_used', [])
        self.assertIsNone(cursor.description)
        self.assertRaises(exc.ProgrammingError, self.run_gen, g)

    @with_cursor
    def test_fetchone_no_data(self, cursor):

        @gen.engine
        def f():
            yield cursor.fetchone()

        self.assertRaises(exc.ProgrammingError, self.run_gen, f)

    @with_cursor
    @gen_test
    def test_fetchmany(self, cursor):
        yield cursor.execute('SELECT * FROM many_rows LIMIT 15')
        self.assertEqual((yield cursor.fetchmany(0)), [])
        self.assertEqual(len((yield cursor.fetchmany(10))), 10)
        self.assertEqual(len((yield cursor.fetchmany(10))), 5)

    @with_cursor
    @gen_test
    def test_arraysize(self, cursor):
        cursor.arraysize = 5
        yield cursor.execute('SELECT * FROM many_rows LIMIT 20')
        self.assertEqual(len((yield cursor.fetchmany())), 5)

    @with_cursor
    @gen_test
    def test_polling_loop(self, cursor):
        """Try to trigger the polling logic in fetchone()"""
        cursor._poll_interval = 0
        yield cursor.execute('SELECT COUNT(*) FROM many_rows')
        self.assertEqual((yield cursor.fetchone()), [10000])

    @with_cursor
    @gen_test
    def test_no_params(self, cursor):
        yield cursor.execute("SELECT '%(x)s' FROM one_row")
        self.assertEqual((yield cursor.fetchall()), [['%(x)s']])

    def test_escape(self):
        """Verify that funny characters can be escaped as strings and SELECTed back"""
        bad_str = '''`~!@#$%^&*()_+-={}[]|\\;:'",./<>?\n\r\t '''
        self.run_escape_case(bad_str)

    @with_cursor
    @gen_test
    def run_escape_case(self, cursor, bad_str):
        yield cursor.execute(
            'SELECT %d, %s FROM one_row',
            (1, bad_str)
        )
        self.assertEqual((yield cursor.fetchall()), [[1, bad_str]])
        yield cursor.execute(
            'SELECT %(a)d, %(b)s FROM one_row',
            {'a': 1, 'b': bad_str}
        )
        self.assertEqual((yield cursor.fetchall()), [[1, bad_str]])

    @with_cursor
    @gen_test
    def test_invalid_params(self, cursor):

        @gen.engine
        def f1():
            yield cursor.execute('', 'hi')

        @gen.engine
        def f2():
            yield cursor.execute('', [{}])

        self.assertRaises(exc.ProgrammingError, self.run_gen, f1)
        self.assertRaises(exc.ProgrammingError, self.run_gen, f2)

    def test_open_close(self):
        with contextlib.closing(self.connect()):
            pass
        with contextlib.closing(self.connect()) as connection:
            with contextlib.closing(connection.cursor()):
                pass

    @with_cursor
    @gen_test
    def test_unicode(self, cursor):
        unicode_str = "王兢"
        yield cursor.execute(
            'SELECT %s FROM one_row',
            (unicode_str,)
        )
        self.assertEqual((yield cursor.fetchall()), [[unicode_str]])
