import unittest
import sqlite3

import test_dialects


def get_connection():
    return sqlite3.connect(":memory:")


class SQLite3FunctionTests(test_dialects.FunctionTests):

    def get_dialect_connection(self):
        return get_connection()


class SQLite3MethodTests(test_dialects.MethodTests):

    def get_dialect_connection(self):
        return get_connection()


class SQLite3MethodTableTests(test_dialects.MethodTableTests):

    def get_dialect_connection(self):
        return get_connection()


if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
