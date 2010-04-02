import unittest

import MySQLdb

import test_dialects


def get_connection():
    return MySQLdb.connect(host="localhost",
                           user="esdb_test",
                           passwd="esdb_test",
                           db="esdb_test")


def drop_table_people(db, cursor):
    try:
        cursor.execute("DROP TABLE people")
        db.commit()
    except Exception:
        pass


class MySQLFunctionTests(test_dialects.FunctionTests):

    def get_dialect_connection(self):
        return get_connection()

    def drop_table_people(self, db, cursor):
        return drop_table_people(db, cursor)


class MySQLMethodTests(test_dialects.MethodTests):

    def get_dialect_connection(self):
        return get_connection()

    def drop_table_people(self, db, cursor):
        return drop_table_people(db, cursor)


class MySQLMethodTableTests(test_dialects.MethodTableTests):

    def get_dialect_connection(self):
        return get_connection()

    def drop_table_people(self, db, cursor):
        return drop_table_people(db, cursor)


if __name__ == '__main__':
    unittest.main()
