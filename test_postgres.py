import unittest

import psycopg2

import test_dialects

def get_connection():
    props = "dbname=esdb_test user=esdb_test password=esdb_test"
    return psycopg2.connect(props)


def drop_table_people(db, cursor):
    try:
        cursor.execute("DROP TABLE people")
        db.commit()
    except Exception, ex:
        print ex


class PostgresFunctionTests(test_dialects.FunctionTests):

    def get_dialect_connection(self):
        return get_connection()

    def drop_table_people(self, db, cursor):
        return drop_table_people(db, cursor)


class PostgresMethodTests(test_dialects.MethodTests):

    def get_dialect_connection(self):
        return get_connection()

    def drop_table_people(self, db, cursor):
        return drop_table_people(db, cursor)


class PostgresMethodTableTests(test_dialects.MethodTableTests):

    def get_dialect_connection(self):
        return get_connection()

    def drop_table_people(self, db, cursor):
        return drop_table_people(db, cursor)


if __name__ == '__main__':
    unittest.main()
