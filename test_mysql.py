import unittest

import MySQLdb

import test_dialects


def get_connection():
    conn = MySQLdb.connect(host="localhost",
                           user="esdb_test",
                           passwd="esdb_test",
                           db="esdb_test")
    cursor = conn.cursor()
    cursor.execute("SET AUTOCOMMIT=0")
    return conn


def drop_table_people(db, cursor):
    try:
        cursor.execute("DROP TABLE people")
        db.commit()
    except Exception:
        pass


def get_create_people_table_ddl():
    return """CREATE TABLE people (
                           id INTEGER PRIMARY KEY,
                           first_name VARCHAR(255) NOT NULL,
                           last_name VARCHAR(255) NOT NULL
              ) ENGINE=InnoDB"""


class MySQLFunctionTests(test_dialects.FunctionTests):

    def get_dialect_connection(self):
        return get_connection()

    def drop_table_people(self, db, cursor):
        return drop_table_people(db, cursor)

    def get_create_people_table_ddl(self):
        return get_create_people_table_ddl()


class MySQLMethodTests(test_dialects.MethodTests):

    def get_dialect_connection(self):
        return get_connection()

    def drop_table_people(self, db, cursor):
        return drop_table_people(db, cursor)

    def get_create_people_table_ddl(self):
        return get_create_people_table_ddl()


class MySQLMethodTableTests(test_dialects.MethodTableTests):

    def get_dialect_connection(self):
        return get_connection()

    def drop_table_people(self, db, cursor):
        return drop_table_people(db, cursor)

    def get_create_people_table_ddl(self):
        return get_create_people_table_ddl()


if __name__ == '__main__':
    unittest.main()
