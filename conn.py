from esdb import select as select_f
from esdb import insert as insert_f
from esdb import delete as delete_f
from esdb import count as count_f
from esdb import table_exists as table_exists_f
from esdb import drop_table as drop_table_f


class Table(object):

    def __init__(self, conn, table_name):
        self.conn = conn
        self.table_name = table_name

    def count(self, where=None):
        return self.conn.count(self.table_name, where=where)

    def insert(self, objects, cols=None):
        return self.conn.insert(self.table_name, objects, cols=cols)

    def delete(self, where=None):
        return self.conn.delete(self.table_name, where=where)

    def commit(self):
        return self.conn.commit()

    def rollback(self):
        return self.conn.rollback()

    def exists(self):
        return self.conn.table_exists(self.table_name)

    def drop(self):
        return self.conn.drop_table(self.table_name)


class WrappedConnection(object):

    def __init__(self, conn):
        self.conn = conn

    def select(self, query, where=None):
        return select_f(self.conn, query, where=where)

    def count(self, table, where=None):
        return count_f(self.conn, table, where=where)

    def insert(self, table, objects, cols=None):
        return insert_f(self.conn, table, objects, cols=cols)

    def delete(self, table, where=None):
        return delete_f(self.conn, table, where=where)

    def get_table(self, table_name):
        return Table(self, table_name)

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def table_exists(self, table):
        return table_exists_f(self.conn, table)

    def drop_table(self, table):
        return drop_table_f(self.conn, table)

