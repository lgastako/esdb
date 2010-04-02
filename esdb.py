import logging

from types import GeneratorType

from ostruct import OpenStruct

logger = logging.getLogger(__name__)


class SQLLite3Dialect(object):
    placeholder = "?"

    @staticmethod
    def table_exists(db, table):
        return count(db, "sqlite_master",
                     where=clause(name=table)) > 0


class MySQLDialect(object):
    placeholder = "%s"

    @staticmethod
    def table_exists(db, table):
        cursor = db.cursor()
        # TODO: SQL injection problem?
        cursor.execute("SHOW TABLES LIKE '%s'" % table)
        rows = cursor.fetchall()
        return len(rows) > 0


class PostgresDialect(object):
    placeholder = "%s"

    @staticmethod
    def table_exists(db, table):
        return count(db, "pg_tables",
                     where=clause(tablename=table)) > 0


def execute_query(cursor, query, args=None):
    logger.debug("insert query: <<%s>>" % query)
    logger.debug("args: <<%s>>" % repr(args))
    if args:
        cursor.execute(query, args)
    else:
        cursor.execute(query)


def open_structify(row, description):
    # For now this will work, but we'll have to adapt it for each database
    # driver/config depending on if it's using dict cursors or not, etc.

    result = OpenStruct()
    for (name, _, _, _, _, _, _,), value in zip(description, row):
        setattr(result, name, value)
    return result


def select(db, query, where=None):
    if where:
        query += u" WHERE " + unicode(where)

    cursor = db.cursor()

    logger.debug("Executing query: %s" % query)
    execute_query(cursor, query)

    for row in cursor.fetchall():
        yield open_structify(row, cursor.description)


# def clause(fragment, *args, **kwargs):
#     # For now this is fine, but eventually we'll have to hook into the
#     # various dbapi driver's escaping mechanism.
#     def quote(arg):
#         if isinstance(arg, basestring):
#             arg = "'%s'" % arg
#         return arg
#     if len(args):
#         fragment %= tuple(quote(a) for a in args)
#     if len(kwargs):
#         fragment = fragment.format(**kwargs)
#     logger.debug("Rendered fragment: %s" % fragment)
#     return fragment

def clause(**kwargs):
    # For now this is fine, but eventually we'll have to hook into the
    # various dbapi driver's escaping mechanism.
    def quote(arg):
        if isinstance(arg, basestring):
            arg = "'%s'" % arg
        return arg
    if not len(kwargs):
        raise Exception("Invalid clause")
    return " AND ".join(["%s = %s" % (key, quote(value))
                         for key, value in kwargs.items()])


def count(db, table, where=None):
    query = "SELECT COUNT(*) AS cnt FROM %s" % table
    if where:
        query += u" WHERE " + unicode(where)
    return next(select(db, query)).cnt


DIALECTS = {"sqlite3": SQLLite3Dialect,
            "MySQLdb.cursors": MySQLDialect,
            "psycopg2._psycopg": PostgresDialect}


def get_dialect(cursor):
    mod = cursor.__class__.__module__
    dialect = DIALECTS.get(mod)
    if not dialect:
        raise Exception("No dialect registered for connection module: %s" %
                        mod)
    return dialect


def insert(db, table, objects, cols=None):
    if not isinstance(objects, (tuple, list, GeneratorType)):
        objects = [objects]

    cursor = db.cursor()
    dialect = get_dialect(cursor)

    query = "INSERT INTO %s (" % table

    if not cols:
        col_names = set()
        for obj in objects:
            params = obj.__dict__.keys()
            for param in params:
                col_names.add(param)

    # TODO: Validate values
    col_clause = ", ".join([c for c in col_names])
    query += col_clause

    query += ") VALUES ("
    value_clause = ", ".join([dialect.placeholder for _ in col_names])
    query += value_clause

    query += ")"

    logger.debug("col_names: <<%s>>" % repr(col_names))
    for obj in objects:
        args = []
        for c in col_names:
            value = getattr(obj, c, None)
            args.append(value)
        execute_query(cursor, query, args)


def update(db, table, **kwargs):
    where = None
    if "where" in kwargs:
        where = kwargs["where"]
        del kwargs["where"]

    if len(kwargs) < 1:
        # TODO: Better message
        raise Exception("Update required kwargs.")

    # TODO: SQL injection
    query = "UPDATE %s SET " % table

    cursor = db.cursor()
    dialect = get_dialect(cursor)

    clauses = []
    args = []
    for key, value in kwargs.items():
        clauses.append("%s = %s" % (key, dialect.placeholder))
        args.append(value)
    query += ", ".join(clauses)

    if where:
        query += " WHERE " + unicode(where)

    execute_query(cursor, query, args)


def delete(db, table, where=None):
    query = "DELETE FROM %s" % table

    if where:
        query += " WHERE " + unicode(where)

    cursor = db.cursor()
    execute_query(cursor, query)


def table_exists(db, table):
    cursor = db.cursor()
    dialect = get_dialect(cursor)
    return dialect.table_exists(db, table)


def drop_table(db, table):
    # TODO: SQL injection
    db.cursor().execute("DROP TABLE %s" % table)
