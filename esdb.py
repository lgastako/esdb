import logging

from types import GeneratorType

from ostruct import OpenStruct

logger = logging.getLogger(__name__)


class SQLLite3Dialect(object):
    placeholder = "?"


class MySQLDialect(object):
    placeholder = "%s"


class PostgresDialect(object):
    placeholder = "I DUNNO"


def open_structify(row, description):
    # For now this will work, but we'll have to adapt it for each database
    # driver/config depending on if it's using dict cursors or not, etc.

    result = OpenStruct()
    for (name, _, _, _, _, _, _,), value in zip(description, row):
        setattr(result, name, value)
    return result


def select(db, query, where=None):
    c = db.cursor()
    if where:
        query += u" WHERE " + unicode(where)

    logger.debug("Executing query: %s" % query)
    c.execute(query)

    for row in c.fetchall():
        yield open_structify(row, c.description)


def clause(fragment, *args, **kwargs):
    # For now this is fine, but eventually we'll have to hook into the
    # various dbapi driver's escaping mechanism.
    def quote(arg):
        if isinstance(arg, basestring):
            arg = "'%s'" % arg
        return arg
    if len(args):
        fragment %= tuple(quote(a) for a in args)
    if len(kwargs):
        fragment = fragment.format(**kwargs)
    logger.debug("Rendered fragment: %s" % fragment)
    return fragment


def count(db, table, where=None):
    query = "SELECT COUNT(*) AS cnt FROM %s" % table
    if where:
        query += u" WHERE " + unicode(where)
    return next(select(db, query)).cnt


def get_dialect(cursor):
    if cursor.__class__.__module__ == "sqlite3":
        return SQLLite3Dialect
    raise "Unknown dialect."


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

    for obj in objects:
        args = []
        for c in col_names:
            value = getattr(obj, c, None)
            args.append(value)
        logger.debug("insert query: <<%s>>" % query)
        logger.debug("args: <<%s>>" % repr(args))
        cursor.execute(query, args)


#def update(db, query, ...


def delete(db, table, query):
    pass
