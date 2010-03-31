from ostruct import OpenStruct


def open_structify(row, description):
    # For now this will work, but we'll have to adapt it for each database
    # driver/config depending on if it's using dict cursors or not, etc.

    result = OpenStruct()
    for (name, _, _, _, _, _, _,), value in zip(description, row):
        setattr(result, name, value)
    return result


def select(db, query):
    c = db.cursor()
    c.execute(query)

    for row in c.fetchall():
        yield open_structify(row, c.description)


def insert(db, table, objects):
    pass


#def update(db, query, ...


def delete(db, table, query):
    pass
