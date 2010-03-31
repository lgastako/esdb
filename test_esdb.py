import logging
import unittest
import sqlite3

from ostruct import OpenStruct
from conn import WrappedConnection
from esdb import select
from esdb import clause
from esdb import insert
from esdb import delete
from esdb import count

logger = logging.getLogger(__name__)


class DatabaseTests(unittest.TestCase):

    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        cursor = self.db.cursor()
        cursor.execute("""CREATE TABLE people (
                            id INTEGER PRIMARY KEY,
                            first_name VARCHAR(255) NOT NULL,
                            last_name VARCHAR(255) NOT NULL
                          )""")

        cursor.execute("""INSERT INTO people (id, first_name, last_name)
                          VALUES (?, ?, ?)""", (1, "Fred", "Flintstone"))

        cursor.execute("""INSERT INTO people (id, first_name, last_name)
                          VALUES (?, ?, ?)""", (2, "Barney", "Rubble"))

        cursor.execute("""INSERT INTO people (id, first_name, last_name)
                          VALUES (?, ?, ?)""", (3, "Wilma", "Flintstone"))
        self.wdb = WrappedConnection(self.db)
        self.people_table = self.wdb.get_table("people")


class FunctionTests(DatabaseTests):
    """Tests the core functions that act on db api connections."""


    def test_basic_select(self):
        people = select(self.db,
                        """SELECT id, first_name, last_name
                           FROM people
                           ORDER BY id
                           ASC LIMIT 3""")

        person = next(people)
        self.assertEquals(1, person.id)
        self.assertEquals("Fred", person.first_name)
        self.assertEquals("Flintstone", person.last_name)

        person = next(people)
        self.assertEquals(2, person.id)
        self.assertEquals("Barney", person.first_name)
        self.assertEquals("Rubble", person.last_name)

        person = next(people)
        self.assertEquals(3, person.id)
        self.assertEquals("Wilma", person.first_name)
        self.assertEquals("Flintstone", person.last_name)

        self.assertRaises(StopIteration, lambda: next(people))

    def test_basic_count(self):
        self.assertEquals(3, count(self.db, "people"))

    def test_basic_insert(self):
        person = OpenStruct(id=4,
                            first_name="Bam Bam",
                            last_name="Rubble")

        n = count(self.db, "people",
                  clause("first_name = %s", person.first_name))
        self.assertEquals(0, n)

        insert(self.db, "people", person)

        n = count(self.db, "people",
                  clause("first_name = %s", person.first_name))
        self.assertEquals(1, n)

    def test_basic_delete_all(self):
        delete(self.db, "people")
        self.assertEquals(0, count(self.db, "people"))

    def test_basic_individual_delete(self):
        delete(self.db, "people", clause("first_name = %s", "Barney"))
        self.assertEquals(2, count(self.db, "people"))


class MethodTests(DatabaseTests):
    """Tests the DB wrapper objects that wrap a dbapi connection and provide
    the helper functions as methods.
    """

    def test_basic_select(self):
        people = self.wdb.select("""SELECT id, first_name, last_name
                                    FROM people
                                    ORDER BY id
                                    ASC LIMIT 3""")

        person = next(people)
        self.assertEquals(1, person.id)
        self.assertEquals("Fred", person.first_name)
        self.assertEquals("Flintstone", person.last_name)

        person = next(people)
        self.assertEquals(2, person.id)
        self.assertEquals("Barney", person.first_name)
        self.assertEquals("Rubble", person.last_name)

        person = next(people)
        self.assertEquals(3, person.id)
        self.assertEquals("Wilma", person.first_name)
        self.assertEquals("Flintstone", person.last_name)

        self.assertRaises(StopIteration, lambda: next(people))

    def test_basic_count(self):
        self.assertEquals(3, self.wdb.count("people"))

    def test_basic_insert(self):
        person = OpenStruct(id=4,
                            first_name="Bam Bam",
                            last_name="Rubble")

        n = self.wdb.count("people",
                           clause("first_name = %s", person.first_name))
        self.assertEquals(0, n)

        self.wdb.insert("people", person)

        n = self.wdb.count("people",
                           clause("first_name = %s", person.first_name))
        self.assertEquals(1, n)

    def test_basic_delete_all(self):
        self.wdb.delete("people")
        self.assertEquals(0, self.wdb.count("people"))

    def test_basic_individual_delete(self):
        self.wdb.delete("people", clause("first_name = %s", "Barney"))
        self.assertEquals(2, self.wdb.count("people"))


class MethodTableTests(DatabaseTests):
    """Same tests but with table objects."""

    # def test_basic_select(self):
    #     people = self.people_table.select("""SELECT id, first_name, last_name
    #                                    FROM people
    #                                    ORDER BY id
    #                                    ASC LIMIT 3""")

    #     person = next(people)
    #     self.assertEquals(1, person.id)
    #     self.assertEquals("Fred", person.first_name)
    #     self.assertEquals("Flintstone", person.last_name)

    #     person = next(people)
    #     self.assertEquals(2, person.id)
    #     self.assertEquals("Barney", person.first_name)
    #     self.assertEquals("Rubble", person.last_name)

    #     person = next(people)
    #     self.assertEquals(3, person.id)
    #     self.assertEquals("Wilma", person.first_name)
    #     self.assertEquals("Flintstone", person.last_name)

    #     self.assertRaises(StopIteration, lambda: next(people))

    def test_basic_count(self):
        self.assertEquals(3, self.people_table.count())

    def test_basic_count_with_clause(self):
        self.assertEquals(1,
                          self.people_table.count(where=\
                                                      clause("first_name = %s",
                                                             "Fred")))

    def test_basic_insert_with_wrapped_table(self):
        person = OpenStruct(id=4,
                            first_name="Bam Bam",
                            last_name="Rubble")

        self.assertEquals(0,
                          self.people_table.count(clause("first_name = %s",
                                                         person.first_name)))

        self.people_table.insert(person)

        self.assertEquals(1,
                          self.people_table.count(clause("first_name = %s",
                                                         person.first_name)))

    def test_basic_delete_all(self):
        self.people_table.delete()
        self.assertEquals(0, self.people_table.count())

    def test_basic_individual_delete(self):
        self.people_table.delete(clause("first_name = %s", "Barney"))
        self.assertEquals(2, self.people_table.count())


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
