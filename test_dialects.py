import logging
import unittest

from ostruct import OpenStruct
from conn import WrappedConnection
from esdb import select
from esdb import clause
from esdb import insert
from esdb import update
from esdb import delete
from esdb import count

logger = logging.getLogger(__name__)


class DialectTests(unittest.TestCase):

    def get_dialect_connection(self):
        raise NotImplementedError

    def get_create_people_table_ddl(self):
        return """CREATE TABLE people (
                               id INTEGER PRIMARY KEY,
                               first_name VARCHAR(255) NOT NULL,
                               last_name VARCHAR(255) NOT NULL
                  )"""

    def setUp(self):
        self.db = self.get_dialect_connection()
        cursor = self.db.cursor()

        self.wdb = WrappedConnection(self.db)
        self.people_table = self.wdb.get_table("people")
        if self.people_table.exists():
            self.people_table.drop()
        self.db.commit()

        cursor.execute(self.get_create_people_table_ddl())
        self.db.commit()

        self.people_table.insert(OpenStruct(id=1,
                                            first_name="Fred",
                                            last_name="Flintstone"))
        self.people_table.insert(OpenStruct(id=2,
                                            first_name="Barney",
                                            last_name="Rubble"))
        self.people_table.insert(OpenStruct(id=3,
                                            first_name="Wilma",
                                            last_name="Flintstone"))
        self.db.commit()
        self.bam_bam = OpenStruct(id=4,
                                  first_name="Bam Bam",
                                  last_name="Rubble")

    def tearDown(self):
        self.db.rollback()


class FunctionTests(DialectTests):
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
                  clause(first_name=person.first_name))
        self.assertEquals(0, n)

        insert(self.db, "people", person)

        n = count(self.db, "people",
                  clause(first_name=person.first_name))
        self.assertEquals(1, n)

    def test_basic_delete_all(self):
        delete(self.db, "people")
        self.assertEquals(0, count(self.db, "people"))

    def test_basic_individual_delete(self):
        delete(self.db, "people", clause(first_name="Barney"))
        self.assertEquals(2, count(self.db, "people"))

    def test_basic_update_all(self):
        update(self.db, "people", last_name="Rubble")
        self.assertEquals(3, count(self.db, "people",
                                   where=clause(last_name="Rubble")))

    def test_basic_update_some(self):
        update(self.db, "people", last_name="Rubble",
               where=clause(first_name="Wilma"))
        self.assertEquals(2, count(self.db, "people",
                                   where=clause(last_name="Rubble")))


class MethodTests(DialectTests):
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
        person = self.bam_bam

        n = self.wdb.count("people",
                           clause(first_name=person.first_name))
        self.assertEquals(0, n)

        self.wdb.insert("people", person)

        n = self.wdb.count("people",
                           clause(first_name=person.first_name))
        self.assertEquals(1, n)

    def test_basic_delete_all(self):
        self.wdb.delete("people")
        self.assertEquals(0, self.wdb.count("people"))

    def test_basic_individual_delete(self):
        self.wdb.delete("people", clause(first_name="Barney"))
        self.assertEquals(2, self.wdb.count("people"))

    def test_rollback(self):
        self.wdb.insert("people", self.bam_bam)
        self.assertEquals(4, self.wdb.count("people"))
        self.wdb.rollback()
        self.assertEquals(3, self.wdb.count("people"))

    def test_commit(self):
        self.wdb.insert("people", self.bam_bam)
        self.assertEquals(4, self.wdb.count("people"))
        self.wdb.commit()
        self.assertEquals(4, self.wdb.count("people"))
        self.wdb.rollback()
        self.assertEquals(4, self.wdb.count("people"))


class MethodTableTests(DialectTests):
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
                                                      clause(
                                      first_name="Fred")))

    def test_basic_insert_with_wrapped_table(self):
        person = OpenStruct(id=4,
                            first_name="Bam Bam",
                            last_name="Rubble")

        self.assertEquals(0,
                          self.people_table.count(clause(
                    first_name=person.first_name)))

        self.people_table.insert(person)

        self.assertEquals(1,
                          self.people_table.count(clause(
                    first_name=person.first_name)))

    def test_basic_delete_all(self):
        self.people_table.delete()
        self.assertEquals(0, self.people_table.count())

    def test_basic_individual_delete(self):
        self.people_table.delete(clause(first_name="Barney"))
        self.assertEquals(2, self.people_table.count())

    def test_rollback(self):
        self.people_table.insert(self.bam_bam)
        self.assertEquals(4, self.people_table.count())
        self.people_table.rollback()
        self.assertEquals(3, self.people_table.count())

    def test_commit(self):
        self.people_table.insert(self.bam_bam)
        self.assertEquals(4, self.people_table.count())
        self.people_table.commit()
        self.assertEquals(4, self.wdb.count("people"))
        self.people_table.rollback()
        self.assertEquals(4, self.wdb.count("people"))


if __name__ == '__main__':
    print "Don't run this directly, run the test_{database}.py"
