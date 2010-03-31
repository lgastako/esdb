import unittest
import sqlite3

from esdb import select


class SelectTests(unittest.TestCase):

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


if __name__ == '__main__':
    unittest.main()
