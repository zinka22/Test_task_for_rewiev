# для запуска тестов по умолчанию выбрана локальная база данных (local)
# для запуска на другом окружении (remote_for_check) запуск из командной строки
# ```$env:TEST_ENV="local"
# python -m unittest <путь>
# $env:TEST_ENV=$null```

import random
import unittest

from dotenv import load_dotenv
from faker import Faker
from tests.helpers import BaseClassForDateBase
from psycopg2 import Error

load_dotenv()


class PositiveTestPeopleTable(BaseClassForDateBase):
    def test_add_string_with_valid_data(self):
        """Test: Add one row -> all input data is valid."""
        fake = Faker("ru_RU")
        string_vars = (fake.name(), fake.date())
        insert_query = """INSERT INTO people (name, dateofbirth) VALUES (%s, %s)"""
        self.cursor.execute(insert_query, string_vars)

        self.cursor.execute("SELECT * FROM people WHERE name = %s", (string_vars[0],))
        record = self.cursor.fetchone()
        self.assertIsNotNone(record)
        self.assertEqual(record[1], string_vars[0])

    def test_add_multiple_strings_with_valid_data(self):
        """Test: Add several rows with valid data. Ensure 4 rows are added."""

        valid_rows = [
            ("A", "1990-06-06"),  # Name with 1 character
            ("A" * 255, "1990-06-06"),  # Name with 255 characters
            ("Наташа", "0001-01-01"),  # Minimum valid date
            ("Наташа", "9999-12-31"),  # Maximum valid date
        ]

        self.cursor.execute("SELECT COUNT(*) FROM people")
        number_of_rows_in_table_now = self.cursor.fetchone()[0]

        insert_query = """
            INSERT INTO people (name, dateofbirth) 
            VALUES (%s, TO_DATE(%s, 'YYYY-MM-DD'))
        """
        self.cursor.executemany(insert_query, valid_rows)

        self.cursor.execute("SELECT COUNT(*) FROM people")
        number_of_rows_after_insert = self.cursor.fetchone()[0]

        self.assertEqual(number_of_rows_after_insert - number_of_rows_in_table_now, 4)

    def test_update_string_by_id(self):
        """Test: Update a row by id -> the row is correctly updated."""
        insert_query = """
            INSERT INTO people (name, dateofbirth)
            VALUES (%s, TO_DATE(%s, 'YYYY-MM-DD'))
            RETURNING index
        """
        self.cursor.execute(insert_query, ("Наташа", "1990-08-29"))
        index = self.cursor.fetchone()[0]

        update_query = """UPDATE people SET name = %s WHERE index = %s"""
        self.cursor.execute(update_query, ("Алексей", index))

        self.cursor.execute("SELECT name FROM people WHERE index = %s", (index,))
        updated_record = self.cursor.fetchone()
        self.assertEqual(updated_record[0], "Алексей")

    def test_delete_string_by_id(self):
        """Test: Delete a row by id -> the row is deleted."""
        insert_query = """
            INSERT INTO people (name, dateofbirth) 
            VALUES (%s, TO_DATE(%s, 'YYYY-MM-DD')) 
            RETURNING index
        """
        self.cursor.execute(insert_query, ("Наташа", "1990-08-29"))
        index = self.cursor.fetchone()[0]

        delete_query = """DELETE FROM people WHERE index = %s"""
        self.cursor.execute(delete_query, (index,))

        self.cursor.execute("SELECT * FROM people WHERE index = %s", (index,))
        deleted_record = self.cursor.fetchone()
        self.assertIsNone(deleted_record)

    def test_insert_name_with_various_characters(self):
        """Test: Field 'name' accepts Latin, Cyrillic, numbers, special characters, and spaces."""
        fake = Faker()
        latin_name = fake.first_name()
        fake_ru = Faker("ru_RU")
        cyrillic_name = fake_ru.first_name()
        numeric_string = str(random.randint(100000, 999999))
        space_string = f"{fake.first_name()} {fake.last_name()}  with   2 and 3 spaces"
        input_data_for_insert = [
            (latin_name, "1990-02-01"),
            (cyrillic_name, "1990-04-03"),
            (numeric_string, "1990-06-05"),
            ("@#&$%^*", "1990-08-07"),  # Special characters
            (space_string, "1990-10-09"),
        ]

        self.cursor.execute("SELECT COUNT(*) FROM people")
        strings_count_before_insert = self.cursor.fetchone()[0]

        insert_query = """
            INSERT INTO people (name, dateofbirth) 
            VALUES (%s, TO_DATE(%s, 'YYYY-MM-DD'))
        """
        self.cursor.executemany(insert_query, input_data_for_insert)

        self.cursor.execute("SELECT COUNT(*) FROM people")
        strings_count_after_insert = self.cursor.fetchone()[0]

        self.assertEqual(
            strings_count_after_insert - strings_count_before_insert,
            len(input_data_for_insert),
            "Inserted row count does not match",
        )

        self.cursor.execute(
            "SELECT name, dateofbirth FROM people ORDER BY name, dateofbirth"
        )
        records = self.cursor.fetchall()

        records_for_compare = [
            (name, date_obj.strftime("%Y-%m-%d")) for name, date_obj in records
        ]

        for string in input_data_for_insert:
            self.assertIn(
                string,
                records_for_compare,
                f"Inserted row {string} not found in records",
            )


class NegativeTestPeopleTable(BaseClassForDateBase):
    def test_insert_invalid_null_name(self):
        """Test: Create a string with name == null -> string is not created."""
        insert_query = """
            INSERT INTO people (name, dateofbirth) 
            VALUES (%s, TO_DATE(%s, 'YYYY-MM-DD')
        """
        with self.assertRaises(Error):
            self.cursor.execute(insert_query, (None, "1990-06-06"))

    def test_insert_invalid_name_256_chars(self):
        """Test: Create a string with a name of 256 characters -> string is not created."""
        insert_query = """
            INSERT INTO people (name, dateofbirth) 
            VALUES (%s, TO_DATE(%s, 'YYYY-MM-DD')
        """
        with self.assertRaises(Error):
            self.cursor.execute(insert_query, ("A" * 256, "1990-06-06"))

    def test_insert_invalid_date(self):
        """Test: Create a string with invalid date 0001-01-00 -> string is not created."""
        insert_query = """
            INSERT INTO people (name, dateofbirth) 
            VALUES (%s, TO_DATE(%s, 'YYYY-MM-DD')
        """
        with self.assertRaises(Error):
            self.cursor.execute(insert_query, ("Наташа", "0001-01-00"))

    def test_insert_invalid_date_10000(self):
        """Test: Create a row with invalid date 10000-01-01 -> string not created."""
        insert_query = """
            INSERT INTO people (name, dateofbirth) 
            VALUES (%s, TO_DATE(%s, 'YYYY-MM-DD')
        """
        with self.assertRaises(Error):
            self.cursor.execute(insert_query, ("Наташа", "10000-01-01"))

    def test_request_for_non_existent_string(self):
        """Test: Try to get a string that does not exist -> string is not received."""
        self.cursor.execute("SELECT COALESCE(MAX(index), 0) FROM people")
        max_index = self.cursor.fetchone()[0]
        self.cursor.execute("SELECT * FROM people WHERE index = %s", (max_index + 1,))
        record = self.cursor.fetchone()
        self.assertIsNone(record)

    def test_update_non_existent_string(self):
        """Test: Try to update string that does not exist -> string is not updated."""
        self.cursor.execute("SELECT COALESCE(MAX(index), 0) FROM people")
        max_index = self.cursor.fetchone()[0]

        update_query = """UPDATE people SET name = %s WHERE index = %s"""
        self.cursor.execute(update_query, ("Новый", max_index + 1))

        self.cursor.execute("SELECT * FROM people WHERE index = %s", (max_index + 1,))
        updated_string = self.cursor.fetchone()

        self.assertIsNone(updated_string)

    def test_delete_non_existent_string(self):
        """Test: Try to delete a row with index max + 1 -> error, deletion failed"""

        self.cursor.execute("SELECT COALESCE(MAX(index), 0) FROM people")
        max_index = self.cursor.fetchone()[0]

        delete_query = """DELETE FROM people WHERE index = %s"""
        self.cursor.execute(delete_query, (max_index + 1,))

        self.cursor.execute("SELECT * FROM people WHERE index = %s", (max_index + 1,))
        deleted_record = self.cursor.fetchone()

        self.assertIsNone(deleted_record)


if __name__ == "__main__":
    unittest.main()
