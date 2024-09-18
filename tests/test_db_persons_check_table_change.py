# для запуска тестов по умолчанию выбрана локальная база данных (local)
# для запуска на другом окружении (remote_for_check) запуск из командной строки
# ```$env:TEST_ENV="local"
# python -m unittest <путь>
# $env:TEST_ENV=$null```

import unittest

import psycopg2
from psycopg2 import errors
from dotenv import load_dotenv
from tests.helpers import BaseClassForDateBase

load_dotenv()


class PositiveTests(BaseClassForDateBase):
    def test_rename_table(self):
        """Test: Rename table -> table is renamed successfully."""

        self.cursor.execute(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'persons' AND table_schema = 'public';
            """
        )
        structure_before_rename = self.cursor.fetchall()

        self.cursor.execute("ALTER TABLE Persons RENAME TO RenamedPersons;")

        self.cursor.execute(
            """
            SELECT * FROM information_schema.tables 
            WHERE table_name = 'persons' AND table_schema = 'public';
            """
        )
        self.assertIsNone(self.cursor.fetchone(), "Table 'Persons' should not exist")

        self.cursor.execute(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'renamedpersons' AND table_schema = 'public';
            """
        )
        structure_after_rename = self.cursor.fetchall()

        self.assertEqual(
            structure_before_rename,
            structure_after_rename,
            "Table's structure is changed, but it shouldn't",
        )

        self.connection.rollback()

    def test_rename_column(self):
        """Test: Rename column -> column is renamed successfully."""

        self.cursor.execute(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'persons' AND table_schema = 'public';
            """
        )
        structure_before = self.cursor.fetchall()

        self.cursor.execute(
            "ALTER TABLE persons RENAME COLUMN FirstName TO First_Name;"
        )

        self.cursor.execute(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'persons' AND table_schema = 'public';
            """
        )
        structure_after = self.cursor.fetchall()

        column_diff = [
            (col_before, col_after)
            for col_before, col_after in zip(structure_before, structure_after)
            if col_before != col_after
        ]

        self.assertEqual(
            len(column_diff),
            1,
            "Различий должно быть ровно одно — переименование столбца.",
        )
        self.assertEqual(
            column_diff[0],
            (("firstname", "character varying"), ("first_name", "character varying")),
            "Ожидаемое различие — переименование столбца FirstName в First_Name.",
        )

        self.connection.rollback()

    def test_add_column(self):
        """Test: Add column -> column is added successfully."""

        self.cursor.execute(
            """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'persons' AND table_schema = 'public';
            """
        )
        structure_before = self.cursor.fetchall()

        self.cursor.execute("ALTER TABLE persons ADD COLUMN Age int;")

        self.cursor.execute(
            """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'persons' AND table_schema = 'public';
            """
        )
        structure_after = self.cursor.fetchall()

        self.assertEqual(
            len(structure_after),
            len(structure_before) + 1,
            "The number of columns was expected to increase by 1.",
        )

        new_column = ("age",)
        self.assertIn(new_column, structure_after, "The 'Age' column should be added.")

        self.connection.rollback()

    def test_delete_column(self):
        """Test: Delete column -> column is deleted successfully."""

        self.cursor.execute(
            """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'persons' AND table_schema = 'public';
            """
        )
        structure_before = self.cursor.fetchall()

        self.cursor.execute("ALTER TABLE persons DROP COLUMN Hobby;")

        self.cursor.execute(
            """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = 'persons' AND table_schema = 'public';
            """
        )
        structure_after = self.cursor.fetchall()

        self.assertEqual(
            len(structure_after),
            len(structure_before) - 1,
            "The number of columns was expected to be decreased by 1.",
        )

        deleted_column = ("hobby",)
        self.assertNotIn(
            deleted_column, structure_after, "The 'Hobby' column should be removed."
        )

        self.connection.rollback()

    def test_change_column_type(self):
        """Test: Change column type -> column type is changed successfully."""

        self.cursor.execute(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'persons' AND column_name = 'dateofbirth' AND table_schema = 'public';
            """
        )
        structure_before = self.cursor.fetchall()

        self.cursor.execute(
            "ALTER TABLE persons ALTER COLUMN DateOfBirth TYPE timestamp;"
        )

        self.cursor.execute(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'persons' AND column_name = 'dateofbirth' AND table_schema = 'public';
            """
        )
        structure_after = self.cursor.fetchall()

        self.assertNotEqual(
            structure_before, structure_after, "The column data type should be changed."
        )
        self.assertEqual(
            structure_after[0][1],
            "timestamp without time zone",
            "The expected data type of the column is TIMESTAMP.",
        )

        self.connection.rollback()

    def test_change_column_type_and_verify_data(self):
        """Test: Change column type -> data is converted correctly."""

        insert_query = """
            INSERT INTO Persons (FirstName, FamilyName, DateOfBirth, PlaceOfBirth, Occupation, Hobby)
            VALUES (%s, %s, TO_DATE(%s, 'YYYY-MM-DD'), %s, %s, %s);
        """
        self.cursor.execute(
            insert_query,
            (
                "Майа",
                "Плисецкая",
                "1925-11-20",
                "Москва",
                "Балерина",
                "Вырезки из газет",
            ),
        )

        self.cursor.execute(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'persons' AND column_name = 'dateofbirth' AND table_schema = 'public';
            """
        )
        structure_before = self.cursor.fetchall()

        self.cursor.execute(
            "ALTER TABLE persons ALTER COLUMN DateOfBirth TYPE timestamp;"
        )

        self.cursor.execute(
            """
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'persons' AND column_name = 'dateofbirth' AND table_schema = 'public';
            """
        )

        structure_after = self.cursor.fetchall()

        self.assertNotEqual(
            structure_before, structure_after, "The column data type should be changed."
        )

        self.cursor.execute(
            "SELECT FirstName, FamilyName, DateOfBirth "
            "FROM persons "
            "WHERE FirstName = 'Майа' AND FamilyName = 'Плисецкая';"
        )
        record = self.cursor.fetchone()
        self.assertIsNotNone(record)

        first_name, family_name, date_of_birth = record
        date_of_birth_str = date_of_birth.strftime("%Y-%m-%d %H:%M:%S")

        self.assertEqual(
            (first_name, family_name, date_of_birth_str),
            ("Майа", "Плисецкая", "1925-11-20 00:00:00"),
            "Данные должны быть корректно преобразованы в новый тип TIMESTAMP.",
        )

        self.connection.rollback()

    def test_delete_table(self):
        """Test: Delete table -> table is deleted from the database."""

        self.cursor.execute(
            "SELECT * FROM information_schema.tables WHERE table_name='persons';"
        )
        table_exists = self.cursor.fetchone()
        self.assertIsNotNone(
            table_exists, "The 'persons' table should exist before deleting."
        )

        self.cursor.execute("DROP TABLE persons;")

        self.cursor.execute(
            "SELECT * FROM information_schema.tables WHERE table_name='persons';"
        )
        table_deleted = self.cursor.fetchone()
        self.assertIsNone(table_deleted, "The table 'persons' should be dropped.")

        self.connection.rollback()


class NegativeTests(BaseClassForDateBase):
    def test_rename_nonexistent_table(self):
        """Test: Rename non-existent table -> error, table does not exist."""
        self.cursor.execute(
            "SELECT table_name "
            "FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_name = 'nonexistenttable';"
        )
        self.assertIsNone(self.cursor.fetchone(), "Table should not exist.")

        with self.assertRaises(psycopg2.errors.UndefinedTable):
            self.cursor.execute("ALTER TABLE NonExistentTable RENAME TO NewTableName;")

    def test_rename_nonexistent_column(self):
        """Test: Rename non-existent column -> error, failed to rename column."""
        self.cursor.execute(
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_name = 'persons' AND column_name = 'non_existing_column';"
        )
        self.assertIsNone(self.cursor.fetchone(), "Column should not exist.")

        with self.assertRaises(psycopg2.errors.UndefinedColumn):
            self.cursor.execute(
                "ALTER TABLE persons RENAME COLUMN non_existing_column TO new_column;"
            )

    def test_add_existing_column(self):
        """Test: Add existing column -> error, failed to add column."""
        self.cursor.execute(
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_name = 'persons' AND column_name = 'firstname';"
        )
        self.assertIsNotNone(self.cursor.fetchone(), "Column FirstName should exist..")

        with self.assertRaises(psycopg2.errors.DuplicateColumn):
            self.cursor.execute("ALTER TABLE persons ADD COLUMN FirstName varchar;")

    def test_delete_nonexistent_column(self):
        """Test: Delete non-existent column -> error, failed to delete column."""
        self.cursor.execute(
            "SELECT column_name "
            "FROM information_schema.columns "
            "WHERE table_name = 'persons' AND column_name = 'non_existing_column';"
        )
        self.assertIsNone(self.cursor.fetchone(), "Column should not exist.")

        with self.assertRaises(psycopg2.errors.UndefinedColumn):
            self.cursor.execute("ALTER TABLE persons DROP COLUMN non_existing_column;")

    def test_change_column_type_incompatible(self):
        """Change data type to incompatible -> error, failed to change data."""
        self.cursor.execute(
            "SELECT data_type "
            "FROM information_schema.columns "
            "WHERE table_name = 'persons' AND column_name = 'dateofbirth';"
        )
        current_type = self.cursor.fetchone()[0]
        self.assertEqual(current_type, "date", "The column data type must be date.")

        with self.assertRaises(psycopg2.errors.DatatypeMismatch):
            self.cursor.execute(
                "ALTER TABLE persons ALTER COLUMN DateOfBirth TYPE int;"
            )


if __name__ == "__main__":
    unittest.main()
