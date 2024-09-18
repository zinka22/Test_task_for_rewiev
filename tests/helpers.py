import os
import unittest

import psycopg2


class BaseClassForDateBase(unittest.TestCase):
    """Base class for setting up and tearing down database connections."""

    @classmethod
    def setUpClass(cls):
        """Load configuration based on environment variable."""
        env = os.getenv("TEST_ENV", "local")
        if env == "local":
            cls.db_config = {
                "host": os.getenv("LOCAL_DB_HOST"),
                "database": os.getenv("LOCAL_DB_NAME"),
                "user": os.getenv("LOCAL_DB_USER"),
                "password": os.getenv("LOCAL_DB_PASSWORD"),
                "port": os.getenv("LOCAL_DB_PORT"),
            }
        elif env == "remote_for_check":
            cls.db_config = {
                "host": os.getenv("REMOTE_FOR_CHECK_DB_HOST"),
                "database": os.getenv("REMOTE_FOR_CHECK_DB_NAME"),
                "user": os.getenv("REMOTE_FOR_CHECK_DB_USER"),
                "password": os.getenv("REMOTE_FOR_CHECK_DB_PASSWORD"),
                "port": os.getenv("REMOTE_FOR_CHECK_DB_PORT"),
            }
        else:
            raise ValueError(f"Unknown environment: {env}")

    def setUp(self):
        """Connect to the database using parameters from the selected environment."""
        try:
            self.connection = psycopg2.connect(
                host=self.db_config["host"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                database=self.db_config["database"],
                port=self.db_config["port"],
            )
            self.cursor = self.connection.cursor()
            self.connection.autocommit = False

            self.cursor.execute("SELECT 1")
            result = self.cursor.fetchone()
            if result is None or result[0] != 1:
                raise Exception("Failed to connect to the database.")

        except (Exception, psycopg2.DatabaseError) as error:
            self.fail(f"Database connection failed: {error}")

    def tearDown(self):
        """Rollback transactions and close connections after each test."""
        self.connection.rollback()
        self.cursor.close()
        self.connection.close()
