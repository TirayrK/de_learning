#!/usr/bin/env python3

import mysql.connector
from faker import Faker
import random
import datetime
import os
import sys


def insert_new_products():
    """
    Insert 5 new random products into the Cloud SQL database
    using Faker library for realistic data generation
    """
    fake = Faker()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # Use the .my_cloudsql.cnf file for Cloud SQL connection
        with mysql.connector.connect(
                option_files=os.path.expanduser('~/.my_cloudsql.cnf'),
                option_groups=['client'],
                database='inventory',
                autocommit=False
        ) as conn:
            with conn.cursor() as cursor:
                print(f"[{timestamp}] SUCCESS: Database connection established")

                # Generate and insert 5 random products
                products_inserted = 0
                for i in range(5):
                    # Generate realistic product data
                    product_name = fake.bs().title()
                    quantity = random.randint(10, 200)
                    # Insert product into database
                    sql = "INSERT INTO products (product_name, quantity) VALUES (%s, %s)"
                    cursor.execute(sql, (product_name, quantity))
                    products_inserted += 1

                # Commit all insertions
                conn.commit()

                end_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{end_timestamp}] SUCCESS: {products_inserted} new products inserted and committed")

    except mysql.connector.Error as e:
        error_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{error_timestamp}] ERROR: Database operation failed - {e}")
        sys.exit(1)

    except Exception as e:
        error_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{error_timestamp}] ERROR: Script execution failed - {e}")
        sys.exit(1)

if __name__ == "__main__":
    insert_new_products()
