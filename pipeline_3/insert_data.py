#!/usr/bin/env python3

import mysql.connector
from faker import Faker
import random
import datetime
import os
import sys

def insert_new_products(config_file='~/.my.cnf'):
    """
    Insert 5 new random products into MySQL database
    using specified configuration file
    """
    fake = Faker()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # Use the specified config file for connection
        with mysql.connector.connect(
                option_files=os.path.expanduser(config_file),
                option_groups=['client'],
                database='inventory',
                autocommit=False
        ) as conn:
            with conn.cursor() as cursor:
                print(f"[{timestamp}] SUCCESS: Database connection established using {config_file}")

                products_inserted = 0
                for i in range(5):
                    product_name = fake.bs().title()
                    quantity = random.randint(10, 200)
                    sql = "INSERT INTO products (product_name, quantity) VALUES (%s, %s)"
                    cursor.execute(sql, (product_name, quantity))
                    products_inserted += 1

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
    # Default to VM config, but allow override via command line
    config_file = sys.argv[1] if len(sys.argv) > 1 else '~/.my.cnf'
    insert_new_products(config_file)