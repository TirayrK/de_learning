import mysql.connector
from faker import Faker
import random
import datetime
import os


def insert_new_products():
    fake = Faker()
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    try:
        # Use the .my.cnf file for local MySQL connection
        with mysql.connector.connect(
                option_files=os.path.expanduser('~/.my.cnf'),
                option_groups=['client'],
                database='inventory'
        ) as conn:
            with conn.cursor() as cursor:
                print(f"[{timestamp}] SUCCESS: Database connection established")
                for i in range(5):
                    product = fake.bs().title()
                    quantity = random.randint(10, 200)
                    sql = "INSERT INTO products (product_name, quantity) VALUES (%s, %s)"
                    cursor.execute(sql, (product, quantity))
                conn.commit()
                end_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{end_timestamp}] SUCCESS: 5 new products inserted and committed")
    except mysql.connector.Error as e:
        error_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{error_timestamp}] ERROR: Database operation failed - {e}")
    except Exception as e:
        error_timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{error_timestamp}] ERROR: Script execution failed - {e}")


if __name__ == "__main__":
    insert_new_products()