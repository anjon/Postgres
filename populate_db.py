import argparse
import psycopg2
from faker import Faker
import random
import time

def parse_arguments():
    parser = argparse.ArgumentParser(description="Populate PostgreSQL database with random data.")
    parser.add_argument("--db_name", type=str, default="kafkadb", help="Database name")
    parser.add_argument("--db_user", type=str, default="postgres", help="Database user")
    parser.add_argument("--db_password", type=str, default="password", help="Database password")
    parser.add_argument("--db_host", type=str, default="localhost", help="Database host")
    parser.add_argument("--db_port", type=int, default=5432, help="Database port")
    parser.add_argument("--num_records", type=int, default=100, help="Number of records to insert")
    return parser.parse_args()

# Create database connection
def connect_db(args):
    conn = psycopg2.connect(
        dbname=args.db_name,
        user=args.db_user,
        password=args.db_password,
        host=args.db_host,
        port=args.db_port
    )
    return conn

# Create table if not exists
def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS persons (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                surname VARCHAR(50),
                age INT,
                gender VARCHAR(10),
                country VARCHAR(100),
                email VARCHAR(100)
            )
        """)
        conn.commit()

# Insert random data with a 2-second delay
def insert_data(conn, num_records):
    fake = Faker()
    with conn.cursor() as cur:
        for i in range(num_records):
            gender = random.choice(["Male", "Female"])
            name = fake.first_name_male() if gender == "Male" else fake.first_name_female()
            surname = fake.last_name()
            age = fake.random_int(min=18, max=80)
            country = fake.country()
            email = fake.email()

            # Print the data before inserting
            print(f"Inserting {i+1}/{num_records}: Name={name}, Surname={surname}, Age={age}, Gender={gender}, Country={country}, Email={email}")

            cur.execute(
                "INSERT INTO persons (name, surname, age, gender, country, email) VALUES (%s, %s, %s, %s, %s, %s)",
                (name, surname, age, gender, country, email)
            )
            conn.commit()  # Commit after each insert
            time.sleep(2)  # Wait 2 seconds before inserting the next record
    
    print(f"\nSuccessfully inserted {num_records} records into the database.")

# Main function
def main():
    args = parse_arguments()
    conn = connect_db(args)
    create_table(conn)
    insert_data(conn, args.num_records)
    conn.close()
    print(f"Database populated with {args.num_records} random records.")

if __name__ == "__main__":
    main()
