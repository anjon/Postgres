import argparse
import psycopg2
from psycopg2 import sql
from faker import Faker
import random
import time

def parse_arguments():
    parser = argparse.ArgumentParser(description="Configure PostgreSQL for CDC and populate with random data.")
    parser.add_argument("--db_name", type=str, default="kafkadb", help="Database name")
    parser.add_argument("--db_user", type=str, default="postgres", help="Database user")
    parser.add_argument("--db_password", type=str, default="password", help="Database password")
    parser.add_argument("--db_host", type=str, default="localhost", help="Database host")
    parser.add_argument("--db_port", type=int, default=5432, help="Database port")
    parser.add_argument("--num_records", type=int, default=10, help="Number of records to insert")
    parser.add_argument("--replication_slot", type=str, default="debezium_slot", help="Replication slot name")
    parser.add_argument("--publication_name", type=str, default="debezium_pub", help="Publication name")
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
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'persons'
            );
        """)
        table_exists = cur.fetchone()[0]
        
        if table_exists:
            print("Table 'persons' already exists.")
        else:
            cur.execute("""
                CREATE TABLE persons (
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
            print("Table 'persons' created successfully.")

# Grant replication privileges
def grant_replication_privileges(conn, user):
    with conn.cursor() as cur:
        cur.execute(f"ALTER ROLE {user} WITH REPLICATION;")
        conn.commit()
        print(f"Replication privileges granted to user '{user}'.")

# Verify existing replication slot and publication
def verify_setup(conn, slot_name, publication_name):
    with conn.cursor() as cur:
        cur.execute("SELECT slot_name FROM pg_replication_slots WHERE slot_name = %s;", (slot_name,))
        slot_exists = cur.fetchone() is not None

        cur.execute("SELECT pubname FROM pg_publication WHERE pubname = %s;", (publication_name,))
        publication_exists = cur.fetchone() is not None

    return slot_exists, publication_exists

# Create replication slot if not exists
def create_replication_slot(conn, slot_name):
    slot_exists, _ = verify_setup(conn, slot_name, "")
    if slot_exists:
        print(f"Replication slot '{slot_name}' already exists.")
        return
    
    with conn.cursor() as cur:
        try:
            cur.execute(f"SELECT pg_create_logical_replication_slot('{slot_name}', 'pgoutput');")
            conn.commit()
            print(f"Replication slot '{slot_name}' created successfully.")
        except Exception as e:
            conn.rollback()
            print(f"Error creating replication slot: {e}")

# Create publication if not exists
def create_publication(conn, publication_name):
    _, publication_exists = verify_setup(conn, "", publication_name)
    if publication_exists:
        print(f"Publication '{publication_name}' already exists.")
        return
    
    with conn.cursor() as cur:
        try:
            cur.execute(f"CREATE PUBLICATION {publication_name} FOR TABLE public.persons;")
            conn.commit()
            print(f"Publication '{publication_name}' created successfully.")
        except Exception as e:
            conn.rollback()
            print(f"Error creating publication: {e}")

# Insert random data
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
    with connect_db(args) as conn:
        try:
            grant_replication_privileges(conn, args.db_user)
            create_table(conn)  # Ensure the table exists before creating publication
            
            slot_exists, publication_exists = verify_setup(conn, args.replication_slot, args.publication_name)

            if not slot_exists:
                create_replication_slot(conn, args.replication_slot)

            if not publication_exists:
                create_publication(conn, args.publication_name)

            insert_data(conn, args.num_records)
            print(f"Database configured and populated with {args.num_records} records.")
        except Exception as e:
            conn.rollback()
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
