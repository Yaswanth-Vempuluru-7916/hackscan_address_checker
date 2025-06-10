import psycopg2
from dotenv import load_dotenv
import os 

def get_db_connection():
    load_dotenv()

    db_params = {
        "dbname" : os.getenv("MAIN_DB_NAME"),
        "user" : os.getenv("MAIN_DB_USER"),
        "password" :  os.getenv("MAIN_DB_PASSWORD"),
        "host": os.getenv("MAIN_DB_HOST"),
        "port": os.getenv("MAIN_DB_PORT")
    }

    try:
        conn = psycopg2.connect(**db_params)
        print("Database connection established")
        return conn
    except psycopg2.Error as e :
        print("Database connection error : ", e)
        return None

def test_connection():
    """Fetch initiator_source_address, initiator_destination_address, and bitcoin_optional_recipient from create_orders."""
    conn = get_db_connection()
    if not conn:
        return
    
    try:
        with conn.cursor() as cur:
            query = """
            SELECT initiator_source_address, initiator_destination_address, 
                   additional_data->>'bitcoin_optional_recipient' AS bitcoin_optional_recipient
            FROM create_orders
            ORDER BY created_at DESC
            LIMIT 5
            """
            cur.execute(query)
            rows = cur.fetchall()
            for row in rows:
                print("Row:", {"initiator_source_address": row[0], 
                               "initiator_destination_address": row[1], 
                               "bitcoin_optional_recipient": row[2]})
    except psycopg2.Error as e:
        print("Query error:", e)
    finally:
        conn.close()
        print("Database connection closed")

if __name__ == "__main__":
    test_connection()