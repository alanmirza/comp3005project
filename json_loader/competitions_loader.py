import psycopg2
import json
import os

def read_json(file_path):
    """Reads a JSON file from the specified path and returns the data."""
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data

def connect_postgres():
    """Connects to the PostgreSQL database and returns the connection and cursor."""
    try:
        # Connect to your postgres DB
        conn = psycopg2.connect(
            dbname="project_database",
            user="postgres",
            password="",
            host="localhost"
        )
        # Open a cursor to perform database operations
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        print(f"An error occurred while connecting to the database: {e}")
        return None, None



def insert_data(record, cur):
    # Extract data from record
    cur.execute("INSERT INTO competition (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING", (record['competition_id'], record['competition_name']))
    cur.execute("INSERT INTO season (id, name) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING", (record['season_id'], record['season_name']))

def main():
    # Directory path to your JSON files
    json_dir_path = ''
    
    # File name
    json_file = 'competitions.json'
    
    # Construct full file path
    json_file_path = os.path.join(json_dir_path, json_file)
    
    # Connect to PostgreSQL
    conn, cur = connect_postgres()
    
    if conn is not None and cur is not None:
        try:
            # Check if file exists
            if os.path.isfile(json_file_path):
                data = read_json(json_file_path)
                for record in data:
                    # Insert data into the database
                    insert_data(record, cur)
            
            # Commit the transaction
            conn.commit()
        except Exception as e:
            print(f"An error occurred while inserting data: {e}")
            # Rollback in case of error
            conn.rollback()
        finally:
            # Close communication with the database
            cur.close()
            conn.close()
if __name__ == "__main__":
    main()
