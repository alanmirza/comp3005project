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
            password="1234",
            host="localhost"
        )
        # Open a cursor to perform database operations
        cur = conn.cursor()
        return conn, cur
    except Exception as e:
        print(f"An error occurred while connecting to the database: {e}")
        return None, None



def insert_data(match_id, record, cur):
    # Extract data from record
    lineup = record.get('lineup')
    team_id = record.get('team_id')
    for player in lineup:
        cur.execute("INSERT INTO player (id, name, nickname, country) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", (player['player_id'], player['player_name'], player['player_nickname'], player['country'].get('name')))
        cur.execute("INSERT INTO match_player (player_id, match_id, jersey_number) VALUES (%s, %s, %s)", (player['player_id'], match_id, player['jersey_number']))
        cur.execute("INSERT INTO team_player (team_id, player_id) VALUES (%s, %s) ON CONFLICT (team_id, player_id) DO NOTHING", (team_id, player['player_id']))


def main():
    # Directory path to your JSON files
    json_dir_path = 'lineups'
    
    # List all files in the directory
    json_files = [f for f in os.listdir(json_dir_path) if f.endswith('.json')]
    
    # Connect to PostgreSQL
    conn, cur = connect_postgres()
    
    if conn is not None and cur is not None:
        try:
            # Loop over all JSON files
            for json_file in json_files:
                # Construct full file path
                json_file_path = os.path.join(json_dir_path, json_file)
                filename_without_extension = int(os.path.splitext(json_file)[0])
                # Read data from JSON file

                # Check if filename_without_extension exists in match_ids
                if filename_without_extension in match_ids:
                    data = read_json(json_file_path)
                    for record in data:
                        # Insert data into the database
                        insert_data(filename_without_extension, record, cur)

                # Insert data into the database
                # Make sure to adapt this to your specific data format
                # insert_data(filename_without_extension, data, cur)
            
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
