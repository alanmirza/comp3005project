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

def insert_data(competition_id, season_id, record, cur):
    # Extract data from record
    stadium = record.get('stadium')
    referee = record.get('referee')
    home_team = record.get('home_team')
    away_team = record.get('away_team')
    home_manager = home_team.get('manager') if home_team else None
    away_manager = away_team.get('manager') if away_team else None

    if stadium:
        cur.execute("INSERT INTO stadium (id, name, country) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (stadium.get('id'), stadium.get('name'), stadium.get('country').get('name') if stadium.get('country') else None))
    
    if referee:
        cur.execute(
        "INSERT INTO referee (id, name, country) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING", 
        (referee.get('id'), referee.get('name'), referee.get('country').get('name') if referee.get('country') else None)
        )

    if home_manager:
        cur.execute(
            "INSERT INTO manager (id, name, country, nickname, date_of_birth) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
            (home_manager.get('id'), home_manager.get('name'), home_manager.get('country').get('name') if home_manager.get('country') else None, home_manager.get('nickname'), home_manager.get('date_of_birth'))
        )
    if away_manager:
        cur.execute(
            "INSERT INTO manager (id, name, country, nickname, date_of_birth) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
            (away_manager.get('id'), away_manager.get('name'), away_manager.get('country').get('name') if away_manager.get('country') else None, away_manager.get('nickname'), away_manager.get('date_of_birth'))
        )

    if home_team:
        cur.execute("INSERT INTO team (id, name, country, manager_id) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
                    (home_team.get('home_team_id'), home_team.get('home_team_name'), home_team.get('country').get('name') if home_team.get('country') else None, home_manager.get('id') if home_manager else None))
    if away_team:
        cur.execute("INSERT INTO team (id, name, country, manager_id) VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING",
                    (away_team.get('away_team_id'), away_team.get('away_team_name'), away_team.get('country').get('name') if away_team.get('country') else None, away_manager.get('id') if away_manager else None))
    
    cur.execute(
        "INSERT INTO match (id, competition_id, season_id, date, kickoff_time, stadium_id, referee_id, home_team_id, away_team_id, home_score, away_score, week) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
        (record.get('match_id'), competition_id, season_id, record.get('match_date'), record.get('kick_off'), stadium.get('id') if stadium else None, referee.get('id') if referee else None, home_team.get('home_team_id') if home_team else None, away_team.get('away_team_id') if away_team else None, record.get('home_score'), record.get('away_score'), record.get('match_week'))
    )

def main():
    # Directory path to your JSON files
    json_dir_path = 'matches'
    
    # Connect to PostgreSQL
    conn, cur = connect_postgres()
    
    if conn is not None and cur is not None:
        try:
            # Loop over all subdirectories and files in the directory
            for root, dirs, files in os.walk(json_dir_path):
                for file in files:
                    if file.endswith('.json'):
                        # Construct full file path
                        json_file_path = os.path.join(root, file)
                        # The parent directory name is the competition_id
                        competition_id = int(os.path.basename(root))
                        # The file name without extension is the match_id
                        match_id = int(os.path.splitext(file)[0])
                        # Read data from JSON file
                        data = read_json(json_file_path)
                        for record in data:
                            # Insert data into the database
                            insert_data(competition_id, match_id, record, cur)
            
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
