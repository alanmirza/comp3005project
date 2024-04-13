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
    """Inserts data into the database. Modify this function based on your schema."""
    type = record.get('type').get('name')

    play_pattern = record.get('play_pattern')
    play_pattern_name = play_pattern.get('name') if play_pattern else None
    player = record.get('player')
    player_id = player.get('id') if player else None

    if(type == 'Shot'):
        cur.execute("INSERT INTO event_shot (event_id, technique, body_part, outcome, statsbomb_xg, first_time) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), technique.get(record.get('shot').get('technique').get('name')), body_part_map.get(record.get('shot').get('body_part').get('name')), pass_outcome_map.get(record.get('shot').get('outcome').get('name')), record.get('shot').get('statsbomb_xg'), record.get('shot').get('first_time', False)))
    if (type == 'Dribble'):
        dribble_data = record.get('dribble')
        if dribble_data:
            outcome = dribble_data.get('outcome')
            outcome_name = outcome.get('name') if outcome else None
            cur.execute("INSERT INTO event_dribble (event_id, outcome, nutmeg, overrun, no_touch) VALUES (%s, %s, %s, %s, %s)", (record.get('id'), pass_outcome_map.get(outcome_name, None), dribble_data.get('nutmeg', False), dribble_data.get('overrun', False), dribble_data.get('no_touch', False)))
    if (type == 'Block'):
        block = record.get('block')
        counterpress = block.get('save_block') if block else False
        save_block = block.get('save_block') if block else False
        deflection = block.get('save_block') if block else False
        offensive = block.get('save_block') if block else False
        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        cur.execute("INSERT INTO event_block (event_id, counterpress, save_block, deflection, offensive) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), counterpress, save_block, deflection, offensive ))

    if (type == 'Interception'):
        interception_data = record.get('interception')
        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
                (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))

        if interception_data:
            outcome = interception_data.get('outcome')
            outcome_name = outcome.get('name') if outcome else None
            cur.execute("INSERT INTO event_interception (event_id, outcome) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), pass_outcome_map.get(outcome_name)))

    if (type == 'Duel'):
        duel_data = record.get('duel')
        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
                (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))

        if duel_data:
            outcome = duel_data.get('outcome')
            outcome_name = outcome.get('name') if outcome else None
            counterpress = record.get('counterpress', False)
            duel_type = duel_data.get('type')
            duel_type_name = duel_type.get('name') if duel_type else None
            cur.execute("INSERT INTO event_duel (event_id, outcome, counterpress, duel_type) VALUES (%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), pass_outcome_map.get(outcome_name), counterpress, duel_type_map.get(duel_type_name)))

    if (type == 'Carry'):
        carry_data = record.get('carry')
        end_location = carry_data.get('end_location')
        if end_location:
            end_location = "({}, {})".format(end_location[0], end_location[1])
        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        cur.execute("INSERT INTO event_carry (event_id, end_location) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), end_location))

    if(type == 'Dribbled Past'):
        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        cur.execute("INSERT INTO event_dribble_past (event_id, counterpress) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), record.get('counterpress', False)))

    if(type == '50-50'):
        outcome = record.get('outcome')
        outcome_name = outcome.get('name') if outcome else None
        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        cur.execute("INSERT INTO event_50_50 (event_id, counterpress, outcome) VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), record.get('counterpress', False), pass_outcome_map.get(outcome_name, None)))

    if(type == 'Bad Behaviour'):
        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        cur.execute("INSERT INTO event_bad_behaviour (event_id, card) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), card_map.get(record.get('bad_behaviour').get('card').get('name'), None)))

    if (type == 'Ball Receipt*'):
        outcome = record.get('ball_receipt')
        outcome_name = outcome.get('outcome').get('name') if outcome else None
        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        cur.execute("INSERT INTO event_ball_receipt (event_id, outcome) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), pass_outcome_map.get(outcome_name, None)))

    if(type == 'Ball Recovery'):
        ball_recovery = record.get('ball_recovery')
        offensive = ball_recovery.get('offensive') if ball_recovery else False
        recovery_failure = ball_recovery.get('recovery_failure') if ball_recovery else False

        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        cur.execute("INSERT INTO event_ball_recovery (event_id, offensive, recovery_failure) VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), offensive, recovery_failure))

    if(type == 'Substitution'):
        substitution = record.get('substitution')
        replacement = substitution.get('replacement').get('id') if substitution else None
        outcome = substitution.get('outcome').get('name') if substitution else None
        
        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
                (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        cur.execute("INSERT INTO event_substitution (event_id, outcome, replacement) VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), pass_outcome_map.get(outcome, None), replacement))

    if(type == 'Pressure'):
        pressure = record.get('pressure')
        counterpress = pressure.get('counterpress') if pressure else False

        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        cur.execute("INSERT INTO event_pressure (event_id, counterpress) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), counterpress))

    if(type == 'Player Off'):
        player_off = record.get('player_off')
        permanent = player_off.get('permanent') if player_off else False

        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        cur.execute("INSERT INTO event_player_off (event_id, permanent) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), permanent))

    if(type == 'Miscontrol'):
        miscontrol = record.get('miscontrol')
        aerial_won = miscontrol.get('aerial_won') if miscontrol else False

        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        cur.execute("INSERT INTO event_miscontrol (event_id, aerial_won) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), aerial_won))

    if(type == 'Injury Stoppage'):
        injury_stoppage = record.get('injury_stoppage')
        in_chain = injury_stoppage.get('in_chain') if injury_stoppage else False

        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        cur.execute("INSERT INTO event_injury_stoppage (event_id, in_chain) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), in_chain))

    if(type == 'Half Start'):
        half_start = record.get('half_start')
        late_video_start = half_start.get('late_video_start') if half_start else False

        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        cur.execute("INSERT INTO event_half_start (event_id, late_video_start) VALUES (%s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), late_video_start))

    if(type == 'Half End'):
        half_end = record.get('half_end')
        early_video_end = half_end.get('early_video_end') if half_end else False
        match_suspended = half_end.get('match_suspended') if half_end else False

        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        cur.execute("INSERT INTO event_half_end (event_id, early_video_end, match_suspended) VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), early_video_end, match_suspended))

    if(type == 'Clearance'):
        clearance_data = record.get('clearance')
        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        if clearance_data:
            body_part = clearance_data.get('body_part')
            body_part_name = body_part.get('name') if body_part else None
            cur.execute("INSERT INTO event_clearance (event_id, aerial_won, body_part) VALUES (%s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), clearance_data.get('aerial_won', False), body_part_map.get(body_part_name)))

    if(type == 'Foul Committed'):
        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))

        foul_committed = record.get('foul_committed')
        if foul_committed is not None:
            card_name = foul_committed.get('card').get('name') if foul_committed.get('card') is not None else None
            counterpress = foul_committed.get('counterpress', False)
            offensive = foul_committed.get('offensive', False)
            advantage = foul_committed.get('advantage', False)
            penalty = foul_committed.get('penalty', False)
            foul_type_name = foul_committed.get('type').get('name') if foul_committed.get('type') is not None else None
        else:
            card_name = counterpress = offensive = advantage = penalty = foul_type_name = None

        cur.execute("INSERT INTO event_foul_committed (event_id, card, counterpress, offensive, advantage, penalty, foul_type) VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", 
                    (record.get('id'), card_map.get(card_name), counterpress, offensive, advantage, penalty, foul_type_map.get(foul_type_name)))

    if(type == 'Foul Won'):
        foun_won_data = record.get('foul_won')
        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        if foun_won_data:
            defensive = foun_won_data.get('defensive', False)
            advantage = foun_won_data.get('advantage', False)
            penalty = foun_won_data.get('penalty', False)
            cur.execute("INSERT INTO event_foul_won (event_id, defensive, advantage, penalty ) VALUES (%s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", (record.get('id'), defensive, advantage, penalty))

    if(type == 'Goal Keeper'):
        goalkeeper_data = record.get('goalkeeper')
        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
            (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))
        if goalkeeper_data:
            body_part = goalkeeper_data.get('body_part')
            body_part_name = body_part.get('name') if body_part else None

            technique = goalkeeper_data.get('technique')
            technique_name = technique.get('name') if technique else None

            outcome = goalkeeper_data.get('outcome')
            outcome_name = outcome.get('name') if outcome else None

            goal_keeper_type = goalkeeper_data.get('type')
            goal_keeper_type_name = goal_keeper_type.get('name') if goal_keeper_type else None

            position = goalkeeper_data.get('position')
            position_name = position.get('name') if position else None

            cur.execute("INSERT INTO event_goal_keeper (event_id, body_part, technique, outcome, goal_keeper_type, position) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING", 
                (record.get('id'), body_part_map.get(body_part_name), techniques.get(technique_name), pass_outcome_map.get(outcome_name) if outcome_name else None, goal_keeper_type_map.get(goal_keeper_type_name) if goal_keeper_type_name else None, position_map.get(position_name) if position_name else None))
    if(type == 'Pass'):
        pass_data = record.get('pass')
        cur.execute("INSERT INTO event (id, match_id, period, timestamp, minute, second, event_type, play_pattern, duration, under_pressure, off_camera, out, player_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING", 
                (record.get('id'), match_id, record.get('period'), record.get('timestamp'), record.get('minute'), record.get('second'), event_type_map.get(type), play_map.get(play_pattern_name), record.get('duration'), record.get('under_pressure', False), record.get('off_camera', False), record.get('out', False), player_id))

        if pass_data:
            end_location = pass_data.get('end_location')
            technique = pass_data.get('technique')
            technique_name = technique.get('name') if technique else None
            body_part = pass_data.get('body_part')
            body_part_name = body_part.get('name') if body_part else None
            pass_type = pass_data.get('type')
            pass_type_name = pass_type.get('name') if pass_type else None
            outcome = pass_data.get('outcome')
            outcome_name = outcome.get('name') if outcome else None
            recipient = pass_data.get('recipient')
            recipient_id = recipient.get('id') if recipient else None

            if end_location:
                end_location = "({}, {})".format(end_location[0], end_location[1])

            cur.execute("""
                INSERT INTO event_pass (
                    event_id, length, angle, height, end_location, backheel, deflected, 
                    miscommunication, pass_cross, cut_back, switch, shot_assist, goal_assist, 
                    body_part, pass_type, outcome, technique, receipient_id
                ) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)                
            """, (
                record.get('id'), pass_data.get('length'), pass_data.get('angle'), height.get(pass_data.get('height').get('name')), end_location, pass_data.get('backheel', False), pass_data.get('deflected', False), 
                pass_data.get('miscommunication', False), pass_data.get('cross', False), pass_data.get('cut_back', False), pass_data.get('switch', False), pass_data.get('shot_assist', False), pass_data.get('goal_assist', False), 
                body_part_map.get(body_part_name), pass_type_map.get(pass_type_name), pass_outcome_map.get(outcome_name, 'Unknown'), techniques.get(technique_name), recipient_id
            ))

play_map = {
    'Regular Play': 'RegularPlay',
    'From Corner': 'FromCorner',
    'From Free Kick': 'FromFreeKick',
    'From Throw In': 'FromThrowIn',
    'Other': 'Other',
    'From Counter': 'FromCounter',
    'From Goal Kick': 'FromGoalKick',
    'From Keeper': 'FromKeeper',
    'From Kick Off': 'FromKickOff'
}
techniques = {
    'Blackheel': 'Backheel',
    'Diving Header': 'DivingHeader',
    'Half Volley': 'HalfVolley',
    'Lob': 'Lob',
    'Normal': 'Normal',
    'Overhead Kick': 'OverheadKick',
    'Volley': 'Volley',
    'Outswinging': 'Outswinging',
    'Inswinging': 'Inswinging',
    'Straight': 'Straight',
    'Through Ball': 'ThroughBall',
    'Diving': 'Diving',
    'Standing': 'Standing'
}

goal_keeper_type_map = {
  'Collected': 'Collected',
  'Goal Conceded': 'GoalConceded',
  'Keeper Sweeper': 'KeeperSweeper',
  'Penalty Conceded': 'PenaltyConceded',
  'Penalty Saved': 'PenaltySaved',
  'Punch': 'Punch',
  'Save': 'Save',
  'Shot Faced': 'ShotFaced',
  'Shot Saved': 'ShotSaved',
  'Smother': 'Smother'
}


height = {
    'Ground Pass': 'GroundPass',
    'Low Pass': 'LowPass',
    'High Pass': 'HighPass'
}

duel_type = {
  'Aerial Lost': 'AerialLost',
  'Tackle': 'Tackle'
}

pass_type_map = {
    'Corner': 'Corner',
    'Free Kick': 'FreeKick',
    'Goal Kick': 'GoalKick',
    'Interception': 'Interception',
    'Kick Off': 'KickOff',
    'Recovery': 'Recovery',
    'Throw In': 'ThrowIn'
}

pass_outcome_map = {
    'Blocked': 'Blocked',
    'Goal': 'Goal',
    'Off T': 'OffT',
    'Post': 'Post',
    'Saved': 'Saved',
    'Wayward': 'Wayward',
    'Saved Off T': 'SavedOffT',
    'Saved To Post': 'SavedToPost',
    'Incomplete': 'Incomplete',
    'Complete': 'Complete',
    'Injury Clearance': 'InjuryClearance',
    'Out': 'Out',
    'Pass Offside': 'PassOffside',
    'Unknown': 'Unknown',
    'Won': 'Won',
    'Lost': 'Lost',
    'Success To Team': 'SuccessToTeam',
    'Success To Opposition': 'SuccessToOpposition',
    'Injury': 'Injury',
    'Tactical': 'Tactical',
    'Success In Play': 'SuccessInPlay',
    'Success Out': 'SuccessOut',
    'Lost In Play': 'LostInPlay',
    'Lost Out': 'LostOut',
    'Success': 'Success',
    'Claim': 'Claim',
    'Clear': 'Clear',
    'Collected Twice': 'CollectedTwice',
    'In Play': 'InPlay',
    'In Play Dangerous': 'InPlayDangerous',
    'In Play Safe': 'InPlaySafe',
    'No Touch': 'NoTouch',
    'Saved Twice': 'SavedTwice',
    'Touched In': 'TouchedIn',
    'Touched Out': 'TouchedOut',
    'Punched Out': 'PunchedOut'
}


body_part_map = {
    'Head': 'Head',
    'Left Foot': 'LeftFoot',
    'Right Foot': 'RightFoot',
    'Other': 'Other'
}

position_map = {
    'Moving': 'Moving',
    'Prone': 'Prone',
    'Set': 'Set'
}
card_map = {
    'Yellow Card': 'YellowCard',
    'Red Card': 'RedCard',
    'Second Yellow': 'SecondYellow',
}


event_type_map = {
    'Ball Receipt*': 'BallReceipt',
    'Ball Recovery': 'BallRecovery',
    'Dispossessed': 'Dispossessed',
    'Duel': 'Duel',
    'Camera On': 'CameraOn',
    'Block': 'Block',
    'Offside': 'Offside',
    'Clearance': 'Clearance',
    'Interception': 'Interception',
    'Dribble': 'Dribble',
    'Shot': 'Shot',
    'Pressure': 'Pressure',
    'Half Start': 'HalfStart',
    'Substitution': 'Substitution',
    'Own Goal Against': 'OwnGoalAgainst',
    'Foul Won': 'FoulWon',
    'Foul Committed': 'FoulCommitted',
    'Goal Keeper': 'GoalKeeper',
    'Bad Behaviour': 'BadBehaviour',
    'Own Goal For': 'OwnGoalFor',
    'Player On': 'PlayerOn',
    'Player Off': 'PlayerOff',
    'Shield': 'Shield',
    'Pass': 'Pass',
    '50/50': '50/50',
    'Half End': 'HalfEnd',
    'Starting XI': 'StartingXI',
    'Tactical Shift': 'TacticalShift',
    'Error': 'Error',
    'Miscontrol': 'Miscontrol',
    'Dribbled Past': 'DribbledPast',
    'Injury Stoppage': 'InjuryStoppage',
    'Referee Ball Drop': 'RefereeBallDrop',
    'Carry': 'Carry'
}

foul_type_map = {
    'Handball': 'Handball',
    'Dive': 'Dive',
    'Dangerous Play': 'DangerousPlay',
    'Foul Out': 'FoulOut',
    '6 Seconds': '6Seconds',
    'Backpass Pick': 'BackpassPick'
}

duel_type_map = {
    'Aerial Lost': 'AerialLost',
    'Tackle': 'Tackle'
}

match_ids = [68314, 68313, 68316, 68315, 69153, 68352, 68353, 15978, 15998, 16029, 16073, 16086, 16190, 16109, 16120, 16182, 16215, 16173, 16265, 15946, 16157, 16136, 16248, 16056, 16289, 16079, 16149, 16275, 15973, 16317, 16131, 16205, 16095, 16306, 16010, 15986, 16240, 16023, 16196, 15956, 16231, 303731, 303532, 303516, 303596, 303430, 303725, 303504, 303451, 303664, 303682, 303400, 303634, 303421, 303493, 303680, 303479, 303615, 303696, 303487, 303600, 303548, 303652, 303674, 303470, 303700, 303707, 303666, 303715, 303377, 303524, 303517, 303610, 303473, 3749052, 3749522, 3749246, 3749257, 3749642, 3749358, 3749346, 3749253, 3749079, 3749465, 3749133, 3749528, 3749233, 3749462, 3749552, 3749296, 3749454, 3749276, 3749068, 3749310, 3749493, 3749434, 3749192, 3749196, 3749448, 3749360, 3749453, 3749278, 3749603, 3749274, 3749590, 3749631, 3749117, 3749108, 3749153, 3749403, 3749526, 3749431]

def main():
    # Directory path to your JSON files
    json_dir_path = 'events'
    
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
