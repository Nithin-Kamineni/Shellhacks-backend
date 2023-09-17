import psycopg2
from psycopg2 import sql
from flask import Flask, jsonify, request
from datetime import datetime
from dateutil import parser
# from flask_cors import CORS

app = Flask(__name__)
# CORS(app)

db_params = {
        'password': '1234',
        'user': 'postgres',
        'host': 'localhost',
        'port': '5432',
        'database': 'video_token_db'
    }
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

#http://localhost:8080/api/data?start_timestamp=2023-09-11%2012:00:00&end_timestamp=2023-09-11%2023:00:00
@app.route('/api/data', methods=['GET'])
def get_data():
    start_timestamp = request.args.get('start_timestamp')
    end_timestamp = request.args.get('end_timestamp')
    try:
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = 'scene_table';")

        # Fetch all the column names
        columns = cur.fetchall()
        
        query = "SELECT * FROM scene_table WHERE timestamps >= %s AND timestamps <= %s order by timestamps"
        cur.execute(query,[start_timestamp, end_timestamp])
        results = cur.fetchall()
        response=[]
        for row in results:
            record={}
            for col, val in enumerate(row):
                record[columns[col][0]]=val
            response.append(record)
        return jsonify(response) 
    except Exception as e:
        return jsonify({"error": str(e)})

#http://127.0.0.1:8080/api/timelines
@app.route('/api/timelines', methods=['GET'])
def get_timelines_data():
    # query = sql.SQL('select min(timestamps) as "starting_time", max(timestamps) as "ending_time" from scene_table')
    
    query = sql.SQL('SELECT to_char(min(timestamps), \'YYYY-MM-DD"T"HH24:MI\') as starting_time, to_char(max(timestamps), \'YYYY-MM-DD"T"HH24:MI\') as ending_time FROM scene_table')


    cur.execute(query)
    results = cur.fetchall()
    print(results)
    print("------------")
    # Parse the date strings from the database
    starting_time_str = results[0][0]
    ending_time_str = results[0][1]

    return {"starting_time": starting_time_str, "ending_time": ending_time_str}


#http://127.0.0.1:8080/api/timepersecond?start_timestamp=2023-09-11%2012:00:00&end_timestamp=2023-09-11%2023:00:00
#http://127.0.0.1:8080/api/timepersecond?start_timestamp=2023-09-11%2012:00:00&end_timestamp=2023-09-11%2023:00:00
@app.route('/api/timepersecond', methods=['GET'])
def get_timelines_data_per_second():
    start_timestamp = request.args.get('start_timestamp')
    end_timestamp = request.args.get('end_timestamp')
    cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = 'scene_table';")

    # Fetch all the column names
    columns = cur.fetchall()

    if(start_timestamp==None or end_timestamp==None):
        query = sql.SQL('SELECT distinct(SUBSTRING(cast(timestamps as varchar), 12,8)) as time_data_stamp,sum(people::integer) as people,sum(cars::integer) as cars,sum(buildings::integer) as buildings,sum(handbags::integer) as handbags,sum(umbrellas::integer) as umbrellas, MAX(img_url) as img_str FROM scene_table group by distinct(SUBSTRING(cast(timestamps as varchar), 12,8))')
        cur.execute(query)
    else:
        query = sql.SQL('SELECT distinct(SUBSTRING(cast(timestamps as varchar), 12,8)) as time_data_stamp,sum(people::integer) as people,sum(cars::integer) as cars,sum(buildings::integer) as buildings,sum(handbags::integer) as handbags,sum(umbrellas::integer) as umbrellas, MAX(img_url) as img_str FROM scene_table WHERE timestamps >= %s AND timestamps <= %s group by distinct(SUBSTRING(cast(timestamps as varchar), 12,8)) order by time_data_stamp')
        cur.execute(query,[start_timestamp,end_timestamp])
    results = cur.fetchall()
    response=[]
    for row in results:
        record={}
        for col, val in enumerate(row):
            record[columns[col][0]]=val
        response.append(record)
    return jsonify(response) 

if __name__ == '__main__':
    app.run(debug=True, port=8080)
    cur.close()
    conn.close()