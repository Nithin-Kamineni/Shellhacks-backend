import psycopg2
from psycopg2 import sql
from flask import Flask, jsonify, request
from flask_cors import CORS


app = Flask(__name__)
cors = CORS(app, resources={r"/api/*": {"origins": "http://localhost:3000"}})

db_params = {
    'password': 'Bhanu@001',
    'user': 'postgres',
    'host': 'localhost',
    'port': '5432',
    'database': 'postgres'
}
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Configure CORS to allow requests from http://localhost:3000
# http://localhost:8080/api/data?start_timestamp=2023-09-11%2012:00:00&end_timestamp=2023-09-11%2023:00:00
@app.route('/api/data', methods=['GET'])
def get_data():
    start_timestamp = request.args.get('start_timestamp')
    end_timestamp = request.args.get('end_timestamp')
    try:
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = 'scene_table';")

        # Fetch all the column names
        columns = cur.fetchall()

        query = "SELECT * FROM scene_table WHERE timestamps >= %s AND timestamps <= %s order by timestamps"
        cur.execute(query, [start_timestamp, end_timestamp])
        results = cur.fetchall()
        response = []
        for row in results:
            record = {}
            for col, val in enumerate(row):
                record[columns[col][0]] = val
            response.append(record)
        return jsonify(response)
    except Exception as e:
        return jsonify({"error": str(e)})


# http://127.0.0.1:8080/api/timelines
@app.route('/api/timelines', methods=['GET'])
def get_timelines_data():
    query = sql.SQL('select min(timestamps) as "starting_time", max(timestamps) as "ending_time" from scene_table')
    cur.execute(query)
    results = cur.fetchall()
    return {"starting_time": results[0][0], "ending_time": results[0][1]}


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
    app.run(host = 'localhost', port = 5000, debug = True, threaded = False)
    cur.close()
    conn.close()