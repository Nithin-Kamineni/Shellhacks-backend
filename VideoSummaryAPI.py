import psycopg2
from psycopg2 import sql
from flask import Flask, jsonify, request

app = Flask(__name__)

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
    query = sql.SQL('select min(timestamps) as "starting_time", max(timestamps) as "ending_time" from scene_table')
    cur.execute(query)
    results = cur.fetchall()
    return {"starting_time":results[0][0], "ending_time":results[0][1]}

if __name__ == '__main__':
    app.run(debug=True, port=8080)
    cur.close()
    conn.close()