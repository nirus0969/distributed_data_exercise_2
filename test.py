from datetime import datetime, timedelta
from geolife import ExampleProgram
from tabulate import tabulate
from haversine import haversine


program = None
try:
    program = ExampleProgram()
    query = """
        SELECT User.id, Activity.id, TrackPoint.date_time
        FROM User
        JOIN Activity ON User.id = Activity.user_id
        JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
        ORDER BY User.id DESC, Activity.id ASC, TrackPoint.date_time ASC;
        """
    program.cursor.execute(query)
    rows = program.cursor.fetchall()
    invalid: dict[set[int]] = {}
    last_user = None
    last_date_time = None
    last_activity = None

    for row in rows:
        user, activity, date_time = row

        if user not in invalid:
            invalid[user] = set()
            last_date_time = None
            last_activity = None

        if last_user == user and last_activity != activity:
            last_date_time = None  

        if last_user == user and last_activity == activity:
            if last_date_time is not None and abs(last_date_time - date_time) > timedelta(minutes=5):
                invalid[user].add(activity)

        last_date_time = date_time
        last_user = user
        last_activity = activity
    
    table_data = []
    for user, activities in invalid.items():
        table_data.append([user, len(activities)]) 

    print(tabulate(table_data, headers=['user', 'invalid activity count']))

   
  


 
except Exception as e:
    print("ERROR: Failed to use database:", e)
finally:
    if program:
        program.connection.close_connection()