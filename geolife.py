from DbConnector import DbConnector
from tabulate import tabulate
from datetime import datetime
from itertools import islice
from haversine import haversine
import os


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        # Comment out the two lines directly below after using insert_activity_data() and insert_trackpoint_data()
        self.users_with_labels: list[str] = self.initialize_users_with_labels()
        self.valid_files: dict[str, bool] = self.initialize_valid_files()

    def create_tables(self) -> None:
        user_table_query = """CREATE TABLE IF NOT EXISTS User (
                        id VARCHAR(255) NOT NULL,
                        has_labels BOOLEAN,
                        PRIMARY KEY (id)
                    );
                """
        activity_table_query = """CREATE TABLE IF NOT EXISTS Activity (
                        id INT NOT NULL AUTO_INCREMENT,
                        user_id VARCHAR(255),
                        transportation_mode VARCHAR(255),
                        start_date_time DATETIME,
                        end_date_time DATETIME,
                        PRIMARY KEY (id),
                        FOREIGN KEY (user_id) REFERENCES User(id)
                    );
                """
        trackpoint_table_query = """CREATE TABLE IF NOT EXISTS TrackPoint (
                        id INT NOT NULL AUTO_INCREMENT,
                        activity_id INT,
                        lat DOUBLE,
                        lon DOUBLE,
                        altitude INT,
                        date_time DATETIME,
                        PRIMARY KEY (id),
                        FOREIGN KEY (activity_id) REFERENCES Activity(id)
                    );
                """
        self.cursor.execute(user_table_query)
        self.cursor.execute(activity_table_query)
        self.cursor.execute(trackpoint_table_query)
        self.db_connection.commit()

    def initialize_valid_files(self) -> dict[str, bool]:
        valid_files = {}
        for dirpath, dirnames, filenames in os.walk("dataset/dataset/Data"):
            for filename in filenames:
                if filename.endswith('.plt'):
                    full_path = os.path.join(dirpath, filename)
                    line_count = 0
                    with open(full_path, 'r') as file:
                        for line in file:
                            line_count += 1
                            if line_count > 2506:
                                valid_files[full_path] = False
                                break
                    if line_count <= 2506:
                        if line_count <= 6:
                            valid_files[full_path] = False
                        else:
                            valid_files[full_path] = True
        return valid_files

    def valid_file(self, filename: str) -> bool:
        return self.valid_files[filename]

    def initialize_users_with_labels(self) -> list[str]:
        users_with_labels = []
        with open("dataset/dataset/labeled_ids.txt", "r") as file:
            for line in file:
                users_with_labels.append(line.strip())
        return users_with_labels

    def user_has_labels(self, user_id: str) -> bool:
        return user_id in self.users_with_labels

    def insert_user_data(self) -> None:
        count = 0
        directory = "dataset/dataset/Data"
        for entry in os.listdir(directory):
            full_path = os.path.join(directory, entry)
            if os.path.isdir(full_path):
                count += 1
                print(str(count) + "/182")
                has_label = self.user_has_labels(user_id=entry)
                query = "INSERT IGNORE INTO User (id, has_labels) VALUES (%s, %s)"
                data_tuple = (entry, has_label)
                self.cursor.execute(query, data_tuple)
            self.db_connection.commit()

    def find_matching_label(self, user: str, start_end_datetime: tuple[datetime, datetime]) -> str | None:
        filename = "dataset/dataset/Data/" + user + "/labels.txt"

        start_time, end_time = start_end_datetime

        with open(filename, 'r') as file:
            lines = file.readlines()

        for line in lines[1:]:
            column = line.strip().split('\t')
            label_start_time = datetime.strptime(
                column[0], '%Y/%m/%d %H:%M:%S')
            label_end_time = datetime.strptime(column[1], '%Y/%m/%d %H:%M:%S')
            transportation_mode = column[2]

            if label_start_time == start_time and label_end_time == end_time:
                return transportation_mode

        return None

    def get_first_last_datetime(self, filename: str) -> tuple[datetime, datetime]:
        first_datetime = None
        last_datetime = None

        with open(filename, 'r') as file:
            for line_number, line in enumerate(file):
                if line_number < 6:
                    continue
                line = line.strip()
                fields = line.split(",")
                if len(fields) < 7:
                    continue
                current_datetime = datetime.strptime(
                    f"{fields[-2]} {fields[-1]}", '%Y-%m-%d %H:%M:%S'
                )
                if first_datetime is None:
                    first_datetime = current_datetime
                last_datetime = current_datetime

        return first_datetime, last_datetime

    def insert_activity_data(self) -> None:
        activities_to_insert = []
        count = 0
        for dirpath, dirnames, filenames in os.walk("dataset/dataset/Data"):
            user = dirpath[-14:-11]
            for filename in filenames:
                if filename.endswith('.plt'):
                    full_path = os.path.join(dirpath, filename)
                    if self.valid_file(filename=full_path):
                        count += 1
                        print(str(count) + "/16048")
                        start_end_datetime = self.get_first_last_datetime(
                            filename=full_path)
                        label = None
                        if self.user_has_labels(user_id=user):
                            label = self.find_matching_label(
                                user=user, start_end_datetime=start_end_datetime)
                        activities_to_insert.append(
                            (user, label, start_end_datetime[0], start_end_datetime[1]))
        batch_size = 1000
        count = 0
        if activities_to_insert:
            for i in range(0, len(activities_to_insert), batch_size):
                count += batch_size
                print(str(count) + "/" + str(len(activities_to_insert)))
                batch = activities_to_insert[i:i + batch_size]
                query = """
                    INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time)
                    VALUES (%s, %s, %s, %s);
                """
                self.cursor.executemany(query, batch)
            print("Committing changes")
            self.db_connection.commit()

    def insert_trackpoint_data(self) -> None:
        trackpoints_to_insert = []
        count = 0
        for dirpath, dirnames, filenames in os.walk("dataset/dataset/Data"):
            user = dirpath[-14:-11]
            for filename in filenames:
                if filename.endswith('.plt'):
                    full_path = os.path.join(dirpath, filename)
                    if self.valid_file(filename=full_path):
                        count += 1
                        print(str(count) + "/16048")
                        start_end_datetime = self.get_first_last_datetime(
                            filename=full_path)
                        query = "SELECT id FROM Activity WHERE user_id = %s AND start_date_time = %s AND end_date_time = %s"
                        self.cursor.execute(
                            query, (user, start_end_datetime[0], start_end_datetime[1]))
                        result = self.cursor.fetchone()
                        if result:
                            activity_id = result[0]
                            with open(full_path, 'r') as file:
                                for line in islice(file, 6, None):  # Skip first 6 lines
                                    line = line.strip()
                                    fields = line.split(",")
                                    if len(fields) < 7:
                                        continue
                                    datetime_obj = datetime.strptime(
                                        f"{fields[5]} {fields[6]}", '%Y-%m-%d %H:%M:%S')
                                    trackpoints_to_insert.append(
                                        (activity_id, fields[0], fields[1], fields[3], datetime_obj))
        batch_size = 10000
        count = 0
        if trackpoints_to_insert:
            for i in range(0, len(trackpoints_to_insert), batch_size):
                count += batch_size
                print(str(count) + "/" + str(len(trackpoints_to_insert)))
                batch = trackpoints_to_insert[i:i + batch_size]
                query = """
                    INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_time)
                    VALUES (%s, %s, %s, %s, %s);
                """
                self.cursor.executemany(query, batch)
            print("Committing changes")
            self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s LIMIT 10"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def show_fields(self, table_name) -> None:
        query = "DESCRIBE %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def drop_table(self, table_name) -> None:
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self) -> None:
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_1(self) -> None:
        query = """
        SELECT 
            (SELECT COUNT(*) FROM User) AS user_count,
            (SELECT COUNT(*) FROM Activity) AS activity_count,
            (SELECT COUNT(*) FROM TrackPoint) AS trackpoint_count;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_2(self) -> None:
        query = """
        SELECT AVG(activity_count)
        FROM (
            SELECT User.id, COUNT(Activity.id) AS activity_count
            FROM User
            LEFT JOIN Activity ON User.id = Activity.user_id
            GROUP BY User.id
        ) AS user_activity_count;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_3(self) -> None:
        query = """
        SELECT User.id, COUNT(Activity.id) AS activity_count
        FROM User 
        LEFT JOIN Activity ON User.id = Activity.user_id
        GROUP BY User.id
        ORDER BY activity_count DESC
        LIMIT 20;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_4(self) -> None:
        query = """
        SELECT DISTINCT User.id
        FROM User 
        LEFT JOIN Activity ON User.id = Activity.user_id
        WHERE Activity.transportation_mode= 'taxi';
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_5(self) -> None:
        query = """
        SELECT transportation_mode, COUNT(id)
        FROM Activity
        WHERE NOT transportation_mode= 'None'
        GROUP BY transportation_mode;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_6a(self) -> None:
        query = """
        SELECT YEAR(start_date_time), COUNT(*) AS activity_count
        FROM Activity
        GROUP BY YEAR(start_date_time)
        ORDER BY activity_count DESC;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_6b(self) -> None:
        query = """
        SELECT YEAR(start_date_time) AS activity_year, 
        SUM(TIMESTAMPDIFF(MINUTE, start_date_time, end_date_time) / 60) AS total_hours
        FROM Activity
        GROUP BY YEAR(start_date_time)
        ORDER BY total_hours DESC;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_7(self) -> None:
        query = """
        SELECT TrackPoint.lat, TrackPoint.lon, Activity.id, TrackPoint.date_time, Activity.transportation_mode
        FROM User
        JOIN Activity ON User.id = Activity.user_id
        JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
        WHERE User.id = '112' 
        AND YEAR(TrackPoint.date_time) = 2008
        AND Activity.transportation_mode = 'walk'
        ORDER BY Activity.id DESC, TrackPoint.date_time ASC;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        distances = {}
        last_activity_id = None
        last_coordinates = None

        for row in rows:
            lat, lon, activity_id, date_time, transportation_mode = row
            coordinates = (lat, lon)

            if activity_id not in distances:
                distances[activity_id] = 0.0
                last_coordinates = None

            if last_coordinates is not None and last_activity_id == activity_id:
                distance = haversine(last_coordinates, coordinates)
                distances[activity_id] += distance

            last_coordinates = coordinates
            last_activity_id = activity_id

        print(f"Total distance walked in 2008 by user 112: {sum(distances.values())} km")

    def task_8(self) -> None:
        query = """
        SELECT User.id, Activity.id, TrackPoint.altitude, TrackPoint.date_time
        FROM User
        JOIN Activity ON User.id = Activity.user_id
        JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
        WHERE NOT TrackPoint.altitude = -777
        ORDER BY User.id DESC, Activity.id ASC, TrackPoint.date_time ASC;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        altitude_gain = {}
        last_user = None
        last_altitude = None
        last_activity = None

        for row in rows:
            user, activity, altitude, _ = row

            if user not in altitude_gain:
                altitude_gain[user] = 0.0
                last_altitude = None
                last_activity = None

            if last_user == user and last_activity != activity:
                last_altitude = None

            if last_user == user and last_activity == activity:
                if last_altitude is not None and last_altitude < altitude:
                    altitude_gain[user] += altitude - last_altitude

            last_altitude = altitude
            last_user = user
            last_activity = activity

        sorted_altitude_gain = sorted(
            altitude_gain.items(), key=lambda x: x[1], reverse=True)
        headers = ["user", "altitude gained"]
        print(tabulate(sorted_altitude_gain[:20],
              headers=headers, floatfmt=".4f"))

    def task_9(self) -> None:
        query = """
            WITH InvalidActivities AS (
                SELECT 
                    Activity.user_id,
                    Activity.id AS activity_id
                FROM Activity
                JOIN TrackPoint T_1 ON Activity.id = T_1.activity_id
                JOIN TrackPoint T_2 ON Activity.id = T_2.activity_id
                WHERE T_1.id = T_2.id - 1
                AND TIMESTAMPDIFF(MINUTE, T_1.date_time, T_2.date_time) >= 5
                GROUP BY Activity.id, Activity.user_id
            )
            SELECT 
                InvalidActivities.user_id,
                COUNT(InvalidActivities.activity_id) AS invalid_activity_count
            FROM InvalidActivities
            GROUP BY InvalidActivities.user_id
            ORDER BY InvalidActivities.user_id DESC;
            """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def task_10(self) -> None:
        query = """
        SELECT DISTINCT User.id
        FROM User
        JOIN Activity ON User.id = Activity.user_id
        JOIN TrackPoint ON Activity.id = TrackPoint.activity_id
        WHERE TrackPoint.lat LIKE '39.916%' 
        AND TrackPoint.lon LIKE '116.397%';
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names, floatfmt=".6f"))

    def task_11(self) -> None:
        query = """
        WITH TransportationCount AS (
            SELECT 
                User.id,
                Activity.transportation_mode,
                COUNT(Activity.transportation_mode) AS mode_count
            FROM User
            JOIN Activity ON User.id = Activity.user_id
            WHERE User.has_labels = true AND Activity.transportation_mode IS NOT NULL
            GROUP BY User.id, Activity.transportation_mode
        ),
        RankedTransportation AS (
            SELECT 
                id, 
                transportation_mode, 
                mode_count,
                ROW_NUMBER() OVER (PARTITION BY id ORDER BY mode_count DESC) AS mode_rank
            FROM TransportationCount
        ) 
        SELECT 
            id, 
            transportation_mode AS most_used_transportation_mode
        FROM RankedTransportation
        WHERE mode_rank = 1
        ORDER BY id ASC;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = ExampleProgram()
        program.task_9()  # Change this to whatever task you want displayed

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
