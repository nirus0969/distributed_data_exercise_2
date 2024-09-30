from DbConnector import DbConnector
from tabulate import tabulate
from datetime import datetime
from itertools import islice
import os


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor
        self.users_with_labels: list[str] = self.initialize_users_with_labels()
        # self.valid_files: dict[str, bool] = self.initialize_valid_files()

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

    def number_of_rows_in_tables(self) -> None:
        query = """
        SELECT COUNT(DISTINCT User.id) AS user_count, 
        COUNT(DISTINCT Activity.id) AS activity_count, 
        COUNT(DISTINCT TrackPoint.id) AS trackpoint_count
        FROM User
        JOIN Activity ON User.id = Activity.user_id
        JOIN TrackPoint ON Activity.id = TrackPoint.activity_id;
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def avg_activites_per_user(self) -> None:
        query = """
        SELECT AVG(activity_count) AS avg_activities_per_user
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

    def top_20_users_most_activites(self) -> None:
        query = """
        SELECT User.id, COUNT(Activity.id) AS activity_count
        FROM User 
        LEFT JOIN Activity ON User.id = Activity.user_id
        GROUP BY User.id
        ORDER BY activity_count DESC
        LIMIT 20
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def users_taken_taxi(self) -> None:
        query = """
        SELECT DISTINCT User.id
        FROM User 
        LEFT JOIN Activity ON User.id = Activity.user_id
        WHERE Activity.transportation_mode= 'taxi'
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def transportation_modes_and_count(self) -> None:
        query = """
        SELECT transportation_mode, COUNT(id)
        FROM Activity
        WHERE NOT transportation_mode= 'None'
        GROUP BY transportation_mode
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))



def main():
    program = None
    try:
        program = ExampleProgram()
        program.top_20_users_most_activites()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
