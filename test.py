from geolife import ExampleProgram
from tabulate import tabulate

program = None
try:
    program = ExampleProgram()
    query = """
        SELECT YEAR(start_date_time) AS activity_year, 
        SUM(TIMESTAMPDIFF(HOUR, start_date_time, end_date_time)) AS total_hours
        FROM Activity
        GROUP BY YEAR(start_date_time)
        ORDER BY total_hours DESC;
        """
    program.cursor.execute(query)
    rows = program.cursor.fetchall()
    print(tabulate(rows, headers=program.cursor.column_names))

except Exception as e:
    print("ERROR: Failed to use database:", e)
finally:
    if program:
        program.connection.close_connection()


