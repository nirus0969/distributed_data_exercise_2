import os
from datetime import datetime

def initialize_valid_files() -> dict[str, bool]:
    valid_files = {}
    for dirpath, _, filenames in os.walk("dataset/dataset/Data"):
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

def get_first_last_datetime(filename: str) -> tuple[datetime, datetime]:
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


valid_file = initialize_valid_files()
count = 0
valid = 0
for dirpath, dirnames, filenames in os.walk("dataset/dataset/Data"):
            user = dirpath[-14:-11]
            for filename in filenames:
               if filename.endswith('.plt'):
                    count += 1
                    print(str(count) + "/18669")
                    full_path = os.path.join(dirpath, filename)
                    if valid_file[full_path]:
                        valid += 1
                        
                            
print("VALID number of files" + str(valid))

