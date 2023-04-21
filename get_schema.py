import os
import settings
from read_write import read_data


def print_schemas():
    for file in os.listdir(settings.DATA_DIR):
        if os.path.isfile(os.path.join(settings.DATA_DIR, file)):
            print(f'name: {file}')
            df = read_data(os.path.join(settings.DATA_DIR, file))
            df.printSchema()
            print('--------')