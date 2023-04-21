from pathlib import Path
import settings
from read_write import write_file, read_data
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

def task():

    df = read_data(settings.PATH_TO_TITLE_BASICS)

    df_filter = df.filter(col("runtimeMinutes").cast(IntegerType()) >= 120)
    file_path = Path('imdb_out/task3.csv')
    write_file(df_filter, file_path)
