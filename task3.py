from pathlib import Path
import settings
from read_write import write_file, read_data
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType


def task():
    '''
    Get titles of all movies that last more than 2 hours.
    '''

    # Load the title.basics.tsv.gz
    df = read_data(settings.PATH_TO_TITLE_BASICS)

    # Filter runtimeMinutes to be greater than 120 min, having cast to int before
    df_filter = df.filter(col("runtimeMinutes").cast(IntegerType()) >= 120)

    # Save results
    file_path = Path('imdb_out/task3.csv')
    write_file(df_filter, file_path)
