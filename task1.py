from pathlib import Path
import settings
from read_write import write_file, read_data
from pyspark.sql.functions import col

def task():

    df = read_data(settings.PATH_TO_TITLE_AKAS)
    # df.filter().withColumn
    ukr_movies = df.filter((col("region") == "UA") & (col("title").isNotNull()))

    file_path = Path('imdb_out/task1.csv')
    wwrite_file(ukr_movies, file_path)
