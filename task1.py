from pathlib import Path
import settings
from read_write import write_file, read_data
from pyspark.sql.functions import col


def task():
    '''
    Get all titles of series/movies etc. that are available in Ukrainian.
    '''

    # Load the title.akas.tsv.gz
    df = read_data(settings.PATH_TO_TITLE_AKAS)

    # Filter "region" == "UA" and "title" not null
    ukr_movies = df.filter((col("region") == "UA") & (col("title").isNotNull()))

    # Save results
    file_path = Path('imdb_out/task1.csv')
    write_file(ukr_movies, file_path)
