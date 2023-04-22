from pathlib import Path
import settings
from read_write import write_file, read_data
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType


def task():
    '''
    Get the list of people’s names, who were born in the 19th century
    '''

    # Load the name.basics.tsv.gz
    df = read_data(settings.PATH_TO_NAME_BASICS)

    # Change datatype from str to int
    df = df.withColumn("birthYear", df["birthYear"].cast(IntegerType()))

    # Filter birthYear to be in 19 century
    born_in_1800 = df.filter((col("birthYear") > 1800) & (col("birthYear") <= 1900))

    # Save results
    file_path = Path('imdb_out/task2.csv')
    write_file(born_in_1800, file_path)
