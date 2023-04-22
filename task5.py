import settings
from read_write import write_file, read_data
from pyspark.sql.functions import col
from pathlib import Path
from pyspark.sql.functions import when


def task():
    '''
    Get information about how many adult movies/series etc. there are per
    region. Get the top 100 of them from the region with the biggest count to
    the region with the smallest one.
    '''

    # Load the title.akas.tsv.gz file
    akas_df = read_data(settings.PATH_TO_TITLE_AKAS)

    # Load the title.basics.tsv.gz file
    basics_df = read_data(settings.PATH_TO_TITLE_BASICS)

    # Check the right names of "titleType"
    # a = basics_df.groupBy("titleType").count()
    # a.show()

    # Join the dataframes
    joined_df = akas_df.join(basics_df, akas_df["titleId"] == basics_df["tconst"])

    # Filter the data to keep only adult movies/series
    adult_df = joined_df.filter((col("isAdult") == 1) & (col("titleType").isin(["movie", "tvSeries"])))

    # Group the data by region and count the number of adult titles per region
    adult_df = adult_df.withColumn("region", when(col("region") == "\\N", None).otherwise(col("region")))

    # Group and sort the data by the count in descending order
    # region_count_df = adult_df.groupBy("region").count()
    # sorted_df = region_count_df.sort("count", ascending=False)
    region_count_df = adult_df.filter(col("region").isNotNull()).groupBy("region").count()
    sorted_df = region_count_df.sort("count", ascending=False)

    # Get the top 100 rows
    top_100_df = sorted_df.limit(100)

    # Show the results
    file_path = Path('imdb_out/task5.csv')
    write_file(top_100_df, file_path)
