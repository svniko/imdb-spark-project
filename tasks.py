from read_write import write_file
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType


def task1(df):
    """
    Get all titles of series/movies etc. that are available in Ukrainian.
    """

    # write_file(ukr_movies, file_path)

    # Filter "region" == "UA" and "title" not null
    df_filter = df.filter((col("region") == "UA") & (col("title").isNotNull()))
    df_filter = df_filter.select("title", "region")
    return df_filter


def task2(df):
    """
    Get the list of people’s names, who were born in the 19th century
    """

    # Change datatype from str to int
    df = df.withColumn("birthYear", df["birthYear"].cast(IntegerType()))

    # Filter birthYear to be in 19 century
    born_in_1800 = df.filter((col("birthYear") > 1800) & (col("birthYear") <= 1900))
    born_in_1800 = born_in_1800.select("primaryName", "birthYear", "deathYear", "primaryProfession")

    return born_in_1800


def task3(df):
    """
    Get titles of all movies that last more than 2 hours.
    """
    # Filter runtimeMinutes to be greater than 120 min, having cast to int before
    df_filter = df.filter(col("runtimeMinutes").cast(IntegerType()) >= 120)
    df_filter = df_filter.select("primaryTitle", "runtimeMinutes")
    return df_filter
