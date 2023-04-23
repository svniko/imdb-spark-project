from read_write import write_file
from pyspark.sql.functions import col, collect_list, when, explode, split, mean, desc
from pyspark.sql.types import IntegerType
import pyspark.sql.types as t


def task1(df):
    """
    Get all titles of series/movies etc. that are available in Ukrainian.
    """

    df_filter = df.filter((col("region") == "UA") & (col("title").isNotNull()))
    df_filter = df_filter.select("title")

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


def task4(principals_df, names_df, titles_df):
    """
    Get names of people, corresponding movies/series and characters they
    played in those films.
    """

    # Select necessary columns
    principals_selected_df = principals_df.select("tconst", "nconst", "category", "characters")
    names_selected_df = names_df.select("nconst", "primaryName")
    titles_selected_df = titles_df.select("tconst", "primaryTitle")

    # Filter the data to keep only actors and actresses
    principals_filtered_df = principals_selected_df.filter(col("category").isin("actor", "actress"))

    # Join the principals and names dataframes
    joined_df = principals_filtered_df.join(names_selected_df, "nconst")
    #
    # Join the principals+names dataframe with the titles dataframe
    whole_df = joined_df.join(titles_selected_df, "tconst")

    # Group the data
    # final_df = whole_df.groupBy("primaryName", "primaryTitle", "characters").count()

    # final_df = final_df.drop("count")
    final_df = whole_df.groupBy("primaryName") \
                       .agg(collect_list("primaryTitle").alias("primaryTitle"),
                            collect_list("characters").alias("characters"))

    return final_df


def task5(akas_df, basics_df):
    """
    Get information about how many adult movies/series etc. there are per
    region. Get the top 100 of them from the region with the biggest count to
    the region with the smallest one.
    """

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
    return top_100_df


def task7(title_df, episode_df, akas_df):
    """
    Get information about how many episodes in each TV Series. Get the top
    50 of them starting from the TV Series with the biggest quantity of
    episodes.
     """
    title_df = title_df.select("tconst", "primaryTitle")
    episode_df = episode_df.select("tconst", "parentTconst")
    df_merged = title_df.join(episode_df, "tconst").drop('tconst')
    df_final = (df_merged.withColumnRenamed('primaryTitle', 'primaryTitle_TVSeries')
                         .join(title_df, df_merged["parentTconst"] == title_df["tconst"])
                         .drop("tconst"))
    df_f = df_final.groupBy("primaryTitle").count().sort(desc('count')).limit(50)

    return df_f


def task8(spark_session, basics_df, ratings_df):
    """
    Get 10 titles of the most popular movies/series etc. by each genre.
    !!Let's assume that most popular are those with max averageRating!!
    """
    basics_df = basics_df.withColumn("genres", when(col("genres") == "\\N", None).otherwise(col("genres")))
    basics_df = basics_df.filter(col("genres").isNotNull())
    genres_df = (basics_df.select("tconst", "primaryTitle", explode(split(col("genres"), ",")))
                          .withColumnRenamed("col", "genres"))

    df_merged = genres_df.join(ratings_df, "tconst").drop("tconst", "numVotes")

    # df_merged = df_merged.drop(" const", "numVotes")
    unique_genres = df_merged.toPandas()['genres'].unique()

    # print(unique_genres)

    schema = t.StructType([
        t.StructField("primaryTitle", t.StringType(), True),
        t.StructField("genres", t.StringType(), True),
        t.StructField("averageRating", t.StringType(), True)
    ])
    empty_rdd = spark_session.sparkContext.emptyRDD()
    df_result = spark_session.createDataFrame(empty_rdd, schema)

    for g in unique_genres:
        # print(f"\nTop 10 {g} movies/series:\n")
        df_genre = df_merged.filter((col("genres") == g)).sort(col('averageRating').desc()).limit(10)
        # df_genre.printSchema()
        df_result = df_result.union(df_genre)

    return df_result
    #

