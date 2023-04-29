from pyspark.sql.window import Window
from pyspark.sql.functions import col, collect_list, when, explode, split, row_number, desc, sequence, floor, mean, regexp_replace
from pyspark.sql.types import IntegerType, DoubleType


def task1(df):
    """
    Get all titles of series/movies etc. that are available in Ukrainian.
    """
    df_filter = df.filter((col("region") == "UA") & (col("title").isNotNull())).select("title")

    return df_filter


def task2(df):
    """
    Get the list of people’s names, who were born in the 19th century
    """

    # Change datatype from str to int
    df = df.withColumn("birthYear", df["birthYear"].cast(IntegerType()))

    # Filter birthYear to be in 19 century
    born_in_1800 = df.filter((col("birthYear") > 1800) & (col("birthYear") <= 1900)).select("primaryName")

    return born_in_1800


def task3(df):
    """
    Get titles of all movies that last more than 2 hours.
    """
    # Filter runtimeMinutes to be greater than 120 min, having cast to int before
    df_filter = (df.filter((col("runtimeMinutes").cast(IntegerType()) > 120) & (col("titleType") == 'movie'))
                   .select("primaryTitle"))

    return df_filter


def task4(principals_df, names_df, titles_df):
    """
    Get names of people, corresponding movies/series and characters they
    played in those films.
    """

    # Select necessary columns
    principals_selected_df = principals_df.select("tconst", "nconst", "category", "characters")
    names_selected_df = names_df.select("nconst", "primaryName")

    # Filter the data to keep only actors and actresses
    principals_filtered_df = principals_selected_df.filter(col("category").isin("actor", "actress"))

    # Join the principals and names dataframes
    whole_df = principals_filtered_df.join(names_selected_df, "nconst").drop("nconst", "tconst", "category")

    # split characters string and  explode
    whole_df = whole_df.withColumn("characters", regexp_replace("characters", "[\"\\[\\]]", ""))
    whole_df = whole_df.select("primaryName", explode(split(col("characters"), ",")))\
                       .withColumnRenamed("col", "characters")

    return whole_df


def task5(akas_df, basics_df):
    """
    Get information about how many adult movies/series etc. there are per
    region. Get the top 100 of them from the region with the biggest count to
    the region with the smallest one.
    """

    # Join the dataframes
    joined_df = akas_df.join(basics_df, akas_df["titleId"] == basics_df["tconst"])

    # Filter the data to keep only adult movies/series
    adult_df = joined_df.filter(col("isAdult") == 1)

    # Group the data by region and count the number of adult titles per region
    adult_df = adult_df.withColumn("region", when(col("region") == "\\N", None).otherwise(col("region")))

    # Group and sort the data by the count in descending order

    region_count_df = adult_df.filter(col("region").isNotNull()).groupBy("region").count()
    sorted_df = region_count_df.sort("count", ascending=False).limit(100)

    return sorted_df


def task6(title_df, episode_df):
    """
    Get information about how many episodes in each TV Series. Get the top
    50 of them starting from the TV Series with the biggest quantity of
    episodes.
     """
    # select needed columns
    title_df = title_df.select("tconst", "primaryTitle")
    episode_df = episode_df.select("tconst", "parentTconst")

    # merge title_df and episode_df
    df_merged = title_df.join(episode_df, "tconst").drop('tconst')

    # find the names of tvSeries corresponding to episodes
    df_final = (df_merged.withColumnRenamed('primaryTitle', 'primaryTitle_TVSeries')
                         .join(title_df, df_merged["parentTconst"] == title_df["tconst"])
                         .drop("tconst"))

    # find number of episodes
    df_f = df_final.groupBy("primaryTitle").count().sort(desc('count')).limit(50)

    return df_f


def task7(title_df, ratings_df):
    """
    Get 10 titles of the most popular movies/series etc. by each decade
    """
    # select needed columns
    title_df = title_df.select("tconst", "primaryTitle", "startYear", "endYear")

    # get rid of episodes - to have only name of the whole series
    title_df = title_df.filter((col("titleType") != "tvEpisode"))

    # if item is not series set endYear = startYear
    title_df = title_df.withColumn("endYear", when(col("endYear") == "\\N", col("startYear")).otherwise(col("endYear")))

    # create decades (187 for 1870, 200 for 2000...) for startYear and endYear
    title_df = title_df.withColumn("endDec", floor(title_df['endYear'] / 10))\
                       .withColumn("startDec", floor(title_df['startYear'] / 10))

    # create decades between startYear and endYear
    title_df = title_df.withColumn('decade', explode(sequence('startDec', 'endDec')))

    # join titles with ratings
    df_whole = title_df.join(ratings_df, "tconst").drop("tconst", "numVotes")

    # calculate mean averageRating value for group
    df_whole = df_whole.groupBy('decade', 'primaryTitle').agg(mean('averageRating').cast(DoubleType()).alias('averageRating'))

    # define the window specification
    window_spec = Window.partitionBy('decade').orderBy(df_whole['averageRating'].desc())
    #
    # # rank the titles by their total rating within each decade
    final_df = df_whole.withColumn("row", row_number().over(window_spec)) \
                       .filter(col("row") <= 10)\
                       .select('primaryTitle', 'decade', 'averageRating') \
                       .withColumn("decade", (df_whole['decade'] * 10))

    return final_df


def task8(basics_df, ratings_df):
    """
    Get 10 titles of the most popular movies/series etc. by each genre.
    !!Let's assume that most popular are those with max averageRating!!
    """

    # filter missing values for genres
    basics_df = basics_df.withColumn("genres", when(col("genres") == "\\N", None).otherwise(col("genres")))
    basics_df = basics_df.filter(col("genres").isNotNull())

    # unpack genres
    genres_df = (basics_df.select("tconst", "primaryTitle", explode(split(col("genres"), ",")))
                          .withColumnRenamed("col", "genres"))

    # join genres and ratings
    df_merged = genres_df.join(ratings_df, "tconst").drop("tconst", "numVotes")

    # create window
    window_genres = Window.partitionBy("genres").orderBy(col("averageRating").desc())
    df_result = df_merged.withColumn("row", row_number().over(window_genres)) \
                         .filter(col("row") <= 10).drop("row")
    return df_result



