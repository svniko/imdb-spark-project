from pathlib import Path
import settings
from read_write import write_file, read_data
from pyspark.sql.functions import col, explode, desc, split, trim, mean


def task():
    '''
    Get 10 titles of the most popular movies/series etc. by each genre.
    Let's assume that most popular are those with max mean averageRating
    '''

    # Load title.basics.tsv.gz and title.ratings.tsv.gz
    basics_df = read_data(settings.PATH_TO_TITLE_BASICS)
    ratings_df = read_data(settings.PATH_TO_TITLE_RATINGS)

    basics_df = basics_df.filter(col("genres").isNotNull())
    genres_df = (basics_df.select("tconst", "primaryTitle", explode(split(col("genres"), ",")))
                          .withColumnRenamed("col", "genres"))

    df_merged = genres_df.join(ratings_df, "tconst")
    # df_merged.show()
    df_merged = genres_df.join(ratings_df, "tconst")
    df_grouped = (df_merged.groupBy("genres", "primaryTitle")
                           .agg(mean("averageRating").alias("avg_rating"))
                           .orderBy("avg_rating", ascending=False))

    for row in df_grouped.collect():
        genre = row["genres"]
        print(f"\nTop 10 {genre} movies/series:\n")
        df_top = df_merged.filter(col("genres") == genre).orderBy("averageRating", ascending=False).limit(10)
        for row1 in df_top.collect():
            title = row1["primaryTitle"]
            rating = row1["averageRating"]
            print(f"{title} (Rating: {rating})")

    # Join to datasets on tconst
    # joined_df = basics.join(ratings, "tconst", "inner")
    #
    # # Select columns
    # selected_df = joined_df.select("genres", "primaryTitle", "averageRating", "numVotes")
    #
    # # Split list in genres to separate genres
    # # selected_df = selected_df.withColumn("genre", split(trim(basics["genres"]), ","))
    # exploded_df = (selected_df.select(selected_df.primaryTitle, "averageRating", "numVotes",
    #                                   explode(split(trim(selected_df.genres), ","))
    #                                   .alias("genre")))
    #
    # # Delete  "\N" and empty genres
    # exploded_df = exploded_df.filter("genre != '' and genre != '\\N'")
    #
    # # групуємо за жанром та обчислюємо рейтинг кожного фільму в межах жанру
    # grouped_df = (exploded_df.groupBy("genre", "primaryTitle")
    #               .agg({"averageRating": "mean", "numVotes": "sum"})
    #               .withColumnRenamed("avg(averageRating)", "averageRating")
    #               .withColumnRenamed("sum(numVotes)", "numVotes"))
    #
    # # відбираємо топ-10 фільмів в межах кожного жанру
    # top_df = (grouped_df.groupBy("genre").agg({"primaryTitle": "count"})
    #           .withColumnRenamed("count(primaryTitle)", "numTitles")
    #           .orderBy("genre"))
    # top_df = (top_df.join(grouped_df, "genre", "inner").filter(col("numTitles") >= 10)
    #           .orderBy("genre", desc("averageRating"), desc("numVotes"))
    #           .groupBy("genre").agg({"primaryTitle": "collect_list"})
    #           .withColumnRenamed("collect_list(primaryTitle)", "topTitles"))

    # виводимо результат
    # top_df.show(truncate=False)


    # Save results
    # file_path = Path('imdb_out/task8.csv')
    # write_file(top_df, file_path)