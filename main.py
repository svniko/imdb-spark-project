from pyspark import SparkConf
from pyspark.sql import SparkSession
from read_write import write_file, read_data

from pathlib import Path
import tasks
import settings

from pyspark.sql.functions import col


def main():
    spark_session = (SparkSession.builder
                                 .master("local")
                                 .appName("IMDb analysis")
                                 .config(conf=SparkConf())
                                 .getOrCreate())


    # Load the title.akas.tsv.gz
    akas_df = read_data(spark_session, settings.PATH_TO_TITLE_AKAS)

    # Load the name.basics.tsv.gz
    name_basics_df = read_data(spark_session, settings.PATH_TO_NAME_BASICS)

    # Load the title.basics.tsv.gz
    title_basics_df = read_data(spark_session, settings.PATH_TO_TITLE_BASICS)

    # Load the title.principals.tsv.gz file
    principals_df = read_data(spark_session, settings.PATH_TO_TITLE_PRINCIPALS)

    # Load the name.basics.tsv.gz file
    names_df = read_data(spark_session, settings.PATH_TO_NAME_BASICS)

    # Load the title.ratings.tsv.gz file
    ratings_df = read_data(spark_session, settings.PATH_TO_TITLE_RATINGS)

    # Load the title.episode.tsv.gz file
    episode_df = read_data(spark_session, settings.PATH_TO_TITLE_EPISODE)


    # # check schemas OK
    # dfs = [{'akas_df': akas_df},
    #        {'name_basics_df': name_basics_df},
    #        {'title_basics_df': title_basics_df},
    #        {'principals_df': principals_df},
    #        {'names_df': names_df},
    #        {'titles_df': titles_df}]
    # for df in dfs:
    #     for key, value in df.items():
    #         print(f'df: {key}')
    #         value.printSchema()
    #         print('--------')

    # # task1 OK
    # films_ua = tasks.task1(akas_df)
    # # Show results
    # films_ua.show()
    # # Save results
    # file_path = Path('imdb_out/task1.csv')
    # write_file(films_ua, file_path)


    # # task2 OK
    # born_in_19c = tasks.task2(name_basics_df)
    # # Show results
    # born_in_19c.show()
    # # Save results
    # file_path = Path('imdb_out/task2.csv')
    # write_file(born_in_19c, file_path)


    # # task 3 OK
    # long_movies = tasks.task3(title_basics_df)
    # # Show results
    # long_movies.show()
    # # Save results
    # file_path = Path('imdb_out/task3.csv')
    # write_file(long_movies, file_path)


    # task4 OK
    # names_movie_char = tasks.task4(principals_df, names_df, title_basics_df)
    # # Show results
    # names_movie_char.show()
    # # Save results
    # file_path = Path('imdb_out/task4.csv')
    # write_file(names_movie_char, file_path)

    # # task5 OK
    # top_100_df = tasks.task5(akas_df, title_basics_df)
    # # Show results
    # top_100_df.show()
    # # Save results
    # file_path = Path('imdb_out/task5.csv')
    # write_file(top_100_df, file_path)

    # tasks7

    episodes = tasks.task7(title_basics_df, episode_df, akas_df)

    # # Show results
    episodes.show()
    # # Save results
    # file_path = Path('imdb_out/task8.csv')
    # write_file(top_100_df, file_path)

    # # tasks8
    #
    # genres = tasks.task8(spark_session, title_basics_df, ratings_df)
    #
    # # # Show results
    # genres.show()
    # # # Save results
    # # file_path = Path('imdb_out/task8.csv')
    # # write_file(top_100_df, file_path)


if __name__ == "__main__":
    main()
