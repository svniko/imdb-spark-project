from pyspark import SparkConf
from pyspark.sql import SparkSession
from read_write import write_file, read_data
from pathlib import Path
import tasks
import settings


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

    # # check schemas OK
    # dfs = [{'akas_df': akas_df},
    #        {'name_basics_df': name_basics_df}]
    # for df in dfs:
    #     for key, value in df.items():
    #         print(f'df: {key}')
    #         value.printSchema()
    #         print('--------')

    # # task1 OK
    # films_ua = tasks.task1(akas_df)
    # # Save results
    # file_path = Path('imdb_out/task1.csv')
    # write_file(films_ua, file_path)
    # # Show results
    # films_ua.show()

    # # task2 OK
    # born_in_19c = tasks.task2(name_basics_df)
    # # Save results
    # file_path = Path('imdb_out/task2.csv')
    # write_file(born_in_19c, file_path)
    # # Show results
    # born_in_19c.show()

    # # task 3 OK
    # long_movies = tasks.task3(title_basics_df)
    # # Save results
    # file_path = Path('imdb_out/task3.csv')
    # write_file(long_movies, file_path)
    # # Show results
    # long_movies.show()


if __name__ == "__main__":
    main()
