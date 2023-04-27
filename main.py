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

    # Load the title.principals.tsv.gz file
    principals_df = read_data(spark_session, settings.PATH_TO_TITLE_PRINCIPALS)

    # Load the name.basics.tsv.gz file
    names_df = read_data(spark_session, settings.PATH_TO_NAME_BASICS)

    # Load the title.ratings.tsv.gz file
    ratings_df = read_data(spark_session, settings.PATH_TO_TITLE_RATINGS)

    # Load the title.episode.tsv.gz file
    episode_df = read_data(spark_session, settings.PATH_TO_TITLE_EPISODE)

    # check schemas OK
    dfs = [{'akas_df': akas_df},
           {'name_basics_df': name_basics_df},
           {'title_basics_df': title_basics_df},
           {'principals_df': principals_df},
           {'names_df': names_df},
           {'ratings_df': ratings_df},
           {'episode_df': episode_df}]
    for df in dfs:
        for key, value in df.items():
            print(f'df: {key}')
            value.printSchema()
            print('--------')

    # task1
    task_1 = tasks.task1(akas_df)
    # # Show results
    task_1.show()

    # task2
    task_2 = tasks.task2(name_basics_df)
    # Show results
    task_2 .show()

    # task 3
    task_3 = tasks.task3(title_basics_df)
    # Show results
    task_3.show()

    # task4
    task_4 = tasks.task4(principals_df, names_df, title_basics_df)
    # Show results
    task_4.show()

    # task5
    task_5 = tasks.task5(akas_df, title_basics_df)
    # Show results
    task_5.show()

    # tasks6
    task_6 = tasks.task6(title_basics_df, episode_df)
    # Show results
    task_6.show()

    # tasks7
    task_7 = tasks.task7(title_basics_df, ratings_df)
    # Show results
    task_7.show()

    # tasks8
    task_8 = tasks.task8(title_basics_df, ratings_df)
    # Show results
    task_8.show()

    # # save results in files
    tasks_list = [{'imdb_out/task_1': task_1},
                  {'imdb_out/task_2': task_2},
                  {'imdb_out/task_3': task_3},
                  {'imdb_out/task_4': task_4},
                  {'imdb_out/task_5': task_5},
                  {'imdb_out/task_6': task_6},
                  {'imdb_out/task_7': task_7},
                  {'imdb_out/task_8': task_8}]

    folder = Path('imdb_out')
    folder.mkdir(parents=True, exist_ok=True)

    for task in tasks_list:
        for key, value in task.items():
            write_file(value, key)


if __name__ == "__main__":
    main()
