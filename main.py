import settings
import pandas as pd
import task1, task2, task3
import get_schema

import pyspark

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t

from pyspark.sql.functions import col

# spark = SparkSession.builder.appName("Basics").getOrCreate()


# def task1(movies_title_akas):
#
#
#     # фільтруємо дані з UA регіоном
#     ukr_movies = movies_title_akas.filter((col("region") == "UA") & (col("title").isNotNull()))
#
#     pandas_df = ukr_movies.select("title").toPandas()
#
#     # зберегти Pandas DataFrame в форматі CSV
#     pandas_df.to_csv(r"imdb_out\ukr_movie_titles.csv", index=False)


def main():
    # spark_session = (SparkSession.builder
    #                              .master("local")
    #                              .appName("task")
    #                              .config(conf=SparkConf())
    #                              .getOrCreate())

    # # test
    # d = [('a', 1), ('b', 2)]
    # s = t.StructType([
    #     t.StructField("name", t.StringType(), True),
    #     t.StructField("value", t.IntegerType(), True)])
    # df = spark.createDataFrame(d, s)
    # df.show()

    get_schema.print_schemas()
    # task1.task()
    # task2.task()
    # task3.task()



if __name__ == "__main__":
    main()
