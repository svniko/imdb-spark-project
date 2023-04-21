import settings
import pandas as pd

import pyspark

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as t

from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Basics").getOrCreate()

def task1(movies_title_akas):


    # фільтруємо дані з UA регіоном
    ukr_movies = movies_title_akas.filter((col("region") == "UA") & (col("title").isNotNull()))

    pandas_df = ukr_movies.select("title").toPandas()

    # зберегти Pandas DataFrame в форматі CSV
    pandas_df.to_csv(r"imdb_out\ukr_movie_titles.csv", index=False)


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

     # task 1
    # завантажуємо title_akas до датафрейму

    movies_title_akas = spark.read.csv(settings.PATH_TO_TITLE_AKAS, sep='\t', header=True)
    task1(movies_title_akas)



    # movies_names = spark_session.read.csv(settings.PATH_TO_NAMES, sep='\t', header=True)
    # movies_names.printSchema()



    # ukr_movie_titles = ukr_movies.select("title")
    #
    # # write DataFrame to CSV
    # ukr_movie_titles.coalesce(1).write.csv(r"imdb_out/ukr_movie_titles.csv", header=True)

    # movies_names.show()
    # data = [('a', 1), ('b', 2)]
    # schema = t.StructType([
    #     t.StructField("name", t.StringType(), True),
    #     t.StructField("value", t.IntegerType(), True)])
    #
    # df = spark_session.createDataFrame(data, schema)
    # df.show()
    spark.stop()

if __name__ == "__main__":
    main()
