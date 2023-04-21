import settings
from pyspark.sql import SparkSession


def read_data(file_path):

    from pyspark.sql import SparkSession

    # create a SparkSession
    spark = SparkSession.builder.appName("ReadData").getOrCreate()

    # read the data from file and create a DataFrame
    df = spark.read.csv(file_path, sep='\t', header=True)
    # df = spark.read.format("csv").option("header", "true").load(file_path)

    # stop the SparkSession
    # spark.stop()

    # return the DataFrame
    return df


def write(df, file_path):
    import pandas as pd
    df.toPandas().to_csv(file_path, index=False)
    # df.write.csv(file_path, header=True )
    return
