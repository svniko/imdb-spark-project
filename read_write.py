from pyspark.sql import SparkSession


def read_data(file_path):
    spark = SparkSession.builder.appName("ReadData").getOrCreate()
    df = spark.read.csv(file_path, sep='\t', header=True)
    return df


def write_file(df, file_path):
    df.toPandas().to_csv(file_path, index=False)

