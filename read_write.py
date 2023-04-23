from pyspark.sql import SparkSession
import pyspark.sql.types as t


def read_data(spark_session, file_path):
    # spark = SparkSession.builder.appName("ReadData").getOrCreate()
    # df = spark.read.csv(file_path, sep='\t', header=True)
    # return df
    # schema = t.StructType([t.StructField(column, t.DoubleType(), True) for column in names])
    return (spark_session.read.csv(file_path,
                                   header=True,
                                   sep='\t'))


def write_file(df, file_path):
    df.toPandas().to_csv(file_path, index=False)
    # # df.write.options(header='True', delimiter=',') \
    # #         .mode('overwrite')\
    # #         .csv(file_path)
    # df.write.format("csv").mode('overwrite').save(file_path)
