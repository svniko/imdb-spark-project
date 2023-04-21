


def read_data(file_path):

    from pyspark.sql import SparkSession

    # create a SparkSession
    spark = SparkSession.builder.appName("ReadData").getOrCreate()

    # read the data from file and create a DataFrame
    df = spark.read.csv(file_path, sep='\t', header=True)


    # return the DataFrame
    return df


def write(df, file_path):

    df.toPandas().to_csv(file_path, index=False)

