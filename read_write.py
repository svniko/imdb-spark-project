
def read_data(spark_session, file_path):
    """
    Read data

    :param spark_session: Spark Session instance
    :param file_path: path to dataset file
    :return: dataframe
    """

    return (spark_session.read.csv(file_path,
                                   header=True,
                                   sep='\t'))


def write_file(df, file_path):
    """
    Save dataframe to file

    :param df: dataframe to save
    :param file_path: path to folder where to save
    :return: None
    """

    df.write.format("csv").mode('overwrite').save(file_path)
