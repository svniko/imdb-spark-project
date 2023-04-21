def task():
    from pathlib import Path
    import settings
    from read_write import write, read_data
    from pyspark.sql.functions import col
    from pyspark.sql.types import IntegerType

    df = read_data(settings.PATH_TO_NAME_BASICS)
    df = df.withColumn("birthYear", df["birthYear"].cast(IntegerType()))

    born_in_1800 = df.filter((col("birthYear") > 1800) & (col("birthYear") <= 1900))

    file_path = Path('imdb_out/task2.csv')
    write(born_in_1800, file_path)
