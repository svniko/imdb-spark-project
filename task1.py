def task():
    from pathlib import Path
    import settings
    from read_write import write, read_data
    from pyspark.sql.functions import col

    df = read_data(settings.PATH_TO_TITLE_AKAS)
    # df.filter().withColumn
    ukr_movies = df.filter((col("region") == "UA") & (col("title").isNotNull()))

    file_path = Path('imdb_out/task1.csv')
    write(ukr_movies, file_path)
