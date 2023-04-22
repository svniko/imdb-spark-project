from pathlib import Path
import settings
from read_write import write_file, read_data
from pyspark.sql.functions import col, collect_list


def task():
    '''
    Get names of people, corresponding movies/series and characters they
    played in those films.
    '''
    # Load the title.principals.tsv.gz file
    principals_df = read_data(settings.PATH_TO_TITLE_PRINCIPALS)

    # Select necessary columns
    principals_selected_df = principals_df.select("tconst", "nconst", "category", "characters")

    # Filter the data to keep only actors and actresses
    principals_filtered_df = principals_selected_df.filter(col("category").isin("actor", "actress"))

    # Group the data by "nconst" and "tconst" columns and get the list of characters played by each person
    principals_grouped_df = principals_filtered_df.groupBy("nconst", "tconst").agg(collect_list("characters").alias("characters"))

    # Load the name.basics.tsv.gz file
    names_df = read_data(settings.PATH_TO_NAME_BASICS)

    # Select necessary columns
    names_selected_df = names_df.select("nconst", "primaryName")

    # Join the principals and names dataframes
    joined_df = principals_grouped_df.join(names_selected_df, "nconst")

    # Load the title.basics.tsv.gz file
    titles_df = read_data(settings.PATH_TO_TITLE_BASICS)

    # Select necessary columns
    titles_selected_df = titles_df.select("tconst", "primaryTitle")

    # Join the principals/names dataframe with the titles dataframe
    final_df = joined_df.join(titles_selected_df, "tconst")

    # Save results
    file_path = Path('imdb_out/task4.csv')
    write_file(final_df, file_path)
