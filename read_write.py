import settings

def read_names():
    pass


def write(df, directory_to_write):
    df.write.csv(directory_to_write, header=True, )
    return
