
import task1, task2, task3, task4, task5, task8


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

    # get_schema.print_schemas()

    # task1.task()
    # task2.task()
    # task3.task()
    # task4.task()
    # task5.task()
    task8.task()

if __name__ == "__main__":
    main()
