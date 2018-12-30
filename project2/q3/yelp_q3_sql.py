from pyspark.sql import SparkSession, Row

if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName("YelpQ3Sql")\
        .getOrCreate()

    # find all restaurants locate in Stanford
    q3_bus_sql_input = spark.read.text('./../business.csv').rdd.map(lambda x: x[0])\
        .map(lambda line: line.split('::'))\
        .filter(lambda x: "Stanford," in x[1])\
        .map(lambda x: Row(bus_id=x[0]))\
        .distinct()

    q3_bus_df = spark.createDataFrame(q3_bus_sql_input)
    q3_bus_df.registerTempTable("bus_table")

    # extract business_id, user_id, star from review.csv
    q3_review_sql_input = spark.read.text('./../review.csv').rdd.map(lambda x: x[0])\
        .map(lambda line: line.split('::'))\
        .map(lambda x: Row(bus_id=x[2], user_id=x[1], star=x[3]))

    q3_review_df = spark.createDataFrame(q3_review_sql_input)
    q3_review_df.registerTempTable("review_table")

    # inner join on business_id to find user_id and star
    output = spark.sql("SELECT r_table.user_id, r_table.star " +
                       "FROM bus_table as b_table INNER JOIN review_table as r_table ON b_table.bus_id = r_table.bus_id")
    output.coalesce(1).write.option('sep', '\t').csv('yelp-q3-output-sql')
    spark.stop()