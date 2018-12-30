from pyspark.sql import SparkSession, Row

if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName("YelpQ4Sql")\
        .getOrCreate()

    # output of each step:
    # 1. (business_id, float(stars))
    # 2. (business_id, (stars, 1))
    # 3. (business_id, (total stars, total count))
    # 4. (business_id, average stars)
    # 5. only top 10 average stars' restaurant remains
    q4_review_input = spark.read.text('./../review.csv').rdd.map(lambda x: x[0])\
        .map(lambda line: line.split('::'))\
        .map(lambda x: (x[2], float(x[3])))\
        .mapValues(lambda x: (x, 1))\
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))\
        .mapValues(lambda x: x[0] / x[1])\
        .takeOrdered(10, key=lambda x: -x[1])

    q4_review_input = spark.sparkContext.parallelize(q4_review_input)

    q4_bus_input = spark.read.text('./../business.csv').rdd.map(lambda x: x[0])\
        .map(lambda line: line.split('::'))\
        .map(lambda x: (x[0], (x[1], x[2])))

    # q4_sql
    q4_review_input = q4_review_input.map(lambda x: Row(bus_id=x[0], ave_star=x[1]))
    q4_bus_input = q4_bus_input.map(lambda x: Row(bus_id=x[0], addr=x[1][0], cate=x[1][1]))
    q4_review_df = spark.createDataFrame(q4_review_input)
    q4_bus_df = spark.createDataFrame(q4_bus_input)
    q4_review_df.registerTempTable("q4_review_table")
    q4_bus_df.registerTempTable("q4_bus_table")

    # todo write into summary: must use left join, since q4_review_table only has 10 rows, if use inner join, will cause Java OutofMemory Error, don't know why?
    q4_sql_output = spark.sql("SELECT distinct b_table.bus_id, b_table.addr, b_table.cate, r_table.ave_star " +
                              "FROM q4_review_table as r_table LEFT JOIN q4_bus_table as b_table ON b_table.bus_id = r_table.bus_id")
    q4_sql_output.coalesce(1).write.option('sep', '\t').csv('yelp-q4-output-sql')
    spark.stop()