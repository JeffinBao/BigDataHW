from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName("YelpQ4Rdd")\
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

    # format: (business_id, (full address, categories))
    q4_bus_input = spark.read.text('./../business.csv').rdd.map(lambda x: x[0])\
        .map(lambda line: line.split('::'))\
        .map(lambda x: (x[0], (x[1], x[2])))

    q4_join = q4_review_input.join(q4_bus_input)
    q4_output = q4_join.map(lambda x: x[0] + '\t' + x[1][1][0] + '\t' + x[1][1][1] + '\t' + str(x[1][0]))\
        .distinct()

    q4_output.coalesce(1).saveAsTextFile('yelp-q4-output-rdd')
    spark.stop()