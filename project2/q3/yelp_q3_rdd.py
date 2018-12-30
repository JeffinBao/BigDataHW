from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName("YelpQ3Rdd")\
        .getOrCreate()

    # find restaurants which locate in Stanford
    q3_bus_input = spark.read.text('./../business.csv').rdd.map(lambda x: x[0])\
        .map(lambda line: line.split('::'))\
        .filter(lambda x: "Stanford," in x[1])\
        .map(lambda x: x[0])\
        .distinct()\
        .collect()

    # extract user_id and stars in review.csv file where the restaurants are in Stanford
    output = spark.read.text('./../review.csv').rdd.map(lambda x: x[0])\
        .map(lambda line: line.split('::'))\
        .filter(lambda x: x[2] in q3_bus_input)\
        .map(lambda x: x[1] + '\t' + x[3])\

    output.coalesce(1).saveAsTextFile('yelp-q3-output-rdd')
    spark.stop()