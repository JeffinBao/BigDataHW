from pyspark.sql import SparkSession, Row

def match_friend_pair_to_list(line):
    """
    format friend pair and yield (friend pair, friend list)
    :param line: string friend list, format: (user, friend list)
    :return: ((user1, user2), friend list)
    """
    line_div = line.split('\t')
    # make sure the results after split are not empty string
    if line_div[0] and line_div[1]:
        user = line_div[0]
        friend_list = line_div[1]
        friend_ids = friend_list.split(',')
        user_int = int(user)
        for friend in friend_ids:
            friend_int = int(friend)
            if user_int < friend_int:
                yield (user + ',' + friend, friend_list)
            elif user_int > friend_int:
                yield (friend + ',' + user, friend_list)

def find_common_friend_sql(friend_list1, friend_list2):
    """
    find common friends from two friend lists
    :param friend_list1: string waits to be split into friends list
    :param friend_list2: string waits to be split into friends list
    :return: length of common friends list
    """
    friend_ids1 = friend_list1.split(',')
    friend_ids2 = friend_list2.split(',')
    intersect = set(friend_ids1).intersection(set(friend_ids2))
    return len(intersect)

if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName("CFNumberSql")\
        .getOrCreate()

    # register udf(user defined function)
    spark.udf.register("find_intersection_udf",
                       lambda x, y: find_common_friend_sql(x, y))

    q1_df = spark.sparkContext.textFile('./../soc-LiveJournal1Adj.txt')\
        .flatMap(lambda line: match_friend_pair_to_list(line))\
        .map(lambda x: Row(friend_pair=x[0], friend_list=x[1]))

    # create df and then create temp table
    q1_df = spark.createDataFrame(q1_df)
    q1_df.registerTempTable("q1_table")

    # inner join to find intersection of friends
    output = spark.sql("SELECT table1.friend_pair as f_pair, find_intersection_udf(table1.friend_list, table2.friend_list) as count " +
                       "FROM q1_table as table1 INNER JOIN q1_table as table2 ON table1.friend_pair = table2.friend_pair " +
                       "WHERE table1.friend_list != table2.friend_list").distinct()
    # filter when count is 0
    output = output.rdd.filter(lambda l: int(l[1]) > 0)
    output = spark.createDataFrame(output)
    # coalesce(1) means combine all partitions into one file
    output.coalesce(1).write.option("sep", "\t").csv('cf-number-output-sql')

    spark.stop()