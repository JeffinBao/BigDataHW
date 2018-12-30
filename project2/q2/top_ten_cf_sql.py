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
        .appName("Top10CFSql")\
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

    output.registerTempTable("q2_user")
    # find top 10 number of common friends
    q2_sql_input = spark.sql("SELECT * " +
                             "FROM q2_user ORDER BY count DESC LIMIT 10")
    q2_sql_input = q2_sql_input.rdd\
        .map(lambda row: (row[0].split(',')[0], row[0].split(',')[1], row[1]))\
        .map(lambda x: Row(count=x[2], friend1=x[0], friend2=x[1]))

    q2_sql_input = spark.createDataFrame(q2_sql_input)
    q2_sql_input.registerTempTable("q2_top_user")

    user_info = spark.sparkContext.textFile('./../userdata.txt')\
        .map(lambda line: line.split(','))\
        .map(lambda x: Row(id=x[0], first_name=x[1], last_name=x[2], addr=x[3]))

    q2_sql_user = spark.createDataFrame(user_info)
    q2_sql_user.registerTempTable("user_info")

    # first inner join to get friend1's user_info
    q2_inter_output = spark.sql("SELECT q2_top_user.count, user_info.first_name as u1_fn, user_info.last_name as u1_ln, user_info.addr as u1_addr, q2_top_user.friend2 " +
                                "FROM q2_top_user INNER JOIN user_info ON q2_top_user.friend1 = user_info.id")

    q2_inter_output.registerTempTable("internal_output")
    # second inner join to get friend2's user_info
    final_output = spark.sql("SELECT t1.count, t1.u1_fn, t1.u1_ln, t1.u1_addr, user_info.first_name as u2_fn, user_info.last_name as u2_ln, user_info.addr as u2_addr " +
                             "FROM internal_output as t1 INNER JOIN user_info ON t1.friend2 = user_info.id")

    final_output.coalesce(1).write.option("sep", "\t").csv('top-ten-cf-output-sql')
    spark.stop()