from pyspark.sql import SparkSession

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

def find_common_friend(friend_list):
    """
    find common friends from two friend lists
    :param friend_list1: string waits to be split into friends list
    :param friend_list2: string waits to be split into friends list
    :return: length of common friends list
    """
    friend_ids1 = friend_list[0].split(',')
    friend_ids2 = friend_list[1].split(',')
    intersect = set(friend_ids1).intersection(set(friend_ids2))
    return len(intersect)

def match_user_info(dict, friend_pair):
    """
    match two user's info
    :param dict: dictionary contains key: user_id, value: first name, last name, address
    :param friend_pair: friend pair ids
    :return: user1 and user2 personal info
    """
    friend_ids = friend_pair.split(',')
    return dict[friend_ids[0]] + dict[friend_ids[1]]

if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName("Top10CFRdd")\
        .getOrCreate()

    lines = spark.read.text('./../soc-LiveJournal1Adj.txt').rdd.map(lambda x: x[0])
    q1_spark_output = lines.flatMap(lambda line: match_friend_pair_to_list(line))\
                 .groupByKey()\
                 .mapValues(list)\
                 .map(lambda l: (l[0], find_common_friend(l[1])))\

    sc = spark.sparkContext
    q2_spark_input = q1_spark_output
    # switch number and friend_pair, find top 10
    q2_spark_output = q2_spark_input.map(lambda row: (row[1], row[0]))\
        .takeOrdered(10, key=lambda x: -x[0])

    user_lines = spark.read.text('./../userdata.txt').rdd.map(lambda x: x[0])
    # extract (userid, (firstname, lastname, address)) info
    q2_spark_user = user_lines.map(lambda line: line.split(','))\
        .map(lambda row: (row[0], (row[1], row[2], row[3])))\
        .collectAsMap()

    # format: (number of common friends, (user1 info, user2 info))
    q2_output = sc.parallelize(q2_spark_output)\
        .map(lambda row: (row[0], match_user_info(q2_spark_user, row[1]))) \
        .map(lambda row: str(row[0]) + '\t' + row[1][0] + '\t' + row[1][1] + '\t' + row[1][2] + '\t' +
                         row[1][3] + '\t' + row[1][4] + '\t' + row[1][5])
    q2_output.coalesce(1).saveAsTextFile('top-ten-cf-output-rdd')
    spark.stop()