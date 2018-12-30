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

if __name__ == '__main__':
    spark = SparkSession\
        .builder\
        .appName("CFNumberRdd")\
        .getOrCreate()

    lines = spark.read.text('./../soc-LiveJournal1Adj.txt').rdd.map(lambda x: x[0])
    q1_spark_output = lines.flatMap(lambda line: match_friend_pair_to_list(line))\
                 .groupByKey()\
                 .mapValues(list)\
                 .map(lambda l: (l[0], find_common_friend(l[1])))\
                 .filter(lambda l: l[1] > 0)\
                 .map(lambda l: l[0] + '\t' + str(l[1]))

    q1_spark_output.saveAsTextFile('cf-number-output-rdd')
    spark.stop()
