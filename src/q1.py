from pyspark import SparkContext

print("*************** QUESTION 1 BEGIN ***************")

def mapPhase(line):
    line = line.split("	")
    keys = []
    if len(line) == 2 and line[1].strip() != "":
        userId = int(line[0])
        friendsList = line[1].split(",")
        for userFriendId in friendsList:
            userFriendId = int(userFriendId)
            if userId == userFriendId:
                continue
            if int(userFriendId) < int(userId):
                pair = (userFriendId, userId)
            else:
                pair = (userId, userFriendId)
            keys.append((pair, line[1].split(',')))
    return keys

def reducePhase(list1, list2):
    count=0
    for userid in list1:
        if userid in list2:
            count=count+1
    return count

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    friends_list = sc.textFile("../resources/soc-LiveJournal1Adj.txt", 1)
    map_op = friends_list.flatMap(mapPhase)
    reduce_op = map_op.reduceByKey(reducePhase)
    result=reduce_op.filter(lambda x: x[1] > 0)
    result.saveAsTextFile("../result/result1/Question1Output")
    sc.stop()

print("*************** QUESTION 1 END ***************")