from pyspark import SparkContext

print("*************** QUESTION 2 BEGIN ***************")

def mapPhase(value):
    keys = []
    userId = int(value[0])
    if value[1][0] != [""]:
        friendslist = [int(x) for x in value[1][0]]
        for userfriendId in friendslist:
            userfriendId = int(userfriendId)
            if userId == userfriendId:
                continue
            if int(userfriendId) < int(userId):
                pair = (userfriendId, userId)
            else:
                pair = (userId, userfriendId)
            keys.append((pair, [friendslist, value[1][1]]))
    return keys

def reducePhase(list1, list2):
    count=0
    for user in list1[0]:
        if user in list2[0]:
            count=count+1
    return [count, list2[1], list1[1]]

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    friends_list = sc.textFile("../resources/soc-LiveJournal1Adj.txt", 1).\
        map(lambda frnd: frnd.split("	")).\
        filter(lambda frnd: len(frnd) == 2).\
        map(lambda frnd: (frnd[0], frnd[1].split(",")))
    userdata_list = sc.textFile("../resources/userdata.txt", 1).\
        map(lambda data: data.split(",")).\
        map(lambda data: (data[0],"FirstName:" + data[1] + ", LastName:" + data[2] + ", Address:" + data[3] ))
    friends_x_userdata = friends_list.join(userdata_list).coalesce(1)
    temp_pair_list = friends_x_userdata.flatMap(mapPhase)
    reduce_op = temp_pair_list.reduceByKey(reducePhase).filter(lambda x: x[1][0] != 0).map(lambda a: (a[1], a[0]))
    result = reduce_op.sortBy(lambda a: a[0][0], ascending=False)
    top_ten_list=result.top(10);
    file_name="../result/result2/q2.txt"
    with open(file_name, 'w') as filehandle:
        for listitem in top_ten_list:
            filehandle.write(str(listitem))
            filehandle.write('\n')
    sc.stop()

print("*************** QUESTION 2 BEGIN ***************")



