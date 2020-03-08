from pyspark import SparkContext

print("*************** QUESTION 4 BEGIN ***************")

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    business_l = sc.textFile("../resources/business.csv").map(lambda x: x.split("::"))
    review_l = sc.textFile("../resources/review.csv").map(lambda x: x.split("::"))
    business_set = business_l.map(lambda x: (x[0], x[1] + "  " + x[2]))

    sum_rating = review_l.map(lambda a: (a[2], float(a[3]))).reduceByKey(lambda a, b: a + b).partitionBy(1)
    count_rating = review_l.map(lambda a: (a[2], 1)).reduceByKey(lambda a, b: a + b).partitionBy(1)

    join_result = sum_rating.join(count_rating)
    rating_avg = join_result.map(lambda a: (a[0], a[1][0] / a[1][1]))
    result_final_1 = business_set.join(rating_avg).distinct(1)
    result_final_2=result_final_1.sortBy(lambda a: a[1][1], ascending=False)
    #result_final_2.saveAsTextFile("../Result/result4/Question4Output")
    result_final =result_final_2.top(10,key=lambda a: a[1][1])
    file_name="../result/result4/q4.txt"
    with open(file_name, 'w') as filehandle:
        for listitem in result_final:
            filehandle.write(str(listitem))
            filehandle.write('\n')

print("*************** QUESTION 4 BEGIN ***************")