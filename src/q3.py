from pyspark import SparkContext

print("*************** QUESTION 3 BEGIN ***************")

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    business_list = sc.textFile("../resources/business.csv").distinct(1).map(lambda x: x.split("::"))
    review_list = sc.textFile("../resources/review.csv").distinct(1).map(lambda x: x.split("::"))
    stanford_business = business_list.filter(lambda record: "Stanford" in record[1]).map(
        lambda record: (record[0], ""))
    review_map = review_list.map(lambda record: (record[2], (record[1], record[3])))
    stanford_biz_review = review_map.join(stanford_business, 1).map(lambda x: x[1][0])
    stanford_biz_review.saveAsTextFile("../result/result3/Question3Output")
    sc.stop()

print("*************** QUESTION 3 END ***************")


