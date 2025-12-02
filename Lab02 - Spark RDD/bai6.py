import datetime
from pyspark import SparkContext

# tạo Spark
sc = SparkContext(appName="labSpark_Bai6")

def parse_line(line):
    return line.split(",")

#lLoad ratings
ratings_rdd = sc.textFile("./data/ratings_1.txt") \
    .union(sc.textFile("./data/ratings_2.txt")) \
    .map(parse_line)

def extract_year(timestamp):
    return datetime.datetime.fromtimestamp(int(timestamp)).year

# map về (Year, (Rating, 1)) và tính tổng
year_stats = ratings_rdd.map(lambda x: (extract_year(x[3]), (float(x[2]), 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .sortByKey()

all_years = year_stats.collect()

with open("./result/bai6.txt", "w", encoding="utf-8") as f:
    for year, (total_score, count) in all_years:
        line = f"{year} - TotalRatings: {int(count)}, AverageRating: {total_score/count:.2f}"
        f.write(line + "\n")

sc.stop()