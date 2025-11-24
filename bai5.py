from pyspark import SparkContext

# tạo Spark
sc = SparkContext(appName="labSpark_Bai5")

def parse_line(line):
    return line.split(",") 

users_rdd = sc.textFile("./data/users.txt") \
    .map(parse_line) \
    .map(lambda x: (x[0], (x[1], x[2], x[3])))

ratings_rdd = sc.textFile("./data/ratings_1.txt") \
    .union(sc.textFile("./data/ratings_2.txt")) \
    .map(parse_line)

occupations_rdd = sc.textFile("./data/occupation.txt") \
    .map(parse_line) \
    .map(lambda x: (x[0], x[1]))

user_occ = users_rdd.map(lambda x: (x[0], x[1][2])) 

# UserID, Rating
rating_val = ratings_rdd.map(lambda x: (x[0], float(x[2])))

# join User-Rating để có (UserID, (Rating, OccID))
# map về (OccID, (Rating, 1))
occ_stats = rating_val.join(user_occ) \
    .map(lambda x: (x[1][1], (x[1][0], 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

final_occ = occ_stats.join(occupations_rdd)

all_occupations = final_occ.collect()

print(f"Total records found: {len(all_occupations)}")


with open("./result/bai5.txt", "w", encoding="utf-8") as f:
    for occ_id, ((total_score, count), occ_name) in all_occupations:
        line = f"{occ_name} - AverageRating: {total_score/count:.2f} (TotalRatings: {int(count)})"
        print(line)
        f.write(line + "\n")

sc.stop()