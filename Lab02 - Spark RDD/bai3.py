from pyspark import SparkContext

# tạo spark context
sc = SparkContext(appName="labSpark_Bai3")

def parse_line(line):
    return line.split(",")

# movies to rdd
movies_rdd = sc.textFile("./data/movies.txt") \
    .map(parse_line) \
    .map(lambda x: (x[0], (x[1], x[2])))

# rating to rdd
ratings_rdd = sc.textFile("./data/ratings_1.txt") \
    .union(sc.textFile("./data/ratings_2.txt")) \
    .map(parse_line)

# user to rdd
users_rdd = sc.textFile("./data/users.txt") \
    .map(parse_line) \
    .map(lambda x: (x[0], (x[1], x[2], x[3])))

# join Ratings với Users theo UserID
# Ratings: (UserID, (MovieID, Rating))
r_mapped = ratings_rdd.map(lambda x: (x[0], (x[1], float(x[2]))))
user_ratings = r_mapped.join(users_rdd)

# 2. Map về (MovieID, (Rating, Gender))
def map_gender_stats(data):
    # data[1] = ((MovieID, Rating), (Gender, ...))
    movie_id = data[1][0][0]
    rating = data[1][0][1]
    gender = data[1][1][0]
    
    if gender == 'M':
        return (movie_id, (rating, 1, 0.0, 0)) # (SumM, CntM, SumF, CntF)
    else:
        return (movie_id, (0.0, 0, rating, 1))

gender_stats = user_ratings.map(map_gender_stats) \
    .reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1], a[2]+b[2], a[3]+b[3]))

final_gender_rdd = gender_stats.join(movies_rdd.map(lambda x: (x[0], x[1][0])))

results = final_gender_rdd.collect()

with open("./result/bai3.txt", "w", encoding="utf-8") as f:
    for mid, (stats, title) in results:
        avg_m = stats[0]/stats[1] if stats[1] > 0 else "NA"
        avg_f = stats[2]/stats[3] if stats[3] > 0 else "NA"

        fmt_m = f"{avg_m:.2f}" if isinstance(avg_m, float) else avg_m
        fmt_f = f"{avg_f:.2f}" if isinstance(avg_f, float) else avg_f
        line = f"{title} - Male_Avg: {fmt_m}, Female_Avg: {fmt_f}"
        print(line)
        f.write(line + "\n")

sc.stop()