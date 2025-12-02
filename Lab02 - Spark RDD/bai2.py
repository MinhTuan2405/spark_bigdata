from pyspark import SparkContext

# Khởi tạo spark context
sc = SparkContext(appName="labSpark_Bai2")

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

# join Rating với Movie trước để lấy Genre
# (MovieID, (Rating, Genres))
rating_with_genres = ratings_rdd.map(lambda x: (x[1], float(x[2]))).join(movies_rdd.map(lambda x: (x[0], x[1][1])))

def explode_genres(data):
    #  (MovieID, (Rating, "Genre1|Genre2"))
    rating, genres_str = data[1]
    genres = genres_str.split("|")
    return [(g, (rating, 1)) for g in genres]

genre_stats = rating_with_genres.flatMap(explode_genres) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda x: (x[0] / x[1], x[1]))

all_genres = genre_stats.collect()

with open("./result/bai2.txt", "w", encoding="utf-8") as f:
    for genre, stats in all_genres:
        line = f"{genre} - AverageRating: {stats[0]:.2f} (TotalRatings: {int(stats[1])})"
        print(line)
        f.write(line + "\n")

sc.stop()