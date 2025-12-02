from pyspark import SparkContext

# tạo spark context
sc = SparkContext(appName="labSpark_Bai1")

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

# Map ra (MovieID, (Rating, 1)) để tính tổng và số lượng
# x[1] là MovieID, x[2] là Rating 
movie_ratings = ratings_rdd.map(lambda x: (x[1], (float(x[2]), 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

# (MovieID, ((TotalScore, Count), (Title, Genres)))
joined_rdd = movie_ratings.join(movies_rdd)

# trung bình (Title, AverageRating, TotalRatings)
avg_ratings = joined_rdd.map(lambda x: (x[1][1][0], x[1][0][0] / x[1][0][1], x[1][0][1]))

all_movies = avg_ratings.collect()

with open("./result/bai1.txt", "w", encoding="utf-8") as f:
    for title, avg, count in all_movies:
        line = f"{title} AverageRating: {avg:.2f} (TotalRatings: {int(count)})"
        print(line)
        f.write(line + "\n")
    
    # Tìm phim có điểm trung bình cao nhất
    best_movie = avg_ratings.filter(lambda x: x[2] >= 5) \
        .max(key=lambda x: x[1]) 
    
    result_line = f"\n{best_movie[0]} is the highest rated movie with an average rating of {best_movie[1]:.2f} among movies with at least 5 ratings."
    print(result_line)
    f.write(result_line + "\n")


sc.stop()