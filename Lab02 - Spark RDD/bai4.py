from pyspark import SparkContext

# tạo spark context
sc = SparkContext(appName="labSpark_Bai4")

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

# users to rdd
users_rdd = sc.textFile("./data/users.txt") \
    .map(parse_line) \
    .map(lambda x: (x[0], (x[1], x[2], x[3])))

# (UserID, (MovieID, Rating))
user_ratings = ratings_rdd.map(lambda x: (x[0], (x[1], float(x[2])))) \
    .join(users_rdd)

def get_age_group_index(age):
    age = int(age)
    if age < 18: return 0
    elif age < 35: return 1
    elif age < 50: return 2
    else: return 3


def map_age_stats(data):
    # data[1] = ((MovieID, Rating), (Gender, Age, Occ))
    movie_id = data[1][0][0]
    rating = data[1][0][1]
    age = data[1][1][1]
    idx = get_age_group_index(age)
    
    # list [Sum0, Cnt0, Sum1, Cnt1, Sum2, Cnt2, Sum3, Cnt3]
    res = [0.0, 0] * 4
    res[idx * 2] = rating
    res[idx * 2 + 1] = 1
    return (movie_id, res)

age_stats = user_ratings.map(map_age_stats) \
    .reduceByKey(lambda a, b: [x + y for x, y in zip(a, b)]) \
    .join(movies_rdd.map(lambda x: (x[0], x[1][0])))  # lấy Title

all_movies = age_stats.collect()

with open("./result/bai4.txt", "w", encoding="utf-8") as f:
    for mid, (stats, title) in all_movies:
        groups = ["0-18", "18-35", "35-50", "50+"]
        out_str = []
        for i in range(4):
            s, c = stats[i*2], stats[i*2+1]
            avg = f"{s/c:.2f}" if c > 0 else "NA"
            out_str.append(f"{groups[i]}: {avg}")
        line = f"{title} - [{', '.join(out_str)}]"
        print(line)
        f.write(line + "\n")

sc.stop()