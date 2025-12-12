from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, expr

spark = SparkSession.builder.appName("bai5").getOrCreate()


data_path = {
   "customer_list": './data/Customer_List.csv',
   "order_item":  './data/Order_Items.csv',
   "order_review":	'./data/Order_Reviews.csv',
	"order": './data/Orders.csv',
	"product": './data/Products.csv'
}

df_review = spark.read.csv(data_path['order_review'], header=True, sep=";")
df_review = df_review.withColumn("Score", expr("try_cast(Review_Score as int)"))
df_clean = df_review.filter(
    col("Score").isNotNull() & 
    (col("Score") >= 1) & 
    (col("Score") <= 5)
)

avg_score = df_clean.agg(avg("Score")).collect()[0][0]

df = df_clean.groupBy("Score") \
                   .agg(count("*").alias("Total_Reviews")) \
                   .orderBy("Score")


# Ghi ra file
with open('./result/bai5.txt', 'w', encoding='utf-8') as f:
    f.write (f"Điểm đánh giá trung bình: {avg_score}\n\n")
    rows = df.collect ()
    for row in rows:
        score = row['Score']
        count = row['Total_Reviews']
        
        f.write (f"{score}: {count} đánh giá\n")

