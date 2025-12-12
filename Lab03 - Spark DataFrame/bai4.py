from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, year, month, asc

spark = SparkSession.builder.appName("bai4").getOrCreate()


data_path = {
   "customer_list": './data/Customer_List.csv',
   "order_item":  './data/Order_Items.csv',
   "order_review":	'./data/Order_Reviews.csv',
	"order": './data/Orders.csv',
	"product": './data/Products.csv'
}

df_orders = spark.read.csv(data_path['order'], header=True, sep=";")


df = df_orders.withColumn("Year", year("Order_Purchase_Timestamp")) \
                       .withColumn("Month", month("Order_Purchase_Timestamp")) \
                       .groupBy("Year", "Month") \
                       .agg(count("Order_ID").alias("Total_Orders")) \
                       .orderBy(asc("Year"), desc("Month"))

with open ('./result/bai4.txt', 'w', encoding='utf-8') as f:
    rows = df.collect()
    for row in rows:
        y = row['Year']
        m = row['Month']
        total = row['Total_Orders']
            
        f.write (f"{y}/{m}:  {total} orders\n")
    