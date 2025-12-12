from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, countDistinct, desc, expr, round

spark = SparkSession.builder.appName("bai10").getOrCreate()

data_path = {
   "customer_list": './data/Customer_List.csv',
   "order_item":  './data/Order_Items.csv',
   "order_review":	'./data/Order_Reviews.csv',
	"order": './data/Orders.csv',
	"product": './data/Products.csv'
}

df_order_item = spark.read.csv(data_path['order_item'], header=True, sep=";")

df_rank = df_order_item.groupBy("Seller_ID") \
                  .agg(sum("Price").alias("Revenue"), 
                       countDistinct("Order_ID").alias("Order_Count")) \
                  .orderBy(col("Revenue").desc(), col("Order_Count").desc())


with open('./result/bai10.txt', 'w', encoding='utf-8') as f:
    rows = df_rank.collect()
    for row in rows:
        rev = row['Revenue'] if row['Revenue'] else 0
        count = row['Order_Count']
        
        f.write(f"{row['Seller_ID']}:  {rev} Doanh thu | {count} Số đơn hàng\n")

spark.stop()