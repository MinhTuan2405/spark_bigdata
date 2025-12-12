from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc

spark = SparkSession.builder.appName("bai3").getOrCreate()


data_path = {
   "customer_list": './data/Customer_List.csv',
   "order_item":  './data/Order_Items.csv',
   "order_review":	'./data/Order_Reviews.csv',
	"order": './data/Orders.csv',
	"product": './data/Products.csv'
}

df_customers = spark.read.csv(data_path['customer_list'], header=True, sep=";")
df_orders = spark.read.csv(data_path['order'], header=True, sep=";")

df_joined = df_orders.join(df_customers, on="Customer_Trx_ID", how="inner")

df_result = df_joined.groupBy("Customer_Country") \
                     .agg(count("Order_ID").alias("Total_Orders")) \
                     .orderBy(desc("Total_Orders"))




with open ('./result/bai3.txt', 'w', encoding='utf-8') as f:
    rows = df_result.collect()
    for row in rows:
        country = row['Customer_Country']
        count = row['Total_Orders']
            
        f.write (f"{country}: {count} orders\n")
    