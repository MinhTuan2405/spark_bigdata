from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, col

spark = SparkSession.builder.appName("bai2").getOrCreate()


data_path = {
   "customer_list": './data/Customer_List.csv',
   "order_item":  './data/Order_Items.csv',
   "order_review":	'./data/Order_Reviews.csv',
	"order": './data/Orders.csv',
	"product": './data/Products.csv'
}
	
order = spark.read.csv(data_path['order'], header=True, sep=";")
total_order_count = order.distinct ().count ()

customer = spark.read.csv(data_path['customer_list'], header=True, sep=";")
total_customer_count = customer.distinct ().count ()

seller = spark.read.csv (data_path['order_item'], header=True, sep=';')
total_seller_count = seller.agg(countDistinct("Seller_ID")).collect()[0][0]



with open ('./result/bai2.txt', 'w', encoding='utf-8') as f:
    f.write (f'Tổng số đơn hàng: {total_order_count}\n')
    f.write (f'Tổng số khách hàng: {total_customer_count}\n')
    f.write (f'Tổng số người bán: {total_seller_count}\n')