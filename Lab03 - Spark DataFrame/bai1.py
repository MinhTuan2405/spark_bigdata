from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("bai1").getOrCreate()

schema_str = ''

data_path = [
	'./data/Customer_List.csv',
	'./data/Order_Items.csv',
	'./data/Order_Reviews.csv',
	'./data/Orders.csv',
	'./data/Products.csv'
]

for path in data_path:
    df = spark.read.csv(path, header=True, inferSchema=True, sep=";")

    first_row = df.first()

    schema_str += f"{path.replace('./data/', '')}\n"

    data_type = df.dtypes
    for field in df.schema:
        schema_str += f"  - {field.name}: {field.dataType.simpleString()}\n"

    schema_str += "\n\n"


with open ('./result/bai1.txt', 'w', encoding='utf-8') as f:
    f.write (schema_str)

spark.stop()