import dbldatagen as dg
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType, FloatType, StringType, TimestampType, 
    StructField, BooleanType, StructType, ArrayType, DecimalType
)
from pg_connector import PostgresConnector

row_count = 1_00

data_spec = (
    dg.DataGenerator(name="transactions", rows=row_count)
    .withIdOutput()
    .withColumn("user_id", IntegerType(), minValue=1, maxValue=100_000_000)
    .withColumn("transaction_amount", FloatType(), minValue=1.0, maxValue=5000.0, random=True)
    .withColumn("transaction_date", TimestampType(), begin="2022-01-01 00:00:00", end="2022-12-31 23:59:59")
    .withColumn("product_category", StringType(), values=["Electronics", "Books", "Clothing", "Home", "Toys", "Sports", "Automotive"], random=True)
    # Add a Boolean column
    .withColumn("is_returned", BooleanType(), values=[True, False], random=True)
    # Add an Array column
    .withColumn("tags", ArrayType(StringType()), expr="array(product_category, substr(transaction_date, 0, 10))")
    # Add a Struct column
    .withColumn("shipping_address", StructType([
        StructField("street", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True)
    ]), expr="""
        named_struct(
            'street', concat(cast(rand() * 9999 as INT), ' Main St'),
            'city', element_at(array('New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'), cast(rand() * 5 + 1 as INT)),
            'state', element_at(array('NY', 'CA', 'IL', 'TX', 'AZ'), cast(rand() * 5 + 1 as INT)),
            'zip', concat(cast(rand() * 89999 + 10000 as INT))
        )
    """)
    # Add a Decimal column
    .withColumn("tax", DecimalType(5, 2), expr="transaction_amount * 0.08")
    # Add a Date column
    .withColumn("delivery_date", TimestampType(), expr="date_add(transaction_date, cast(rand() * 7 as INT))")
    # Add a Column with Skewed Data
    .withColumn("payment_method", StringType(), values=["Credit Card"] * 80 + ["PayPal"] * 15 + ["Bitcoin"] * 5, random=True)
    # Introduce Null Values
    .withColumn("coupon_code", StringType(), expr="CASE WHEN rand() < 0.2 THEN concat('SAVE', cast(rand() * 100 as INT)) ELSE NULL END")
    # Add a Column with Dependent Values
    .withColumn("loyalty_points", IntegerType(), expr="CASE WHEN user_id % 2 = 0 THEN cast(transaction_amount / 10 as INT) ELSE 0 END")
    # Add a Nested Array of Structs
    .withColumn("items", ArrayType(StructType([
        StructField("item_id", IntegerType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", FloatType(), True)
    ])), expr="""
        array(
            named_struct('item_id', cast(rand() * 1000 as INT), 'quantity', cast(rand() * 5 + 1 as INT), 'price', rand() * 100),
            named_struct('item_id', cast(rand() * 1000 as INT), 'quantity', cast(rand() * 5 + 1 as INT), 'price', rand() * 100)
        )
    """)
    # Add a Geospatial Data Column
    .withColumn("location", StringType(), expr="concat(cast(rand() * 180 - 90 as STRING), ', ', cast(rand() * 360 - 180 as STRING))")
)

# Build the DataFrame
df = data_spec.build()

# Explore the Generated Data
df.show(5)
print(df.count())
# df.to_csv("sample_data.csv", index=False)
# df.to_csv("sample_data.csv", index=False)
pdf = df.toPandas()

PostgresConnector().df_to_postgres_table(df=pdf, table_name="transactions", db_schema="public")
# Save as CSV
pdf.to_csv("sample_data.csv", index=False)
