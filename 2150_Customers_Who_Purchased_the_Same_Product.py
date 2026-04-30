# Import your libraries
from pyspark.sql import functions as F

# we filter our data as per our requirements 
filtered_online_products = online_products.filter(F.col('product_class') == 'FURNITURE').select ('product_id','brand_name')

# now we perform join between furniture products with orders
joined_df = filtered_online_products.join(
        online_orders,
        filtered_online_products['product_id'] == online_orders['product_id'],
        "inner"
    ).select(
        filtered_online_products['product_id'],
        "brand_name",
        "customer_id"
)

# group by product and brand .
final_df = joined_df.groupBy(
    "product_id",
    "brand_name"
    ).agg(
        F.collect_set("customer_id").alias("unique_customer_ids"),
        F.countDistinct("customer_id").alias("customer_count")
     ).orderBy(F.col("customer_count").desc())


# To validate your solution, convert your final pySpark df to a pandas df
final_df.toPandas()