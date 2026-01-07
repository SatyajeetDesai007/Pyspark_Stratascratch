
import pyspark
from pyspark.sql import functions as s 

# Start writing code
wfm_transactions = wfm_transactions.join(
    wfm_products, wfm_transactions.product_id == wfm_products.product_id
).groupBy("product_category").agg(
    s.count_distinct("customer_id").alias("customers")
).sort(s.col("customers").desc())
# count_distinct: The "Loyalty" check. If one person bought 10 items,
# count_distinct only counts them as ONE person.


wfm_transactions.toPandas()