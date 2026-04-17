# Import your libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window


#1st filter and aggregate orders
daily_record = (
    orders.filter(F.col("order_date").between("2019-02-01","2019-05-01"))
    .groupBy("cust_id","order_date")
    .agg(sum("total_order_cost").alias("daily_total_spend"))
    )

#2 join with customers 
enriched_df = daily_record.join(
    F.broadcast(customers),
    daily_record.id == customers.id,
    "inner"
    )
    
#3 Find the Global Maximum using a Window Function
window_spec = Window.orderBy(F.desc("daily_total_spend"))

# 4 We use RANK() because the question says return ALL ties
final_df = (
    enriched_df.withColumn("rank",F.rank().over(window_spec))
    .filter(F.col('rank') == 1)
    .select("first_name", "daily_total_spend", "order_date")
    )


# To validate your solution, convert your final pySpark df to a pandas df
customers.toPandas()