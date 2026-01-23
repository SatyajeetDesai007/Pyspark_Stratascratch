# Import your libraries
import pyspark
from pyspark.sql import functions as F


# here we filter the dimensins as per bussiness neeed 
customers = customers.filter(F.col("first_name").isin("Jill","Eva"))

# now we inner join facts and dimensions
customers_orders = orders.join("customers", customers.id == orders.cust_id , "inner" ).select(
    F.col("order_date"),F.col("order_details"),F.col("total_order_cost").orderBy(F.col("cust_id").asc())
    
    )



# To validate your solution, convert your final pySpark df to a pandas df
customers_orders.toPandas()