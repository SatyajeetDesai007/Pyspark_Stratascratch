# Import your libraries
import pyspark
from pyspark.sql import functions as F

# we group by cust_id and do sum of total order cost.
orders_detail = orders.groupBy('cust_id') .agg( 
    F.sum('total_order_cost').alias('total_cost'))
    
# then we inner join both dataframes customers and order_detail
customer_order_detail = orders_detail.join(
    customers,
    orders_detail.cust_id == customers.id,
    "inner"
    )
    
# we select only bussiness required information 
customers = customer_order_detail.select("id","first_name","total_cost").orderBy('first_name')

# To validate your solution, convert your final pySpark df to a pandas df
customers.toPandas()

#Performance: groupBy collapses the data before the join, reducing Network I/O (Shuffle).
#Safety: Using F.sum() prevents the MISSING_AGGREGATION error and ensures mathematical accuracy.
#Readability: Chaining with () or \ keeps the Transformation DAG clean and maintainable.