# Import your libraries
import pyspark
from pyspark.sql import functions as F

# we filter online_orders as per requirement 
online_orders = online_orders.filter((F.month('date_sold') == 4) & (F.year('date_sold') == 2022))

# we filter online_customer as per requirement 
online_customers = online_customers.filter(F.col('state') == 'Oregon')

# join both DF
online_customer_orders = online_orders.join(
    online_customers,
    online_orders.customer_id  == online_customers.id,
    "inner"
    )

#aggreagte as per our final requirement (revenue)
# revenue  = unit * cost
online_customer_orders = online_customer_orders.withColumn("revenue", F.col('cost_in_dollars')*F.col('units_sold')).agg(F.sum('revenue').alias('total revenue'))


# To validat*F.col(''e your solution, convert your final pySpark df to a pandas df
online_customer_orders.toPandas()

# 1st we filter both tables as per our problems need .then only we do shuffle('join') . that help us to reduce cost of program execution,usage less memory and executed fast .....