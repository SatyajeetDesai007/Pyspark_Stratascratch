# Import your libraries
import pyspark
from pyspark.sql import functions as F

# Start writing code
fact_events = fact_events.withColumn("month",F.month(F.col('time_id'))).groupBy('client_id','month').agg(F.countDistinct('user_id').alias('users_num'))
 
# To validate your solution, convert your final pySpark df to a pandas df
fact_events.toPandas()


# 1st we transfer month in integer format. then we group it by using client_id and month buckets, then major unique quantities from every bucket.