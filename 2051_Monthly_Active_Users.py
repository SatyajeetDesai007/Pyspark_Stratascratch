# Import your libraries
import pyspark
from pyspark.sql import functions as F

# 1st we filter data and take required (january 2021) data only
sf_events = sf_events.filter((F.col('record_date')>='2021-01-01') & (F.col('record_date')<='2021-01-31'))
 
# We count the unique users found within that account's January data.
monthly_count = sf_events.groupBy('account_id').agg(F.countDistinct('user_id')).alias("CountDatewise")

# To validate your solution, convert your final pySpark df to a pandas df
monthly_count.toPandas()