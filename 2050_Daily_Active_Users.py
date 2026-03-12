# Import your libraries
import pyspark
from pyspark.sql import functions as F

# we required only january data so we first filter required data.
sf_events = sf_events.filter((F.col('record_date') >= '2021-01-01') & (F.col('record_date') <= '2021-01-31'))

# here we take only distinct user day wise 
daily_active = sf_events.groupBy('record_date','account_id').agg(F.countDistinct('user_id').alias('daily_active_user'))

# we calculate average of daily active users
sf_events = daily_active.groupBy('account_id').agg(F.avg('daily_active_user').alias('avg_daily_active_users'))
# To validate your solution, convert your final pySpark df to a pandas df
sf_events.toPandas()