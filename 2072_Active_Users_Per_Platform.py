# Import your libraries
import pyspark
from pyspark.sql import functions as F

# Start writing code
user_sessions = user_sessions.groupBy('platform').agg(F.countDistinct('user_id').alias('n_users'))

# To validate your solution, convert your final pySpark df to a pandas df
user_sessions.toPandas()

#   here we 1st put all platform in its separate buckets usign group by and then using countDistinct we remove duplicate user_ids .....