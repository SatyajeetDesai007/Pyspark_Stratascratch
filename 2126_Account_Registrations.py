# Import your libraries
import pyspark
from pyspark.sql import functions as F

# add new column for (YYYY-MM) .
noom_signups = noom_signups.withColumn('month_year', F.date_format(F.col('started_at'),'yyyy-MM'))

noom_signups = noom_signups.groupBy('month_year').agg(F.count('signup_id').alias('registration_count'))
# To validate your solution, convert your final pySpark df to a pandas df
noom_signups.toPandas()

# We needed to count how many new accounts were registered each month, turning a list of specific dates into a clean monthly summary