# Import your libraries
import pyspark
from pyspark.sql import functions as F

# Start writing code
library_usage = library_usage.filter((F.col('circulation_active_year') == 2016) & ( F.col("provided_email_address") == False) & ( F.col("notice_preference_definition") == 'email'))

library_usage = library_usage.select(F.col('home_library_code'))
# To validate your solution, convert your final pySpark df to a pandas df
library_usage.toPandas()

# here we use filter instead groupBy, groupBy we use when we need to do # # # aggregations, but here we filter out data only 