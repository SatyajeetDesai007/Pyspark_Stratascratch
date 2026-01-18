# Import your libraries
import pyspark
from pyspark.sql import functions as F

# Start writing code
google_adwords_earnings = google_adwords_earnings.groupBy('business_type').agg(F.sum('adwords_earnings').alias('total earnings'))
 
# To validate your solution, convert your final pySpark df to a pandas df
google_adwords_earnings.toPandas()