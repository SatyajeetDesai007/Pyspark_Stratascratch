# Import your libraries
import pyspark
from pyspark.sql import functions as F
# Bussines Logic
lyft_rides =lyft_rides.groupBy('hour').agg(F.sum('gasoline_cost').alias('total_gas_cost')).orderBy(F.col('total_gas_cost').desc())
 
# Select required data
lyft_rides = lyft_rides.select('hour')

# To validate your solution, convert your final pySpark df to a pandas df
lyft_rides.toPandas()