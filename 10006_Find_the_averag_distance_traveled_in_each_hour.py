# Import your libraries
import pyspark
from pyspark.sql import functions as F

# Start writing code
lyft_rides = lyft_rides.groupBy('hour').agg(F.avg('travel_distance').alias('average')).orderBy(F.col('hour').asc())


lyft_rides = lyft_rides.select('hour','average')
# To validate your solution, convert your final pySpark df to a pandas df
lyft_rides.toPandas()