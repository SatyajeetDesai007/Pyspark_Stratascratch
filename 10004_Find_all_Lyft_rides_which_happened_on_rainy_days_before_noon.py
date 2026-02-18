# Import your libraries
import pyspark
from pyspark.sql import functions as F

# we need to only filter out data as per bussiness requirements . that why we use filter only
lyft_rides = lyft_rides.filter((F.lower(F.col('weather')) == 'rainy') & (F.col('hour') < 12))

# To validate your solution, convert your final pySpark df to a pandas df
lyft_rides.toPandas()