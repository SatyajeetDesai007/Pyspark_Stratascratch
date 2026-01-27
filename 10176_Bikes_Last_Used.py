# Import your libraries
import pyspark
from pyspark.sql import functions as F

# Start writing code
dc_bikeshare_q1_2012 = dc_bikeshare_q1_2012.groupBy("bike_number").agg(F.max("end_time").alias('last_used')).orderBy(F.col("last_used").desc())

# To validate your solution, convert your final pySpark df to a pandas df
dc_bikeshare_q1_2012.toPandas()