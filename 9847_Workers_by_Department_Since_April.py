# Import your libraries
import pyspark
from pyspark.sql import functions as F

# 1st we filter data on date .. then group it as per department and then count as per id.
worker =  worker.filter(F.col('joining_date')>= '2014-04-01').groupBy('department').agg(F.count('worker_id').alias('total_workers')).orderBy(F.col('total_workers').desc())

# To validate your solution, convert your final pySpark df to a pandas df
worker.toPandas()