# Import your libraries
import pyspark
from pyspark.sql import functions as F

# Start writing code
worker = worker.filter(F.col('department') == 'Admin').filter(F.month(F.col('joining_date')) >= 4).agg(F.count(F.col('worker_id')))

# To validate your solution, convert your final pySpark df to a pandas df
worker.toPandas()