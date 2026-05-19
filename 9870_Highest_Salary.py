# Import your libraries
from pyspark.sql import functions as F

# here we find out highest salary using max aggregation function.
max_salary = worker.agg(F.max("salary").alias("max_salary")).collect()[0]['max_salary']

# now Filter employees with max salary
final_df = worker.filter(F.col("salary") == max_salary).select("first_name","salary")

# To validate your solution, convert your final pySpark df to a pandas df
final_df.toPandas()