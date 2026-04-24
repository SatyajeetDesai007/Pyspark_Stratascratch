# Import your libraries
from pyspark.sql import functions as F

# here we separate email using group by so we can identify duplicate email.
email_counts = employee.groupBy('email').agg(F.count('email').alias('email_count'))

# we filter only required data here 
final_df = email_counts.filter(F.col('email_count') > 1).select('email')


# To validate your solution, convert your final pySpark df to a pandas df
final_df.toPandas()