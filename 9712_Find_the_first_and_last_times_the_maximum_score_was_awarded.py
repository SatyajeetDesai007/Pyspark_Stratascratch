# Import your libraries
from pyspark.sql import functions as F

# now we wants to check highest score 
high_score = los_angeles_restaurant_health_inspections.agg(
    F.max("score").alias('max_score')
    ).collect()[0]['max_score']

# we filter data as per max score 
filtered_df = los_angeles_restaurant_health_inspections.filter(
    F.col('score') == high_score
    )
    
# final df get first and last date of max score 
final_df = filtered_df.agg(
    F.min("activity_date").alias("first_time"),
    F.max('activity_date').alias("last_time")
    )

# To validate your solution, convert your final pySpark df to a pandas df
los_angeles_restaurant_health_inspections.toPandas()