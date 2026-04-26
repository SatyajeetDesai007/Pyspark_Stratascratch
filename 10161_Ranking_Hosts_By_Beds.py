# Import your libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window 

# here count bed host_wise 
beds_per_count = airbnb_apartments.groupBy("host_id").agg(F.sum('n_beds').alias('bed_counts'))

# Create window for rank 
window_spec = Window.orderBy(F.col('bed_counts').desc())

# final result as per bussiness requirements.
final_df = beds_per_count.select(
    "host_id",
    "bed_counts",
    F.dense_rank().over(window_spec).alias('rank')
    )

# To validate your solution, convert your final pySpark df to a pandas df
final_df.toPandas()