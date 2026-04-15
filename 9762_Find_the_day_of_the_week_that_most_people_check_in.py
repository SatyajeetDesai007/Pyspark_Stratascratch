# Import your libraries
import pyspark
from pyspark.sql import functions as F

# here we find out particular day as per date.
day_list_df = airbnb_contacts.withColumn(
    "week_days",
    F.date_format(F.col("ds_checkin"), 'UUUU')
)


# final output
final_count_df = (
    day_list_df
    .groupBy("week_days")
    .agg(F.count('*') .alias ("check-in count"))
    .orderBy(F.desc('check-in count'))
)
# To validate your solution, convert your final pySpark df to a pandas df
final_count_df.toPandas()