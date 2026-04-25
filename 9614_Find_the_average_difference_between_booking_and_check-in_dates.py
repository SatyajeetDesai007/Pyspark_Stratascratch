# Import your libraries
from pyspark.sql import functions as F

# we filter data on the basis of our requirement.
refined_df = airbnb_contacts.filter(F.col("ts_booking_at").isNotNull())

# modify one column schema and calculate different .
day_gap_df = refined_df.withColumn("booking_date",F.to_date(F.col("ts_booking_at"))).withColumn("day_diff",F.datediff(F.col("ds_checkin"),F.col("booking_date")))

# here we select the data which required for business
final_df = day_gap_df.groupBy("id_host").agg(F.avg('day_diff').alias("avg_days_between_booking_and_checkin")).orderBy(F.col("avg_days_between_booking_and_checkin").desc())

# To validate your solution, convert your final pySpark df to a pandas df
final_df.toPandas()