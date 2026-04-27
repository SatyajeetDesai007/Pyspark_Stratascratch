# Import your libraries
from pyspark.sql import functions as F

# here we will join both tables.
joined_df = signups.join(
    plans,
    signups['plan_id'] == plans['id'],
    "inner"
    ).select(
        "signup_start_date",
        "billing_cycle"
    )
    
    
# here we assign number to week days.
weekday_df = joined_df.withColumn("number_of_day", F.dayofweek(F.col('signup_start_date'))-1)

# now we use pivot . and finsl output as bussiness requirements
pivot_df  = weekday_df.groupBy('number_of_day').pivot('billing_cycle',["monthly","quarterly","annual"]).agg(F.count("billing_cycle")).fillna(0).orderBy(F.col("number_of_day").asc())


# To validate your solution, convert your final pySpark df to a pandas df
pivot_df.toPandas()