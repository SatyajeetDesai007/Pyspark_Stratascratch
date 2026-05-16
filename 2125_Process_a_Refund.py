# Import your libraries
from pyspark.sql import functions as F

# we filter required accounts as per our requirement.
filtered_noom_signups = noom_signups.filter(F.col("started_at") >= "2019-01-01")

# now we need to join filtered_noom_signups and noom_transactions.so we perform inner join and keep only relatable data
filtered_signups_transactions = filtered_noom_signups.join(
    noom_transactions,
    "signup_id",
    "inner"
    )
    
# here we need to do filtered_signups_transactions join with noom_plans, because key factor in our solution is done by "billing_cycle_in_months" so we need to do join.

full_joined_df = filtered_signups_transactions.join(
    noom_plans,
    "plan_id",
    "inner"
    )
    
    
# we calculate refund days at below for every row in full_joined_df
refund_days_df = full_joined_df.withColumn("refund_days",F.datediff(F.col("refunded_at"), F.col("settled_at")))

    
# so now we perform group_by and aggreation here for our final answer.
final_df = refund_days_df.groupBy("billing_cycle_in_months").agg(
    F.min("refund_days").alias("min_days"),
    F.avg("refund_days").alias("avg_days"),
    F.max("refund_days").alias("max_days")
    ).orderBy("billing_cycle_in_months")

final_df.explain(True)
# To validate your solution, convert your final pySpark df to a pandas df
final_df.toPandas()

