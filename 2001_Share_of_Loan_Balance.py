# Import your libraries
from pyspark.sql import functions as F
from pyspark.sql.window  import Window 

# We partition by rate_type to get totals per group (Fixed vs Variable)
window_spec = Window.partitionBy('rate_type')

# Calculate the Percentage
# Formula: (Individual Balance / Total Group Balance) * 100
df_with_share = (
    submissions
    .withColumn('total_balance_per_rate_type',F.sum('balance').over(window_spec))
    .withColumn('balance_shares', F.col('balance')/F.col('total_balance_per_rate_type') * 100 )
    )
    
#final output DF
final_df = df_with_share.select("rate_type","loan_id","balance","balance_shares").orderBy("rate_type","loan_id")


# To validate your solution, convert your final pySpark df to a pandas df
final_df.toPandas()