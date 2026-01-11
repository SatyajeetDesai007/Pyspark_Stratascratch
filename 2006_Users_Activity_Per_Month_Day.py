# Import your libraries
import pyspark
from pyspark.sql import functions as F

# (trim) we create separate column for month. beacause on month we need to aggregate.
facebook_posts = facebook_posts.withColumn("month",F.month(F.col('post_date')))

# group by months 
facebook_posts = facebook_posts.groupby("month").agg(F.count('*')).alias("total number")
 
# To validate your solution, convert your final pySpark df to a pandas df
facebook_posts.toPandas()

# The Goal: We wanted to see post count for months, regardless of which year it.

# The Extraction: We didn't look at the full date. because we need only month. If it says "January 2019" or "January 2025," we just mark them both as "Month 1".

# Bucket (The Shuffle): We set out 12 buckets on the floor (one for each month). We tossed every post into its matching month's bucket. This is where the GroupBy happened.

# Count: Finally, we just counted how many pieces of paper were in each bucket.

# note : We ignored the "trash" data (user IDs, post text, and years) and only moved the Month number across our network. its efficient and fast .