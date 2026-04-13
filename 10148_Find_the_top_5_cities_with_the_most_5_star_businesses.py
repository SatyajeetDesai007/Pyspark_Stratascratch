# Import your libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# we filter out only required data and make partition of it.and count it city wise.
stars_filter_df = (
    yelp_business.filter(F.col("stars") == 5)
    .groupBy('city')
    .agg(F.count("business_id").alias("n_counts"))
)

# using window we arrange above count as descending form
window_rank_df = Window.orderBy(F.desc("n_counts"))

# as per position we provide rank them 
yelp_business_df = (
    stars_filter_df
    .withColumn('rank', F.rank().over(window_rank_df))
    .filter(F.col('rank') <= 5)
    .select('city', 'n_counts')
    .orderBy(F.desc('n_counts'))
)

# To validate your solution
yelp_business_df.toPandas()
