# Import your libraries
from pyspark.sql import functions as f

# we take only required things from fact table to reduce load on clusters memory.
event_details = playbook_events.select('user_id','location')

# insted of sort-merge we use broadcast join, because our dim table is very samll. and using broadcast helps to reduce shuffling 
playbook_events = (
    event_details.join(f.broadcast(playbook_users),on = 'user_id', how = 'inner').
    groupBy(f.col('location').alias('country'),'language').
    agg(f.countDistinct("user_id").alias("n_speakers"))
    .orderBy('country')
    )
# To validate your solution, convert your final pySpark df to a pandas df
playbook_events.toPandas()