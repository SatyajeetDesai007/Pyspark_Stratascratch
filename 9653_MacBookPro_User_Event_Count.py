from pyspark.sql import functions as F

# 1. THE CLEANUP (Standardize and Filter)
playbook_events = playbook_events.filter(
    F.lower(F.col("device")) == 'macbook pro'
)
# lower() kills case-sensitivity bugs.

# 2. THE BUCKETING (Group and Count)
playbook_events = playbook_events.groupBy("event_name").agg(
    F.count("*").alias("event_count")
).sort("event_name")
# groupBy: Creates a bucket for every unique action (login, home_page, etc.)
# agg: The "math mode" trigger. Tells Spark to start calculating.
# count("*"): Counts every single row sitting inside each bucket.


# 3. THE HANDOVER (Local Python)
playbook_events.toPandas()
#toPandas: Moves data from the massive cluster to a small local table.


# filter un-necessary data keep only "macbook pro" -> macbook users hav eenrolled in multiple events so we create event wise bucket using groupBy -> then count rows in every bucket -> after that we sort it with the name of event . -> then with the help of topanda() Moves data from the massive cluster to a small local table.