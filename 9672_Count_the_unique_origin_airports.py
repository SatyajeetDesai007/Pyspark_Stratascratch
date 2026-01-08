port pyspark


us_flights = us_flights.select("origin").distinct()
# we use distinct to remove deplicates.

us_flights.toPandas()