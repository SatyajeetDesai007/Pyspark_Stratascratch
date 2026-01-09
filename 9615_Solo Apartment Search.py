# Import your libraries
import pyspark
from pyspark.sql import functions as F

# Start writing code
airbnb_search_details = airbnb_search_details.filter(
    (F.col('property_type') == 'Apartment' ) & 
    (F.col('accommodates') == 1 )
)


airbnb_search_details.toPandas()

# We used .filter() instead of .groupBy() because the goal was 'Details'.
# Filtering keeps the original rows; Grouping would have squashed them.