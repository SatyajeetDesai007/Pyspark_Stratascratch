# Import your libraries
import pyspark
from pyspark.sql import functions as F

# Start writing code
oscar_nominees = oscar_nominees.filter(F.col('nominee') == 'Abigail Breslin').agg(F.countDistinct('movie').alias('n_movies'))

# To validate your solution, convert your final pySpark df to a pandas df
oscar_nominees.toPandas()

# here using filter we only take required data. then count only distinct data as per movie.