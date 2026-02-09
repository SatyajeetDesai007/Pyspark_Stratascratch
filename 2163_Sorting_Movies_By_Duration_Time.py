# Import your libraries
import pyspark 
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# Start writing code
movie_catalogue = movie_catalogue.withColumn("time",F.split(F.col("duration")," ").getItem(0).cast(IntegerType())).orderBy(F.col("time").desc()).drop(F.col("time"))


# To validate your solution, convert your final pySpark df to a pandas df
movie_catalogue.toPandas()



# here we need to rearrange table as per duration . given durations are in string so we convert it in integer and then sort all dataframe.