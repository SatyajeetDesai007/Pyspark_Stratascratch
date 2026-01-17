
# Import your libraries
import pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window

#using window functions we separate by shipment_id
shimpent_window =  Window.partitionBy('shipment_id')


# Start writing code
amazon_shipment = amazon_shipment.withColumn('total weight', F.sum('weight').over(shimpent_window))

# To validate your solution, convert your final pySpark df to a pandas df
amazon_shipment.toPandas()