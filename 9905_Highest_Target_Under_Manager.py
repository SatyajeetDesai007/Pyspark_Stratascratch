# Import your libraries
import pyspark
from pyspark.sql import functions as F
from pyspark.sql.window import Window 
 
# program start from here 
salesforce_employees = salesforce_employees.filter(F.col('manager_id') == 13 )

window_value =  Window.orderBy(F.col('target').desc())

salesforce_employees = salesforce_employees.withColumn('rank', F.dense_rank().over(window_value)).filter(F.col('rank') == 1).select('first_name', 'target')

# To validate your solution, convert your final pySpark df to a pandas df
salesforce_employees.toPandas()

# here we 1st filter out unnecessary data. then we use window function dense_rank() instead of max() beacuse max is give us only one value . but we required all values are indicated as maximun in target.