# Import your libraries
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

#Initialize the Spark Session
spark = SparkSession.builder.appName("EmployeeAnalysis").getOrCreate()
# Start writing code
counts = facebook_employees.select(F.count (F.when(F.col('is_senior') == 'TRUE',1 )).alias('senior_count'),F.count(F.when(F.col('location') == 'USA',1)).alias('USA_count')).collect()[0]

# separate that count as per category
senior_count = counts['senior_count']
usa_count = counts['USA_count']

# now comapre count 
if senior_count > usa_count:
    result = "More seniors"
else :
    result = "More USA-based"

# create new DF for result
final_op = spark.createDataFrame([(result,)], ["winner"])


# To validate your solution, convert your final pySpark df to a pandas df
final_op.toPandas()