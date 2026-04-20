# Import your libraries
from pyspark.sql import functions as F

# we filter our other data and grab only 2019 and 2020 data.
filter_car_launches =  car_launches.filter(F.col('year').isin(2019,2020))

# Pivot Pattern (The Senior standard for YoY reports)
# it creates column : 2019, 2020
pivoted_df = (
    filter_car_launches
    .groupBy('company_name')
    .pivot('year',[2019,2020])
    .agg(F.count("product_name"))  
    .fillna(0)
    )
    

# calculate net difference 
final_df = (
    pivoted_df
    .withColumn("Net difference", F.col("2020") - F.col("2019"))
    .select("company_name","Net difference")
    .orderBy("Net difference")
    )


# To validate your solution, convert your final pySpark df to a pandas df
final_df.toPandas()