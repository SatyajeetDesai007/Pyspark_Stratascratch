# Import your libraries
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# we use window function row_number 
window_spec  = Window.orderBy(F.col("sat_writing").asc())

# assign rank to every row 
df_rank = sat_scores.withColumn("student_rank", F.row_number().over(window_spec))

total_count =  sat_scores.count()
target_rank = (total_count+1)//2


# we filter out and take only required data
final_df = df_rank.filter(F.col("student_rank") == target_rank).select("id")

# To validate your solution, convert your final pySpark df to a pandas df
final_df.toPandas()