import pyspark.sql.functions as f

# 1. Standard Join

joined_df = db_employee.join(db_dept, db_employee.department_id == db_dept.id)

# 2. Conditional Aggregation (Faster & cleaner than Pivot)
# We find the max salary for each dept on the same row

result_df = joined_df.agg(
    f.max(f.when(f.col("department") == 'marketing', f.col("salary"))).alias("mkt_max"),
    f.max(f.when(f.col("department") == 'engineering', f.col("salary"))).alias("eng_max")
)

# 3. Final Calculation
# Column name must be EXACTLY 'salary_difference'

final_res = result_df.select(
    f.abs(f.col("mkt_max") - f.col("eng_max")).alias("salary_difference")
)

# StrataScratch Trick: Convert to Pandas for the platform grader
return final_res.toPandas()