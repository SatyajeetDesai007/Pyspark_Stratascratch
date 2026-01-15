# Import your libraries
import pyspark
from pyspark.sql import functions as F

# here we collect max values at node levels. and collect that rows.
max_val = aapl_historical_stock_price.select(F.max('open')).collect()[0][0]

aapl_historical_stock_price = aapl_historical_stock_price.filter(F.col('open') == max_val ).select ('date')


aapl_historical_stock_price.toPandas()

# if we do sort using desc and limit -> spark do total shuffle over network(moving single rows over network ) to putting them in order .
# instead of it we use filter . which filter the max open at every node level.