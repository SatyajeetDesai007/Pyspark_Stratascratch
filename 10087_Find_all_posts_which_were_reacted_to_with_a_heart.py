# Import your libraries
import pyspark
from pyspark.sql import functions as F

# Start writing code
# we 1st filter out data on reactions(heart),there have multiple hearts for single post. so we only select distinct post_id which help us during join two tables.
facebook_reactions = facebook_reactions.filter(F.col('reaction') == 'heart').select('post_id').distinct()

# then we join("inner")  both tables based on post_id and select all required attriibutes for output
facebook_posts_reactions = facebook_posts.join(facebook_reactions,facebook_reactions.post_id == facebook_reactions.post_id,"inner").select(facebook_posts["*"])

# To validate your solution, convert your final pySpark df to a pandas df
facebook_posts_reactions.toPandas()