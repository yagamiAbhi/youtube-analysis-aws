# Import necessary libraries
import sys
from awsglue.transforms import *  # Import all transformation functions
from awsglue.utils import getResolvedOptions  # Get job arguments
from pyspark.context import SparkContext  # Spark context for distributed processing
from awsglue.context import GlueContext  # Glue context for interacting with Glue services
from awsglue.job import Job  # Job object for defining and running Glue jobs

# DynamicFrame import
from awsglue.dynamicframe import DynamicFrame

# Get job arguments passed from Glue workflow
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Create SparkContext & GlueContext to interact with Glue services
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session # Get SparkSession from GlueContext
job = Job(glueContext)            # Create a Glue Job object
job.init(args['JOB_NAME'], args)  # Initialize the job with name and arguments

# Define predicate to filter data during read 
predicate_pushdown = "region in ('ca','gb','us')"

# Read data from Glue Catalog table with predicate pushdown
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="de-utube-raw",
    table_name="raw_statistics",
    transformation_ctx="datasource0",
    push_down_predicate=predicate_pushdown
)

# Apply schema mapping to ensure column types are consistent
applymapping1 = ApplyMapping.apply(
    frame=datasource0,
    mappings=[
        # Define each column: source name, source type, target name, target type
        ("video_id", "string", "video_id", "string"), 
        ("trending_date", "string", "trending_date", "string"), 
        ("title", "string", "title", "string"), 
        ("channel_title", "string", "channel_title", "string"), 
        ("category_id", "long", "category_id", "long"), 
        ("publish_time", "string", "publish_time", "string"), 
        ("tags", "string", "tags", "string"), 
        ("views", "long", "views", "long"), 
        ("likes", "long", "likes", "long"), 
        ("dislikes", "long", "dislikes", "long"), 
        ("comment_count", "long", "comment_count", "long"), 
        ("thumbnail_link", "string", "thumbnail_link", "string"), 
        ("comments_disabled", "boolean", "comments_disabled", "boolean"), 
        ("ratings_disabled", "boolean", "ratings_disabled", "boolean"), 
        ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"), 
        ("description", "string", "description", "string"), 
        ("region", "string", "region", "string")
    ],
    transformation_ctx="applymapping1"
)

# Convert DynamicFrame to a structured format
resolvechoice2 = ResolveChoice.apply(
    frame=applymapping1,
    choice="make_struct",
    transformation_ctx="resolvechoice2"
)

# Drop rows with null values (optional)
dropnullfields3 = DropNullFields.apply(
    frame=resolvechoice2,
    transformation_ctx="dropnullfields3"
)

# Convert DynamicFrame back to DataFrame for writing
datasink1 = dropnullfields3.toDF().coalesce(1)  # Coalesce partitions for efficient write

# Create a DynamicFrame from the DataFrame
df_final_output = DynamicFrame.fromDF(datasink1, glueContext, "df_final_output")

# Define S3 output path with partition key
datasink4 = glueContext.write_dynamic_frame.from_options(
    frame=df_final_output,
    connection_type="s3",
    connection_options={
        "path": "s3://de-on-utube-cleaned-useast1-dev-abhi/youtube/raw_statistics/",
        "partitionKeys": ["region"]
    },
    format="parquet",
    transformation_ctx="datasink4"
)

# Commit the job to run the defined transformations and write data
job.commit()
