import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

DataSource0 = glueContext.create_dynamic_frame.from_catalog(database = "dlg-test", table_name = "csv_files", transformation_ctx = "DataSource0")

Transform0 = ApplyMapping.apply(frame = DataSource0, mappings = [("forecastsitecode", "long", "forecastsitecode", "long"), ("observationtime", "long", "observationtime", "long"), ("observationdate", "string", "observationdate", "date"), ("winddirection", "long", "winddirection", "long"), ("windspeed", "long", "windspeed", "long"), ("windgust", "long", "windgust", "long"), ("visibility", "long", "visibility", "long"), ("screentemperature", "double", "screentemperature", "double"), ("pressure", "long", "pressure", "long"), ("significantweathercode", "long", "significantweathercode", "long"), ("sitename", "string", "sitename", "string"), ("latitude", "double", "latitude", "double"), ("longitude", "double", "longitude", "double"), ("region", "string", "region", "string"), ("country", "string", "country", "string")], transformation_ctx = "Transform0")

DataSink0 = glueContext.write_dynamic_frame.from_options(frame = Transform0, connection_type = "s3", format = "parquet", connection_options = {"path": "s3://dlg-weather-test/parquet-files/", "partitionKeys": []}, transformation_ctx = "DataSink0")
job.commit()