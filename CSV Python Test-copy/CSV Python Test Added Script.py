import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Addition for Google Translate
import googletrans
from googletrans import Translator

# Additions for reading excel
import awswrangler as wr
import openpyxl

# Addition for DynamicFrame converion
from awsglue.dynamicframe import DynamicFrame

#standard sparc/glue context
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={
        "paths": ["s3://socialhealthai.tempfiles/Data/courses.csv"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=S3bucket_node1,
    mappings=[
        ("id", "string", "id", "string"),
        (
            "`crecer para ser 14 a 17 (2013)`",
            "string",
            "`crecer para ser 14 a 17 (2013)`",
            "int",
        ),
        (
            "`crecer para ser 10 a 13 (2015)`",
            "string",
            "`crecer para ser 10 a 13 (2015)`",
            "int",
        ),
        (
            "`crecer para ser 18 a 24 (2015)`",
            "string",
            "`crecer para ser 18 a 24 (2015)`",
            "int",
        ),
        (
            "girlsmart sexual health course",
            "string",
            "girlsmart sexual health course",
            "int",
        ),
        (
            "girlsmart nutrition exercise course",
            "string",
            "girlsmart nutrition exercise course",
            "int",
        ),
        (
            "`crecer por la paz (14 a 24)`",
            "string",
            "`crecer por la paz (14 a 24)`",
            "int",
        ),
        (
            "girlsmart curso salud sexual español",
            "string",
            "girlsmart curso salud sexual español",
            "int",
        ),
        (
            "curso nutrición y ejercicio girlsmart",
            "string",
            "curso nutrición y ejercicio girlsmart",
            "int",
        ),
        ("`cuída-t (2015)`", "string", "`cuída-t (2015)`", "int"),
        (
            "`crecer por la paz (10 a 13)`",
            "string",
            "`crecer por la paz (10 a 13)`",
            "int",
        ),
        ("`tomá-t el tiempo (2015)`", "string", "`tomá-t el tiempo (2015)`", "int"),
        ("tóma-t el tiempo", "string", "tóma-t el tiempo", "int"),
        (
            "`conóce-t mujeres (14 a 17)`",
            "string",
            "`conóce-t mujeres (14 a 17)`",
            "int",
        ),
        ("`crecer para ser (14 a 17)`", "string", "`crecer para ser (14 a 17)`", "int"),
        ("`crecer para ser (10 a 13)`", "string", "`crecer para ser (10 a 13)`", "int"),
        (
            "`crecer para ser 18 a 24 (2020)`",
            "string",
            "`crecer para ser 18 a 24 (2020)`",
            "int",
        ),
        (
            "capacitación en facilitación virtual",
            "string",
            "capacitación en facilitación virtual",
            "int",
        ),
        (
            "capacitación en consejería virtual",
            "string",
            "capacitación en consejería virtual",
            "int",
        ),
        (
            "`conóce-t hombres (14 a 17)`",
            "string",
            "`conóce-t hombres (14 a 17)`",
            "int",
        ),
        ("`cuida-t (14 a 17)`", "string", "`cuida-t (14 a 17)`", "int"),
        ("`smartclick (14 a 17)`", "string", "`smartclick (14 a 17)`", "int"),
        ("`cuida-t (10 a 13)`", "string", "`cuida-t (10 a 13)`", "int"),
        ("`smartclick (10 a 13)`", "string", "`smartclick (10 a 13)`", "int"),
        (
            "`conóce-t hombres (10 a 13)`",
            "string",
            "`conóce-t hombres (10 a 13)`",
            "int",
        ),
        (
            "`conóce-t mujeres (10 a 13)`",
            "string",
            "`conóce-t mujeres (10 a 13)`",
            "int",
        ),
        ("`crecer para ser (18 a 24)`", "string", "`crecer para ser (18 a 24)`", "int"),
        (
            "`conóce-t hombres (18 a 24)`",
            "string",
            "`conóce-t hombres (18 a 24)`",
            "int",
        ),
        (
            "`conóce-t mujeres (18 a 24)`",
            "string",
            "`conóce-t mujeres (18 a 24)`",
            "int",
        ),
        (
            "construyendo emociones 14 a 17 años",
            "string",
            "construyendo emociones 14 a 17 años",
            "int",
        ),
        (
            "construyendo emociones 10 a 13 años",
            "string",
            "construyendo emociones 10 a 13 años",
            "int",
        ),
        (
            "construyendo emociones 18 a 24 años",
            "string",
            "construyendo emociones 18 a 24 años",
            "int",
        ),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Convert sparc DynamicFrame to Sparc DataFrame to Pandas DataFrame
ApplyMapping_node2_DF = ApplyMapping_node2.toDF().toPandas()

# Read translations Excel and create dictionary
ad_request_path = 's3://socialhealthai.tempfiles/Data/Translation dictionary-of-variables (1).xlsx'
tran_DF = wr.s3.read_excel(ad_request_path, engine='openpyxl',sheet_name='Translation Values Pasted')
#tran_PDF = tran_DF.toPandas()
tran_label = tran_DF[tran_DF["Instrument"] == "Risk Profile"][["label", "alias"]]
tran_label = tran_label.drop_duplicates().reset_index(drop = True)
tran_dict = tran_label.set_index("label")["alias"].to_dict()

# and rename columns using dictionary
ApplyMapping_node2_DF = ApplyMapping_node2_DF.rename(columns = tran_dict)

# Use google translate for translate column names
ApplyMapping_node2_DF_cols = ApplyMapping_node2_DF.columns
translator = Translator()
translations = {}
for colname in ApplyMapping_node2_DF_cols:
  translations[colname] = translator.translate(colname, dest = 'en', src = 'es').text
ApplyMapping_node2_DF = ApplyMapping_node2_DF.rename(columns = translations)

# Drop all with 100% missing (could change to some other level like 95%) The metric 
# indictates the level of course completed before the health profile. 0 is reasonable 
# for null values.
ApplyMapping_node2_DF = ApplyMapping_node2_DF.drop(ApplyMapping_node2_DF.columns[ApplyMapping_node2_DF.apply(lambda col: col.isnull().sum()/ApplyMapping_node2_DF.count()[0] >= 1.0)], axis=1)
ApplyMapping_node2_DF.fillna(0, inplace=True)

# Convert Pandas dataframe back to sparc DynamicFrame
ApplyMapping_node2 = DynamicFrame.fromDF(spark.createDataFrame(ApplyMapping_node2_DF), glueContext, "nested")

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node2,
    connection_type="s3",
    format="csv",
    connection_options={
        "path": "s3://glue-studio-tutorial-socialhealthai/transformed/courses/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
