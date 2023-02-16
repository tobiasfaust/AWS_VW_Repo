import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType,StructField, StringType, TimestampType

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

bedarfstraeger_path = 's3://vwgroup-dlp-source-ssf-confidential/user/vwfmh1a/data/bb_bedarfe/bedarfstraeger'
bedarfslauf_path = 's3://vwgroup-dlp-source-ssf-confidential/user/vwfmh1a/data/bb_bedarfe/bedarfslauf'
bedarfsmenge_path = 's3://vwgroup-dlp-source-ssf-confidential/user/vwfmh1a/data/bb_bedarfe/bedarfsmenge'

out_belab_besi = 's3://vwgroup-dlp-source-ssf-confidential/user/vwfmh1a/data/out_belab_besi'
def collect_belab_summary():
    #*************************
    # Die Funktion ermittelt alle noch nicht im BELAB-Mart prozessierten BESI Läufe aus der LZ
    #*************************
    
    # hole letzten Stand aus dem Mart
    try:
        lastdata = spark.read.parquet(out_belab_besi)\
        .groupBy('WERK')\
        .agg(F.max('BESI_LAUFDATUM_TS').alias("BESI_LAST_LAUFDATUM_TS"))
    except:
        schema = StructType([StructField('WERK',StringType(), True),
            StructField("BESI_LAST_LAUFDATUM_TS", TimestampType(), True),                 
        ])

        lastdata = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
    
    # ermittle alle noch nicht prozessierten BESI SY Läufe aus dem BB, welche noch nicht im MART vorhanden sind
    s = spark.read.parquet(bedarfslauf_path)\
    .filter(F.col('BESI_RECHNUNG') == 'SY')\
    .select('WERK', 'BESI_LAUFDATUM_TS')\
    .dropDuplicates(['WERK', 'BESI_LAUFDATUM_TS'])\
    .withColumn('WERK', F.when(F.length('WERK') > 1, F.col('WERK')).otherwise(F.concat(F.lit('0'),F.col('WERK'))))\
    .join(lastdata, 'WERK' ,'leftouter')\
    .fillna({'BESI_LAST_LAUFDATUM_TS': '1900-01-01 00:00:00'})\
    .filter(F.col('BESI_LAUFDATUM_TS') > F.col('BESI_LAST_LAUFDATUM_TS'))\
    .drop('BESI_LAST_LAUFDATUM_TS')\
    .orderBy('WERK', 'BESI_LAUFDATUM_TS')
    
    return s

df_belab_todo = collect_belab_summary()\
.filter(F.col('WERK') == '11')

df_belab_todo.show(100)
job.commit()