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
import boto3
from urllib.parse import urlparse

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
# Komplettlieferung vom Samstag
file = 's3://vwgroup-dlp-data-prod-confidential/lz/besi/tables/BESISY/etl_year=2022/etl_month=6/etl_day=26/'

# Nachlieferung -> klein
#file = 's3://vwgroup-dlp-data-prod-confidential/lz/besi/tables/BESI3/etl_year=2022/etl_month=7/etl_day=4/'
#file = 's3://vwgroup-dlp-data-prod-confidential/lz/besi/tables/BESISY/etl_year=2022/etl_month=7/'

df = spark.read.parquet(file)
df = df.withColumnRenamed("FSD_WERK", "WERK")\
.withColumnRenamed("FSD_VERWKZ", "VERWENKZ")\
.withColumn("STARTDATUM", F.to_date(F.col("STARTDATUM"),"ddMMyyyy"))\
.withColumn("BESI_LAUFDATUM_TS", F.to_timestamp(F.col("BESI_LAUFDATUM"),"ddMMyyyyHHmmss"))\
.withColumn("SACHNR_KARTE", F.rtrim(F.col("SACHNR_KARTE")))\
.withColumn("FZGKLASSE", F.trim(F.col("FZGKLASSE")))\
.withColumn("VORLTAGE_INSPEKTION", F.col("VORLTAGE_INSPEKTION").cast("int") * 0.1)\
.withColumn("VORLTAGE_BETRIEB", F.col("VORLTAGE_BETRIEB").cast("int") * 0.1)\
.withColumn("VORLTAGE_SICHERHEIT", F.col("VORLTAGE_SICHERHEIT").cast("int") * 0.1)\
.withColumn("VORLTAGE_RA", F.col("VORLTAGE_RA").cast("int") * 0.1)\
.withColumn("MINTAGE", F.col("MINTAGE").cast("int"))\
.withColumn("MINMENGE", F.col("MINMENGE").cast("int"))\
.withColumn("MAXTAGE", F.col("MAXTAGE").cast("int"))\
.withColumn("MENGE_KUM_RA", F.col("MENGE_KUM_RA").cast("int"))\
.withColumn("MENGE_KUM_EP", F.col("MENGE_KUM_EP").cast("int"))\
.withColumn("MENGE_KUM_ZP8", F.col("MENGE_KUM_ZP8").cast("int"))\
.withColumn("MENGE_KUM_WE", F.col("MENGE_KUM_WE").cast("int"))\
.withColumn("BEDARFSTRAEGER_ID", F.concat_ws("|", F.trim(F.col("WERK")), F.trim(F.col("VERWENKZ")),F.trim(F.col("SACHNR_KARTE")),F.trim(F.col("ZP8_WERK")),F.trim(F.col("WERK_KUNDE")),F.trim(F.col("KUNDE")),F.trim(F.col("FZGKLASSE"))))\
.withColumn("BEDARFSLAUF_ID", F.concat_ws("|", F.col("BEDARFSTRAEGER_ID"),F.trim(F.col("BESI_RECHNUNG")),F.col("BESI_LAUFDATUM")))\
.repartition("BESI_RECHNUNG","BESI_LAUFDATUM", "WERK")\
.cache()

# nur zum testen damit es schneller geht ;)
#df = df.limit(2)
#df = df.filter(F.col("SACHNR_KARTE").like('%N  90817302%'))\
#.filter(F.col("FSD_WERK") == "22")\
#.filter(F.col("FZGKLASSE").like("%4J1%"))\

#.show(100,truncate=50)

bedarfstraeger_df = df.select(['WERK', 'VERWENKZ', 'SACHNR_KARTE', 'ZP8_WERK', 'WERK_KUNDE', 'KUNDE', 'FZGKLASSE', 'BEDARFSTRAEGER_ID', 'BESI_LAUFDATUM_TS'])\
.dropDuplicates(['BEDARFSTRAEGER_ID'])\
.cache()
bedarfstraeger_path = 's3://vwgroup-dlp-source-ssf-confidential/user/vwfmh1a/data/bb_bedarfe/bedarfstraeger'
try:
    bt_old_df = spark.read.parquet(bedarfstraeger_path).cache()
except:
    schema = StructType([StructField('WERK',StringType(), True),
        StructField('VERWENKZ',StringType(), True),
        StructField('SACHNR_KARTE',StringType(), True),
        StructField('ZP8_WERK',StringType(), True),
        StructField('WERK_KUNDE',StringType(), True),
        StructField('KUNDE',StringType(), True),
        StructField('FZGKLASSE',StringType(), True),
        StructField('BEDARFSTRAEGER_ID',StringType(), True),
        StructField("BESI_LAUFDATUM_TS", TimestampType(), True),                 
    ])

    bt_old_df = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)

# auf temp sichern da sonst gelöschte partitionen gelesen werden müssen

bedarfstraeger_df.unionByName(bt_old_df, allowMissingColumns=True)\
.repartition("WERK")\
.dropDuplicates(['BEDARFSTRAEGER_ID'])\
.write\
.mode('overwrite')\
.parquet(bedarfstraeger_path + "_temp")
spark.read.parquet(bedarfstraeger_path + "_temp")\
.write\
.mode('overwrite')\
.parquet(bedarfstraeger_path)
# clean-up temporary folder
s3 = boto3.resource('s3')

o = urlparse(bedarfstraeger_path + "_temp")
bucket = o.netloc
key = o.path

my_bucket = s3.Bucket(bucket)
response = my_bucket.objects.filter(Prefix=key[1:]).delete()
bedarfslauf_df = df.select(['BEDARFSTRAEGER_ID', 'BEDARFSLAUF_ID', 'BESI_RECHNUNG', 'BESI_LAUFDATUM', 'BESI_LAUFDATUM_TS', 'WERK', 'LAUFKENNZEICHEN', 'INDEX_FUWOCHE', 'VORLTAGE_INSPEKTION', 'VORLTAGE_BETRIEB', 'VORLTAGE_SICHERHEIT', 'VORLTAGE_RA', 'MAXTAGE', 'MINTAGE', 'MINMENGE', 'SMM', 'SVE', 'SVA', 'ME', 'BE', 'TOL', 'MENGE_KUM_RA', 'MENGE_KUM_EP', 'MENGE_KUM_ZP8', 'MENGE_KUM_WE', 'STARTDATUM', 'MENGENKZ'])

bedarfslauf_path = 's3://vwgroup-dlp-source-ssf-confidential/user/vwfmh1a/data/bb_bedarfe/bedarfslauf'

bedarfslauf_df.write\
.partionBy("BESI_RECHNUNG","BESI_LAUFDATUM", "WERK")\
.option("partitionOverwriteMode","dynamic")\
.mode('overwrite')\
.parquet(bedarfslauf_path)
bedarfsmenge_path = 's3://vwgroup-dlp-source-ssf-confidential/user/vwfmh1a/data/bb_bedarfe/bedarfsmenge'
#Dataframe zum joinen für 217 Tage erzeugen
df_len = 217
freq =1
ref = spark.range(
    0, df_len, freq
).toDF("id")
bedarfsmenge_df = df.join(F.broadcast(ref))\
.withColumn("id", F.col("id").cast("int"))\
.withColumn("BEDARFSTAG", F.expr("date_add(STARTDATUM,id)"))\
.withColumn("MENGE_RA",   F.col("MENGEN").substr((F.col('id') * F.lit(28)) + F.lit(1),  F.lit(7) ).cast('int') )\
.withColumn("MENGE_EP",   F.col("MENGEN").substr((F.col('id') * F.lit(28)) + F.lit(8),  F.lit(7) ).cast('int') )\
.withColumn("MENGE_ZP8",  F.col("MENGEN").substr((F.col('id') * F.lit(28)) + F.lit(15), F.lit(7) ).cast('int') )\
.withColumn("MENGE_WE",   F.col("MENGEN").substr((F.col('id') * F.lit(28)) + F.lit(22), F.lit(7) ).cast('int') )\
.drop("MENGEN", "id")\
.withColumn("BEDARFSMENGE_ID", F.concat_ws("|", F.col("BEDARFSLAUF_ID"),F.col("BEDARFSTAG")))\
.drop("VERWENKZ",
     "SACHNR_KARTE",
     "ZP8_WERK",
     "WERK_KUNDE",
     "KUNDE",
     "FZGKLASSE",
     "LAUFKENNZEICHEN",
     "INDEX_FUWOCHE",
     "VORLTAGE_INSPEKTION",
     "VORLTAGE_BETRIEB",
     "VORLTAGE_SICHERHEIT",
     "VORLTAGE_RA",
     "MAXTAGE",
     "MINTAGE",
     "MINMENGE",
     "SMM",
     "SVE",
     "SVA",
     "ME",
     "BE",
     "TOL",
     "MENGE_KUM_RA",
     "MENGE_KUM_EP",
     "MENGE_KUM_ZP8",
     "MENGE_KUM_WE",
     "FILLER",
     "STARTDATUM",
     "MENGENKZ",
     "etl_year",
     "etl_month",
     "etl_day",
     "etl_time",
     "BEDARFSTRAEGER_ID")\
.write\
.partionBy("BESI_RECHNUNG","BESI_LAUFDATUM", "WERK")\
.option("partitionOverwriteMode","dynamic")\
.mode('overwrite')\
.parquet(bedarfsmenge_path)

#from py4j.java_gateway import java_import
#java_import(spark._sc._jvm, "org.apache.spark.sql.api.python.*")

#bedarfsmenge_df.explain(mode="formatted") 
#df.join(ref)\
#.withColumn("id", F.col("id").cast("int"))\
#.write\
#.mode('overwrite')\
#.parquet(bedarfsmenge_path + "_temp")
#bedarfsmenge_df = spark.read.parquet(bedarfsmenge_path + "_temp")\
#.withColumn("BEDARFSTAG", F.expr("date_add(STARTDATUM,id)"))\
#.withColumn("MENGE_RA",   F.col("MENGEN").substr((F.col('id') * F.lit(28)) + F.lit(1),  F.lit(7) ).cast('int') )\
#.withColumn("MENGE_EP",   F.col("MENGEN").substr((F.col('id') * F.lit(28)) + F.lit(8),  F.lit(7) ).cast('int') )\
#.withColumn("MENGE_ZP8",  F.col("MENGEN").substr((F.col('id') * F.lit(28)) + F.lit(15), F.lit(7) ).cast('int') )\
#.withColumn("MENGE_WE",   F.col("MENGEN").substr((F.col('id') * F.lit(28)) + F.lit(22), F.lit(7) ).cast('int') )\
#.drop("MENGEN", "id")\
#.withColumn("BEDARFSMENGE_ID", F.concat_ws("|", F.col("BEDARFSLAUF_ID"),F.col("BEDARFSTAG")))
#bedarfsmenge_df.count()
#bedarfsmenge_df = bedarfsmenge_df.select(['BEDARFSLAUF_ID', 
#                                          'BEDARFSMENGE_ID', 
#                                          'BESI_RECHNUNG', 
#                                          'BESI_LAUFDATUM', 
#                                          'BESI_LAUFDATUM_TS', 
#                                          'WERK', 
#                                          'BEDARFSTAG', 
#                                          'MENGE_RA', 
#                                          'MENGE_EP', 
#                                          'MENGE_ZP8', 
#                                          'MENGE_WE'])
#bedarfsmenge_df.repartition(4)\
#.write\
#.partitionBy("BESI_RECHNUNG","BESI_LAUFDATUM", "WERK")\
#.option("partitionOverwriteMode","dynamic")\
#.mode('overwrite')\
#.parquet(bedarfsmenge_path)
# clean-up temporary folder

#s3 = boto3.resource('s3')

#o = urlparse(bedarfsmenge_path + "_temp")
#bucket = o.netloc
#key = o.path

#my_bucket = s3.Bucket(bucket)
#response = my_bucket.objects.filter(Prefix=key[1:]).delete()
#spark.stop()
job.commit()