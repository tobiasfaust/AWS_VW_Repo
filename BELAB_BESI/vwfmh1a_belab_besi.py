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
# hole die Daten eines exakten BESI Laufes (Werk + Datum) 

def get_besi_run(BESI_RECHNUNG, WERK, TIMESTAMP, VERS):
    #********************************
    # Die Funktion holt einen speziellen BESI Lauf aus der LZ
    #
    # Parameter:
    # BESI_RECHNUNG: Typ des BESI Laufes, [SY | B2 | B3]
    # WERK:          2stellige Werksnummer
    # TIMESTAMP:     BESI Laufdatum als Timestamp
    # VERS:          VERSION, 0 = Daten exakt zum Timestamp; 1 = Daten zum letzten Lauf kleiner Timestamp
    #********************************
    
    if (VERS == 0):
        TS = TIMESTAMP
    else:
        x = spark.read.parquet(bedarfslauf_path)\
            .filter(F.col('BESI_RECHNUNG') == BESI_RECHNUNG)\
            .filter(F.col('WERK') == WERK)\
            .filter(F.col('BESI_LAUFDATUM_TS') < TIMESTAMP)\
            .agg(F.max('BESI_LAUFDATUM_TS').alias("BESI_LAUFDATUM"))\
            .collect()
        TS = x[0]['BESI_LAUFDATUM']
        
    # hole die Bedarfsträger
    obj = spark.read.parquet(bedarfstraeger_path)\
    .filter(F.col('WERK') == WERK)\
    .drop('BESI_LAUFDATUM_TS')

    # hole die Bedarfsmengen
    menge = spark.read.parquet(bedarfsmenge_path)\
    .filter((F.col('WERK') == WERK) & (F.col('BESI_RECHNUNG') == BESI_RECHNUNG) )\
    .filter(F.col('BESI_LAUFDATUM_TS') == TS)\
    .drop('WERK', 'BESI_LAUFDATUM','BESI_RECHNUNG','BESI_LAUFDATUM_TS')
    
    # hole den Bedarfslauf und joine die Bedarfsträger unf Mengen dazu
    lauf = spark.read.parquet(bedarfslauf_path)\
    .filter((F.col('WERK') == WERK) & (F.col('BESI_RECHNUNG') == BESI_RECHNUNG) )\
    .filter(F.col('BESI_LAUFDATUM_TS') == TS)\
    .drop('WERK')
    
    df = lauf.join(obj, 'BEDARFSTRAEGER_ID', 'inner')\
    .dropDuplicates(['BEDARFSTRAEGER_ID'])\
    .join(menge, 'BEDARFSLAUF_ID', 'inner')\
    .drop('BEDARFSMENGE_ID')\
    .withColumn('WERK', F.when(F.length('WERK') > 1, F.col('WERK')).otherwise(F.concat(F.lit('0'),F.col('WERK'))))\
    .withColumn('WEEK', F.concat_ws(
        "-",
        F.expr("EXTRACT(YEAROFWEEK FROM BEDARFSTAG)"),
        F.lpad(F.weekofyear('BEDARFSTAG'), 2, "0")))\
    .withColumn('BELAB_ID', F.concat_ws("|", F.trim(F.col("WERK")), F.trim(F.col("VERWENKZ")),F.trim(F.col("SACHNR_KARTE"))))\
    .groupBy(['BELAB_ID', 'BEDARFSTRAEGER_ID', 'BEDARFSLAUF_ID', 'BESI_RECHNUNG', 'BESI_LAUFDATUM', 'BESI_LAUFDATUM_TS', 'WERK', 'VERWENKZ', 'SACHNR_KARTE', 'ZP8_WERK', 'WERK_KUNDE', 'KUNDE', 'FZGKLASSE', 'LAUFKENNZEICHEN', 'INDEX_FUWOCHE', 'VORLTAGE_INSPEKTION', 'VORLTAGE_BETRIEB', 'VORLTAGE_SICHERHEIT', 'VORLTAGE_RA', 'MAXTAGE', 'MINTAGE', 'MINMENGE', 'SMM', 'SVE', 'SVA', 'ME', 'BE', 'TOL', 'MENGE_KUM_RA', 'MENGE_KUM_EP', 'MENGE_KUM_ZP8', 'MENGE_KUM_WE', 'STARTDATUM', 'MENGENKZ', 'WEEK'])\
    .agg(F.sum('MENGE_RA').alias('MENGE_RA'), F.sum('MENGE_EP').alias('MENGE_EP'), F.sum('MENGE_ZP8').alias('MENGE_ZP8'), F.sum('MENGE_WE').alias('MENGE_WE'))
    
    return df

#get_besi_run('SY', '11', '2022-07-02 02:28:58', 0).show(1,truncate=100)

s = df_belab_todo.collect()
# -> test, nur ein lauf
#s = df_belab_todo.head(1)  # last: tail(1)
 
for x in range(len(s)): 
    # über alle BESI Läufe die jetzt aus dem BB in den Mart prozessiert werden müssen
    # im finalen Stand läuft der Code aller nachfolgenden Jupyter Zellen in dieser Schleife
    
    print(f'Werk: ', s[x]['WERK'], ' -> ', s[x]['BESI_LAUFDATUM_TS'])
    #get_besi_run('SY', '22', F.current_timestamp(), 1)\
    #.show()
df_data = get_besi_run('SY', s[2]['WERK'], s[2]['BESI_LAUFDATUM_TS'], 0)\
.filter(F.col('SACHNR_KARTE') == ' 03L121275')

#.filter(F.col('BEDARFSLAUF_ID') == '11|01|03L121275||11|ZR*K60|ZM54|SY|02072022022858')
# extrahiere alle KUM´s 
df_data_kum = df_data.select(['BELAB_ID', 'BEDARFSTRAEGER_ID','BESI_LAUFDATUM', 'BESI_LAUFDATUM_TS', 'WERK', 'VERWENKZ', 'SACHNR_KARTE', 'FZGKLASSE', 'LAUFKENNZEICHEN', 'INDEX_FUWOCHE', 'VORLTAGE_INSPEKTION', 'VORLTAGE_BETRIEB', 'VORLTAGE_SICHERHEIT', 'VORLTAGE_RA', 'MAXTAGE', 'MINTAGE', 'MINMENGE', 'SMM', 'SVE', 'SVA', 'ME', 'BE', 'TOL', 'MENGE_KUM_RA', 'MENGE_KUM_EP', 'MENGE_KUM_ZP8', 'MENGE_KUM_WE', 'STARTDATUM', 'MENGENKZ'])\
.dropDuplicates()\
.withColumnRenamed('MENGE_KUM_RA', 'MENGE_RA')\
.withColumnRenamed('MENGE_KUM_EP', 'MENGE_EP')\
.withColumnRenamed('MENGE_KUM_ZP8', 'MENGE_ZP8')\
.withColumnRenamed('MENGE_KUM_WE', 'MENGE_WE')\
.withColumn('WEEK', F.lit('1900-01'))
# Extrahiere Daten der Vorserie
df_vs = df_data.filter(df_data["FZGKLASSE"].isin(['VS']))\
.groupBy(['BELAB_ID', 'WEEK'])\
.agg(F.sum('MENGE_ZP8').alias('MENGE_ZP8_VS'))

df_vs_kum = df_data_kum.filter(df_data["FZGKLASSE"].isin(['VS']))\
.select(['BELAB_ID', 'BEDARFSTRAEGER_ID', 'MENGE_ZP8'])\
.dropDuplicates()\
.groupBy(['BELAB_ID'])\
.agg(F.sum('MENGE_ZP8').alias('MENGE_ZP8_VS'))

# Extrahiere Daten von OT
df_ot = df_data.filter(df_data["FZGKLASSE"].isin(['OT']))\
.groupBy(['BELAB_ID', 'WEEK'])\
.agg(F.sum('MENGE_ZP8').alias('MENGE_ZP8_OT'))

df_ot_kum = df_data_kum.filter(df_data["FZGKLASSE"].isin(['OT']))\
.select(['BELAB_ID', 'BEDARFSTRAEGER_ID', 'MENGE_ZP8'])\
.dropDuplicates()\
.groupBy(['BELAB_ID'])\
.agg(F.sum('MENGE_ZP8').alias('MENGE_ZP8_OT'))

# Extrahiere Daten vom CKD
df_ckd = df_data.filter(df_data["FZGKLASSE"].isin(['CKD']))\
.groupBy(['BELAB_ID', 'WEEK'])\
.agg(F.sum('MENGE_ZP8').alias('MENGE_ZP8_CKD'))

df_ckd_kum = df_data_kum.filter(df_data["FZGKLASSE"].isin(['CKD']))\
.select(['BELAB_ID', 'BEDARFSTRAEGER_ID', 'MENGE_ZP8'])\
.dropDuplicates()\
.groupBy(['BELAB_ID'])\
.agg(F.sum('MENGE_ZP8').alias('MENGE_ZP8_CKD'))

# Extrahiere Daten vom Ausschuss
df_aussch = df_data.filter(df_data["FZGKLASSE"].isin(['ZM52','ZM72']))\
.groupBy(['BELAB_ID', 'WEEK'])\
.agg(F.sum('MENGE_ZP8').alias('MENGE_ZP8_AUSSCH'))

df_aussch_kum = df_data_kum.filter(df_data["FZGKLASSE"].isin(['ZM52','ZM72']))\
.select(['BELAB_ID', 'BEDARFSTRAEGER_ID', 'MENGE_ZP8'])\
.dropDuplicates()\
.groupBy(['BELAB_ID'])\
.agg(F.sum('MENGE_ZP8').alias('MENGE_ZP8_AUSSCH'))

# Extrahiere Daten der Ersatzmengen
df_ersatz = df_data.filter(df_data["FZGKLASSE"].isin(['ZM54','ZM74']))\
.groupBy(['BELAB_ID', 'WEEK'])\
.agg(F.sum('MENGE_ZP8').alias('MENGE_ZP8_ERS'))

df_ersatz_kum = df_data_kum.filter(df_data["FZGKLASSE"].isin(['ZM54','ZM74']))\
.select(['BELAB_ID', 'BEDARFSTRAEGER_ID', 'MENGE_ZP8'])\
.dropDuplicates()\
.groupBy(['BELAB_ID'])\
.agg(F.sum('MENGE_ZP8').alias('MENGE_ZP8_ERS'))

# Berechne die Gesamtmengen
# .filter(~df_data["FZGKLASSE"].isin(['ZM52', 'ZM54','ZM72','ZM74','VS','CKD','OT']))\
df_main = df_data.groupBy(['BELAB_ID', 'BESI_LAUFDATUM', 'BESI_LAUFDATUM_TS', 'WERK', 'VERWENKZ', 'SACHNR_KARTE', 'LAUFKENNZEICHEN', 'INDEX_FUWOCHE', 'VORLTAGE_INSPEKTION', 'VORLTAGE_BETRIEB', 'VORLTAGE_SICHERHEIT', 'VORLTAGE_RA', 'MAXTAGE', 'MINTAGE', 'MINMENGE', 'SMM', 'SVE', 'SVA', 'ME', 'BE', 'TOL', 'STARTDATUM', 'MENGENKZ', 'WEEK'])\
.agg(F.sum('MENGE_RA').alias('MENGE_GES_RA'), F.sum('MENGE_EP').alias('MENGE_GES_EP'), F.sum('MENGE_ZP8').alias('MENGE_GES_ZP8'), F.sum('MENGE_WE').alias('MENGE_GES_WE'))

df_kum = df_data_kum.select(['BELAB_ID', 'BEDARFSTRAEGER_ID', 'BESI_LAUFDATUM', 'BESI_LAUFDATUM_TS', 'WERK', 'VERWENKZ', 'SACHNR_KARTE', 'LAUFKENNZEICHEN', 'INDEX_FUWOCHE', 'VORLTAGE_INSPEKTION', 'VORLTAGE_BETRIEB', 'VORLTAGE_SICHERHEIT', 'VORLTAGE_RA', 'MAXTAGE', 'MINTAGE', 'MINMENGE', 'SMM', 'SVE', 'SVA', 'ME', 'BE', 'TOL', 'STARTDATUM', 'WEEK', 'MENGENKZ', 'MENGE_RA', 'MENGE_EP', 'MENGE_ZP8', 'MENGE_WE'])\
.groupBy(['BELAB_ID', 'BESI_LAUFDATUM', 'BESI_LAUFDATUM_TS', 'WERK', 'VERWENKZ', 'SACHNR_KARTE', 'LAUFKENNZEICHEN', 'INDEX_FUWOCHE', 'VORLTAGE_INSPEKTION', 'VORLTAGE_BETRIEB', 'VORLTAGE_SICHERHEIT', 'VORLTAGE_RA', 'MAXTAGE', 'MINTAGE', 'MINMENGE', 'SMM', 'SVE', 'SVA', 'ME', 'BE', 'TOL', 'STARTDATUM', 'MENGENKZ', 'WEEK'])\
.agg(F.sum('MENGE_RA').alias('MENGE_GES_RA'), F.sum('MENGE_EP').alias('MENGE_GES_EP'), F.sum('MENGE_ZP8').alias('MENGE_GES_ZP8'), F.sum('MENGE_WE').alias('MENGE_GES_WE'))


# hole die SY Daten des vorherigen Laufes um die Differenzen auszuweisen
data = get_besi_run('SY', s[2]['WERK'], s[2]['BESI_LAUFDATUM_TS'], 1)

df_last_run = data.filter(F.col('SACHNR_KARTE') == ' 03L121275')\
.groupBy(['BELAB_ID', 'WEEK'])\
.agg(F.sum('MENGE_ZP8').alias('MENGE_ZP8_LASTRUN'))

df_last_run_kum = data.filter(F.col('SACHNR_KARTE') == ' 03L121275')\
.select(['BELAB_ID', 'BEDARFSTRAEGER_ID', 'MENGE_KUM_ZP8'])\
.dropDuplicates()\
.groupBy(['BELAB_ID'])\
.agg(F.sum('MENGE_KUM_ZP8').alias('MENGE_ZP8_LASTRUN'))

#df_last_run_kum.show(truncate=100)

# hole die letzten B3 Daten zum SY Lauf
# ToDo: weitere Filter??
data = get_besi_run('B3', s[2]['WERK'], s[2]['BESI_LAUFDATUM_TS'], 1)

df_B3_data = data.filter(F.col('SACHNR_KARTE') == ' 03L121275')\
.filter(~data["FZGKLASSE"].isin(['ZM52', 'ZM54','ZM72','ZM74','VS','CKD','OT']))\
.groupBy(['BELAB_ID', 'WEEK'])\
.agg(F.sum('MENGE_RA').alias('MENGE_B3'))

df_B3_data_kum = data.filter(F.col('SACHNR_KARTE') == ' 03L121275')\
.filter(~data["FZGKLASSE"].isin(['ZM52', 'ZM54','ZM72','ZM74','VS','CKD','OT']))\
.select(['BELAB_ID', 'BEDARFSTRAEGER_ID', 'MENGE_KUM_RA'])\
.dropDuplicates()\
.groupBy(['BELAB_ID'])\
.agg(F.sum('MENGE_KUM_RA').alias('MENGE_B3'))

df_B3_data_kum.show()
# hole die letzten B2 Daten zum SY Lauf
# ToDo: weitere Filter??
data = get_besi_run('B2', s[2]['WERK'], s[2]['BESI_LAUFDATUM_TS'], 1)

df_B2_data = data.filter(F.col('SACHNR_KARTE') == ' 03L121275')\
.filter(~data["FZGKLASSE"].isin(['ZM52', 'ZM54','ZM72','ZM74','VS','CKD','OT']))\
.groupBy(['BELAB_ID', 'WEEK'])\
.agg(F.sum('MENGE_ZP8').alias('MENGE_B2'))

df_B2_data_kum = data.filter(F.col('SACHNR_KARTE') == ' 03L121275')\
.filter(~data["FZGKLASSE"].isin(['ZM52', 'ZM54','ZM72','ZM74','VS','CKD','OT']))\
.select(['BELAB_ID', 'BEDARFSTRAEGER_ID', 'MENGE_KUM_ZP8'])\
.dropDuplicates()\
.groupBy(['BELAB_ID'])\
.agg(F.sum('MENGE_KUM_ZP8').alias('MENGE_B2'))

df_B2_data_kum.show(truncate=100)
#df_B2_data.show(truncate=100)
# im df_data die laufenden wöchentlichen Kumulative über "window partition" berechnen 

# die Kumulative denormalisieren und pro BELAB_ID als neue Zeile anhängen
res_kum = df_kum.join(df_vs_kum, on=['BELAB_ID'], how='leftouter')\
.join(df_B2_data_kum, on=['BELAB_ID'], how='leftouter')\
.join(df_B3_data_kum, on=['BELAB_ID'], how='leftouter')\
.join(df_ot_kum, on=['BELAB_ID'], how='leftouter')\
.join(df_ckd_kum, on=['BELAB_ID'], how='leftouter')\
.join(df_aussch_kum, on=['BELAB_ID'], how='leftouter')\
.join(df_ersatz_kum, on=['BELAB_ID'], how='leftouter')\
.join(df_last_run_kum, on=['BELAB_ID'], how='leftouter')\

res_kum.show(truncate=100)
# alles verjoinen
res = df_main.join(df_B2_data, on=['BELAB_ID','WEEK'], how='leftouter')\
.join(df_B3_data, on=['BELAB_ID','WEEK'], how='leftouter')\
.join(df_last_run, on=['BELAB_ID','WEEK'], how='leftouter')\
.join(df_vs, on=['BELAB_ID','WEEK'], how='leftouter')\
.join(df_ckd, on=['BELAB_ID','WEEK'], how='leftouter')\
.join(df_aussch, on=['BELAB_ID','WEEK'], how='leftouter')\
.join(df_ersatz, on=['BELAB_ID','WEEK'], how='leftouter')\
.join(df_ot, on=['BELAB_ID','WEEK'], how='leftouter')\
.unionByName(res_kum, allowMissingColumns=True)

res.orderBy('WEEK')\
.show(truncate=10)

#.drop('BESI_LAUFDATUM', 'BESI_LAUFDATUM_TS', 'WERK', 'VERWENKZ', 'SACHNR_KARTE', 'LAUFKENNZEICHEN', 'INDEX_FUWOCHE', 'VORLTAGE_INSPEKTION', 'VORLTAGE_BETRIEB', 'VORLTAGE_SICHERHEIT', 'VORLTAGE_RA', 'MAXTAGE', 'MINTAGE', 'MINMENGE', 'SMM', 'SVE', 'SVA', 'ME', 'BE', 'TOL', 'STARTDATUM', 'MENGENKZ')\

res.printSchema()
#fabka_path = 's3://vwgroup-dlp-data-prod-confidential/lz/besi/tables/Fabrikkalender/'
#werkref_path = 's3://vwgroup-dlp-data-prod-confidential/lz/besi/tables/Werksreferenz/'
#df_fabka_data = spark.read.parquet(fabka_path)
#df_werkref_data = spark.read.parquet(werkref_path)

df_werkref_data = spark.read.text('s3://vwgroup-dlp-source-ssf-confidential/user/vwfmh1a/data/WERKREF/DFX.R11KGZ.WERKREF.D22260.D20220917.T024335')\

df_werkref_data = df_werkref_data.select(
    df_werkref_data.value.substr(1, 2).alias('KARTENART'),
    df_werkref_data.value.substr(4, 30).alias('BESCHREIBUNG'),
    df_werkref_data.value.substr(35, 2).alias('TGN_SIS_Werk'),
    df_werkref_data.value.substr(38, 4).alias('TGN_TMA'),
    df_werkref_data.value.substr(43, 2).alias('TGN_DISPOWERK'),
    df_werkref_data.value.substr(46, 15).alias('PR_NUMMER'),
    df_werkref_data.value.substr(62, 2).alias('TGN_AUFTRAGSART'),
    df_werkref_data.value.substr(65, 2).alias('FSD_WERK'),
    df_werkref_data.value.substr(68, 1).alias('FSD_VERWENDUNG'),
    df_werkref_data.value.substr(71, 3).alias('FABRIKKALENDERWERK'),
    df_werkref_data.value.substr(74, 2).alias('FTI_TGN_SEGMENT'),
    df_werkref_data.value.substr(77, 2).alias('PRISMA_GRUPPIERUNG')
)

df_werkref_data.filter((F.trim('FABRIKKALENDERWERK') == '11') & (F.trim('TGN_SIS_Werk') == '11') & (F.trim('FSD_VERWENDUNG') == '2'))\
.show(30, truncate=1000)

#filter((F.col('WERK') == '08') & (F.col('PERIODE') == '04') & (F.col('DATUM') == '25.04.2022') )\
#df_fabka_data.filter((F.col('etl_month') == '8') & (F.col('etl_day') == '28') & (F.col('WERK') == '11') & F.col('DATUM').like('%21.11.%'))\
#.show()
df_fabka_data = spark.read.text('s3://vwgroup-dlp-source-ssf-confidential/user/vwfmh1a/data/FABKA/DFX.R11KGZ.FABKA.D22260.D20220917.T024335')\

#df_fabka_data.show(100, truncate=1000)

df_fabka_data = df_fabka_data.select(
    df_fabka_data.value.substr(1, 2).alias('WERK'),
    df_fabka_data.value.substr(4, 10).alias('DATUM'),
    df_fabka_data.value.substr(15, 3).alias('TAG_IM_JAHR'),
    df_fabka_data.value.substr(19, 2).alias('WOCHE_IM_JAHR'),
    df_fabka_data.value.substr(22, 2).alias('TAG_IN_WOCHE'),
    df_fabka_data.value.substr(25, 2).alias('PERIODE'),
    df_fabka_data.value.substr(28, 2).alias('AT_IN_PERIODE'),
    df_fabka_data.value.substr(31, 2).alias('WOCHE_IN_PERIODE'),
    df_fabka_data.value.substr(34, 1).alias('AT_IN_WOCHE'),
    df_fabka_data.value.substr(38, 3).alias('TMA'),
    df_fabka_data.value.substr(45, 2).alias('ANZAHL_SCHICHTEN'),
    df_fabka_data.value.substr(48, 2).alias('FRUEHSCHICHT'),
    df_fabka_data.value.substr(51, 2).alias('SPAETSCHICHT'),
    df_fabka_data.value.substr(54, 2).alias('NACHTSCHICHT'),
    df_fabka_data.value.substr(57, 1).alias('ANLIEFERTAG'),
    df_fabka_data.value.substr(61, 2).alias('PERIODE_JAHR')
)

df_fabka_data.filter((F.col('WERK') == '11'))\
.show()

#df_fabka_data.filter((F.col('WERK') == '08') & (F.col('PERIODE') == '04') & (F.col('DATUM') == '25.04.2022') )\

df_fabka_data = df_fabka_data.select(['WERK', 'DATUM', 'PERIODE', 'PERIODE_JAHR', 'ANZAHL_SCHICHTEN'])\
.withColumn('DATUM', F.to_date("DATUM","dd.MM.yyyy"))\
.withColumn('PERIODE_JAHR', F.when(F.length(F.trim('PERIODE_JAHR')) == 0, F.year('DATUM')).otherwise(F.concat(F.lit('20'),'PERIODE_JAHR')))\
.withColumn('DOW', F.dayofweek('DATUM'))\
.withColumn('WEEK', F.concat_ws(
        "-",
        F.expr("EXTRACT(YEAROFWEEK FROM DATUM)"),
        F.lpad(F.weekofyear('DATUM'), 2, "0")))\
.withColumn('WORKDAY', F.when(F.col('ANZAHL_SCHICHTEN').cast("int") > 0, 1).otherwise(0).cast("int"))\
.filter(F.trim('TMA') == '')\

df_fabka_data.filter((F.col('WERK') == '11'))\
.show()
df_workdays_basis = df_fabka_data\
.dropDuplicates(['WERK', 'PERIODE', 'PERIODE_JAHR', 'WEEK', 'DATUM', 'WORKDAY'])\
.groupBy(['WERK', 'PERIODE', 'PERIODE_JAHR', 'WEEK'])\
.agg(F.sum('WORKDAY').alias('WORKDAYS'))

#df_workdays_basis.filter((F.col('WERK') == '11') & (F.col('PERIODE') == '05') & (F.col('WEEK') == '2022-05-09'))\
#.show(2)

df_workdays_basis.filter((F.col('WERK') == '11') & (F.col('PERIODE_JAHR') == '2022'))\
.orderBy("WEEK")\
.show(100)

#.filter((F.col('WERK') == '08') & (F.col('PERIODE') == '04') & (F.col('WEEK') == '2022-04-25'))\
df_workdays_verwenkz = df_fabka_data\
.join(df_werkref_data, F.trim(df_werkref_data.FABRIKKALENDERWERK) == F.col('WERK'))\
.filter(F.trim('FSD_WERK') == F.trim('TGN_SIS_WERK'))\
.withColumn('FSD_VERWENDUNG', F.lpad(F.trim('FSD_VERWENDUNG'), 2, "0"))\
.dropDuplicates(['WERK', 'FSD_VERWENDUNG', 'PERIODE', 'PERIODE_JAHR', 'WEEK', 'DATUM', 'WORKDAY'])\
.groupBy(['WERK', 'FSD_VERWENDUNG', 'PERIODE', 'PERIODE_JAHR', 'WEEK'])\
.agg(F.sum('WORKDAY').alias('WORKDAYS'))


#df_workdays_verwenkz.filter((F.col('WERK') == '11') & (F.col('PERIODE') == '05') & (F.col('FSD_VERWENDUNG') == '02') & (F.col('WEEK') == '2022-05-09'))\
#.show()

#.groupBy(['WERK', 'FSD_VERWENDUNG', 'PERIODE', 'PERIODE_JAHR', 'WEEK'])\
#.agg(F.sum('WORKDAY').alias('WORKDAYS'))\
#.filter((F.col('WERK') == '08') & (F.col('PERIODE') == '04') & (F.col('WEEK') == '2022-04-25'))\
file_inventur = 's3://vwgroup-dlp-source-ssf-confidential/user/vwfmh1a/data/DIM_INVENTURJAHR'
df_inv = spark.read.parquet(file_inventur)
df_inv.orderBy("WERK").show()
# Arbeitstage ab Inventur ohne Berücksichtung eines VerwenKz
df_workdays_basis_inv = df_fabka_data.join(df_inv.alias('inv'), (df_inv.WERK == df_fabka_data.WERK) & (df_inv.VALID_TO == F.lit('9999-12-31')))\
.filter((F.col('Datum') >= F.col('INVDATE')) & (F.col('Datum') <= F.current_date()))\
.dropDuplicates(['WERK', 'PERIODE', 'PERIODE_JAHR', 'WEEK', 'DATUM', 'WORKDAY'])\
.groupBy(['inv.WERK'])\
.agg(F.sum('WORKDAY').alias('WORKDAYS'))\
.withColumn('WEEK', F.lit('1900-01'))

#.filter(F.col("WERK")=='11')\

# Arbeitstage ab Inventur mit Berücksichtung definierter VerwenKz

df_workdays_verwenkz_inv = df_fabka_data\
.join(df_werkref_data, F.trim(df_werkref_data.FABRIKKALENDERWERK) == F.col('WERK'))\
.join(df_inv.alias('inv'), (df_inv.WERK == df_fabka_data.WERK) & (df_inv.VALID_TO == F.lit('9999-12-31')))\
.filter(F.trim('FSD_WERK') == F.trim('TGN_SIS_WERK'))\
.withColumn('FSD_VERWENDUNG', F.lpad(F.trim('FSD_VERWENDUNG'), 2, "0"))\
.filter((F.col('Datum') >= F.col('INVDATE')) & (F.col('Datum') <= F.current_date()))\
.dropDuplicates(['WERK', 'FSD_VERWENDUNG', 'PERIODE', 'PERIODE_JAHR', 'WEEK', 'DATUM', 'WORKDAY'])\
.groupBy(['inv.WERK', 'FSD_VERWENDUNG'])\
.agg(F.sum('WORKDAY').alias('WORKDAYS'))\
.withColumn('WEEK', F.lit('1900-01'))

#.filter(F.col("WERK")=='11')
res\
.join(df_workdays_basis.alias('wdb'), on=['WERK','WEEK'], how='leftouter')\
.join(df_workdays_verwenkz.alias('wdv'), on=['WERK','WEEK'], how='leftouter')\
.join(df_workdays_basis_inv.alias('wdbi'), on=['WERK','WEEK'], how='leftouter')\
.join(df_workdays_verwenkz_inv.alias('wdvi'), on=['WERK','WEEK'], how='leftouter')\
.withColumn('WDAYS', F.when(F.length('wdb.WORKDAYS') > 0, F.col('wdb.WORKDAYS')).otherwise(\
                     F.when(F.length('wdv.WORKDAYS') > 0, F.col('wdv.WORKDAYS')).otherwise(\
                     F.when(F.length('wdbi.WORKDAYS') > 0, F.col('wdbi.WORKDAYS')).otherwise(\
                     F.col('wdvi.WORKDAYS'))\
           )))\
.select(['WERK', 'WEEK', 'WDAYS', 'BELAB_ID'])\
.withColumnRenamed('WDAYS', 'WORKDAYS')\
.orderBy("WERK", "WEEK")\
.show()

#.select(['WERK', 'WEEK', 'WORKDAYS', 'BELAB_ID'])\

TODO: WEEKSTART oder MONTH fehlt zur Berechnung des Monats
#df_werkref_data.printSchema()
#df_werkref_data.show(truncate=100)

#filter(F.col('FABRIKKALENDERWERK') == '08')\
#spark.read.parquet(bedarfsmenge_path).printSchema()
job.commit()