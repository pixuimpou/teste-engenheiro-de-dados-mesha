from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

database = "enem"
user = os.environ['DESAFIO_MESHA_SQL_USER']
password = os.environ['DESAFIO_MESHA_SQL_PASS']
 
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

path_curated = '/opt/spark/work-dir/data/curated'

df_dim_escola = spark.read.parquet(f'{path_curated}/dim_escola')
df_dim_cor_raca = spark.read.parquet(f'{path_curated}/dim_cor_raca')
df_dim_presenca = spark.read.parquet(f'{path_curated}/dim_presenca')
df_dim_status_redacao = spark.read.parquet(f'{path_curated}/dim_status_redacao')
df_fato_enem = spark.read.parquet(f'{path_curated}/fato_enem').repartition(6)

def save_to_db(df, table):
    df.write.mode("append")\
        .format("jdbc")\
        .option("url", f"jdbc:sqlserver://mssql_db_desafio:1433;databaseName={database};encrypt=true;trustServerCertificate=true;")\
        .option("dbtable", table)\
        .option("user", user)\
        .option("password", password)\
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")\
        .save()

save_to_db(df_dim_escola, 'dim_escola')
save_to_db(df_dim_cor_raca, 'dim_cor_raca')
save_to_db(df_dim_presenca, 'dim_presenca')
save_to_db(df_dim_status_redacao, 'dim_status_redacao')
save_to_db(df_fato_enem, 'fato_enem')
