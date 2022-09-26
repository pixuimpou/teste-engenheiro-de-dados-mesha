from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, greatest, expr

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#Paths
path_raw = '/opt/spark/work-dir/data/raw'
path_process = '/opt/spark/work-dir/data/process'

#leitura do arquivo
df_microdados = spark.read.option('encoding', 'latin1').csv(f'{path_raw}/MICRODADOS_ENEM_2020.csv', sep=';', header=True)

#seleção de colunas
df_microdados_cols = df_microdados\
.select(
    'NU_INSCRICAO',
    'NU_ANO',
    'TP_SEXO',
    'TP_COR_RACA',
    'TP_ESCOLA',
    'TP_ENSINO',
    'CO_MUNICIPIO_ESC',
    'NO_MUNICIPIO_ESC',
    'CO_UF_ESC',
    'SG_UF_ESC',
    'TP_DEPENDENCIA_ADM_ESC',
    'TP_LOCALIZACAO_ESC',
    'TP_SIT_FUNC_ESC',
    'NU_NOTA_CN',
    'NU_NOTA_CH',
    'NU_NOTA_LC',
    'NU_NOTA_MT',
    'NU_NOTA_REDACAO',
    'TP_PRESENCA_CN',
    'TP_PRESENCA_CH',
    'TP_PRESENCA_LC',
    'TP_PRESENCA_MT',
    'TP_STATUS_REDACAO'
)

cols_presenca = ['TP_PRESENCA_CN','TP_PRESENCA_CH','TP_PRESENCA_LC','TP_PRESENCA_MT', 'TP_STATUS_REDACAO']
cols_nota = [
    'NU_NOTA_CN',
    'NU_NOTA_CH',
    'NU_NOTA_LC',
    'NU_NOTA_MT',
    'NU_NOTA_REDACAO'
]

# checagem se o aluno faltou alguma prova
df_microdados_f = df_microdados_cols\
.withColumn('Flag_Falta', greatest(*[col(c) == 0 if c != 'TP_STATUS_REDACAO' else col(c) == 4 for c in cols_presenca]))\
.fillna('0', subset=cols_nota) 

# calculo da média de notas
df_microdados_final = df_microdados_f\
.withColumn('Nota_Media', expr(f"({'+'.join(cols_nota)})/{len(cols_nota)}"))


# gravação dos dados
df_microdados_final.write.parquet(f'{path_process}/microdados', mode='overwrite')
