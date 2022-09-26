from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#Paths
path_process = '/opt/spark/work-dir/data/process'
path_curated = '/opt/spark/work-dir/data/curated'

#Ler dados Process
df_microdados = spark.read.parquet(f'{path_process}/microdados')
df_dicionario = spark.read.parquet(f'{path_process}/dicionario')

#funcao para relacionar o dicionario com a dim escola
def add_col_dicio(df_dic, df_dim, config):
    df_dic_1 = df_dic\
    .filter(col('Nome_Var') == config['fk'])\
    .select(
        col('Valor').alias(config['fk']),
        col('Desc_Valor').alias(config['nome_col'])
    )
    df_dim_f = df_dim\
    .join(df_dic_1, config['fk'], 'left')\
    .fillna('')
    
    return df_dim_f

# ---- DIM ESCOLA ----
w = Window.partitionBy().orderBy('TP_ESCOLA')

#criacao da surrogate key
df_dim_escola = df_microdados\
.select(
    'TP_ESCOLA',
    'TP_ENSINO',
    'CO_MUNICIPIO_ESC',
    'NO_MUNICIPIO_ESC',
    'CO_UF_ESC',
    'SG_UF_ESC',
    'TP_DEPENDENCIA_ADM_ESC',
    'TP_LOCALIZACAO_ESC',
    'TP_SIT_FUNC_ESC',
)\
.fillna('')\
.dropDuplicates()\
.withColumn('ID_Escola', row_number().over(w))

# adicionar colunas de descricao do dicionario
config_escola = [
    {"fk":'TP_ESCOLA', "nome_col":"Tipo_Escola"},
    {"fk":'TP_ENSINO', "nome_col":"Tipo_Ensino"},
    {"fk":'TP_DEPENDENCIA_ADM_ESC', "nome_col":"Dep_Adm"},
    {"fk":'TP_LOCALIZACAO_ESC', "nome_col":"Localizacao"},
    {"fk":'TP_SIT_FUNC_ESC', "nome_col":"Situacao_Funcionamento"},
]

for config in config_escola:
    df_dim_escola = add_col_dicio(df_dicionario, df_dim_escola, config)

# dim escola final
df_dim_escola_1 = df_dim_escola\
.select(
    'ID_Escola',
    'Tipo_Escola',
    'Tipo_Ensino',
    col('CO_MUNICIPIO_ESC').alias('Codigo_Municipio'),
    col('NO_MUNICIPIO_ESC').alias('Nome_Municipio'),
    col('CO_UF_ESC').alias('Codigo_UF'),
    col('SG_UF_ESC').alias('Sigla_UF'),
    'Dep_Adm',
    'Localizacao',
    'Situacao_Funcionamento'
)

df_dim_escola_1.write.parquet(f'{path_curated}/dim_escola', mode='overwrite')

# ---- DIM COR RACA ----
df_dim_cor_raca = df_dicionario\
.filter(col('Nome_Var') == 'TP_COR_RACA')\
.select(
    col('Valor').alias('ID_Cor_Raca'),
    col('Desc_Valor').alias('Cor_Raca')
)

df_dim_cor_raca.write.parquet(f'{path_curated}/dim_cor_raca', mode='overwrite')

# ---- DIM PRESENCA ----
df_dim_presenca = df_dicionario\
.filter(col('Nome_Var') == 'TP_PRESENCA_CN')\
.select(
    col('Valor').alias('ID_Presenca'),
    col('Desc_Valor').alias('Descricao_Presenca')
)

df_dim_presenca.write.parquet(f'{path_curated}/dim_presenca', mode='overwrite')

# ---- DIM STATUS REDACAO ----

df_dim_status_redacao = df_dicionario\
.filter(col('Nome_Var') == 'TP_STATUS_REDACAO')\
.select(
    col('Valor').alias('ID_Status_Redacao'),
    col('Desc_Valor').alias('Status_Redacao')
)

df_dim_status_redacao.write.parquet(f'{path_curated}/dim_status_redacao', mode='overwrite')

# ---- TABELA FATO ----

cols_escola = [
    'TP_ESCOLA',
    'TP_ENSINO',
    'CO_MUNICIPIO_ESC',
    'NO_MUNICIPIO_ESC',
    'CO_UF_ESC',
    'SG_UF_ESC',
    'TP_DEPENDENCIA_ADM_ESC',
    'TP_LOCALIZACAO_ESC',
    'TP_SIT_FUNC_ESC',
]

df_microdados_1 = df_microdados.fillna('', subset=cols_escola)

#Pega a surrogate key da dim escola
df_fato_enem = df_microdados_1\
.join(
    df_dim_escola,
    cols_escola
)\
.select(
    col('NU_INSCRICAO').alias('Numero_Inscricao'),
    col('NU_ANO').alias('Ano'),
    col('TP_SEXO').alias('Sexo'),
    col('TP_COR_RACA').alias('ID_Cor_Raca'),
    col('ID_Escola'),
    col('NU_NOTA_CN').alias('Nota_Natureza'),
    col('NU_NOTA_CH').alias('Nota_Humanas'),
    col('NU_NOTA_LC').alias('Nota_Linguagens'),
    col('NU_NOTA_MT').alias('Nota_Matematica'),
    col('NU_NOTA_REDACAO').alias('Nota_Redacao'),
    col('TP_PRESENCA_CN').alias('Presenca_Natureza'),
    col('TP_PRESENCA_CH').alias('Presenca_Humanas'),
    col('TP_PRESENCA_LC').alias('Presenca_Linguagens'),
    col('TP_PRESENCA_MT').alias('Presenca_Matematica'),
    col('TP_STATUS_REDACAO').alias('ID_Status_Redacao'),
    col('Flag_Falta'),
    col('Nota_Media')
)

df_fato_enem.write.parquet(f'{path_curated}/fato_enem', mode='overwrite')
