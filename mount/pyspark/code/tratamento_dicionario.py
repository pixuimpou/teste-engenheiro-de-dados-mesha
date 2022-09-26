from pyspark import SparkContext
from pyspark.sql import SparkSession
import pandas as pd
import pyspark.pandas as ps
from pyspark.sql.functions import trim, regexp_replace, col
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

#Paths
path_raw = '/opt/spark/work-dir/data/raw'
path_process = '/opt/spark/work-dir/data/process'

# Leitura do Dicionario
pd_df = pd.read_excel(f'{path_raw}/Dicionário_Microdados_Enem_2020.xlsx', header=4)

# Renomear Colunas
columns = {
    pd_df.columns[0]: 'Nome_Var',
    pd_df.columns[1]: 'Desc_Var',
    pd_df.columns[2]: 'Valor',
    pd_df.columns[3]: 'Desc_Valor',
    pd_df.columns[4]: 'Tamanho',
    pd_df.columns[5]: 'Tipo'
}
pd_df.rename(columns=columns, inplace=True)
pd_df['idx'] = pd_df.index

# Tratar celulas mescladas
ps_df = ps.from_pandas(pd_df.astype('str'))
df_aux = ps_df[['Nome_Var','idx']].replace('nan', None).fillna(method='ffill').to_spark(index_col=None)

df_dicionario = ps_df.to_spark(index_col=None)\
.select('Valor', 'Desc_Valor', 'idx')

df_dicionario_ffill = df_dicionario\
.join(df_aux, 'idx')

#tirar espaços em branco e quebras de linhas / selecionar as variaveis utilizadas
final_columns = ['Nome_Var', 'Valor', 'Desc_Valor']
campos_dic = [
    'TP_COR_RACA',
    'TP_ESCOLA',
    'TP_ENSINO',
    'TP_DEPENDENCIA_ADM_ESC',
    'TP_LOCALIZACAO_ESC',
    'TP_SIT_FUNC_ESC',
    'TP_PRESENCA_CN',
    'TP_STATUS_REDACAO'
]
df_dicionario_final = df_dicionario_ffill\
.select(
    [regexp_replace(trim(c), '\n', '').alias(c) for c in final_columns]
)\
.filter(col('Nome_Var').isin(campos_dic))

# Salvar arquivo

df_dicionario_final.write.parquet(f'{path_process}/dicionario', mode='overwrite')
