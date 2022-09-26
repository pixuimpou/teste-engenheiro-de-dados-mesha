from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
import docker
import pyodbc
import os

# ---------- Declaração da DAG ----------

default_args = {
    'owner': 'Rafael',
    'depends_on_past': False,
    'start_date': datetime(2022, 9, 20, 1, 0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1), 
}

dag = DAG(
    'dag_desafio_mesha',
    description='Dag do Desafio da Mesha',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)


# Função de execução
def exec_spark(code_name):
    print(f'------------------------------------ Executing {code_name} ------------------------------------')
    client = docker.from_env()
    container = client.containers.get('pyspark_desafio')
    cmd = container.exec_run(f"/opt/spark/bin/spark-submit /opt/spark/work-dir/code/{code_name}.py",stream=False)
    out = cmd.output.splitlines()
    for a in out:
        print(a)
    exit_code = cmd.exit_code
    print(f'{code_name} exited with code {exit_code}')
    if int(exit_code) != 0:
        raise Exception(f'Error running code: {exit_code}')

def truncar_tabelas():
	database = "enem"
	username = os.environ['DESAFIO_MESHA_SQL_USER']
	password = os.environ['DESAFIO_MESHA_SQL_PASS']
	
	cnxn = pyodbc.connect(
	'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+'mssql_db_desafio'+';DATABASE='+database+';UID='+username+';PWD='+ password, 		autocommit=True, TrustServerCertificate="YES"
	)
	
	cursor = cnxn.cursor()
	cursor.execute("TRUNCATE TABLE dbo.dim_escola")
	cursor.execute("TRUNCATE TABLE dbo.dim_cor_raca")
	cursor.execute("TRUNCATE TABLE dbo.dim_presenca")
	cursor.execute("TRUNCATE TABLE dbo.dim_status_redacao")
	cursor.execute("TRUNCATE TABLE dbo.fato_enem")
	cnxn.close()

 # ---------- Declaração das tasks ----------]

task_tratamento_dicionario = PythonOperator(
    task_id='task_tratamento_dicionario',
    python_callable=exec_spark,
    op_args=['tratamento_dicionario'],
    dag=dag
)

task_tratamento_microdados = PythonOperator(
    task_id='task_tratamento_microdados',
    python_callable=exec_spark,
    op_args=['tratamento_microdados'],
    dag=dag
)

task_modelagem_dims = PythonOperator(
    task_id='task_modelagem_dims',
    python_callable=exec_spark,
    op_args=['modelagem_dims'],
    dag=dag
)

task_truncar_tabelas = PythonOperator(
    task_id='task_truncar_tabelas',
    python_callable=truncar_tabelas,
    dag=dag
)

task_salvar_db = PythonOperator(
    task_id='task_salvar_db',
    python_callable=exec_spark,
    op_args=['salvar_db'],
    dag=dag
)

task_tratamento_dicionario >> task_tratamento_microdados >> task_modelagem_dims >> task_truncar_tabelas >> task_salvar_db
