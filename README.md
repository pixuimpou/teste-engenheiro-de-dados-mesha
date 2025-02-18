# Teste de Eng. de Dados

Foram utilizadas as tecnologias: 
- Spark (com PySpark) para fazer o tratamento dos dados
- Docker para containerização
- Airflow para orquestração das tarefas

As respostas das perguntas se encontram no arquivo *Respostas Desafio Mesha.docx*

## Modelagem:

- Modelagem dimensional no esquema estrela:
![Imagem da modelagem](https://i.imgur.com/HegZcFc.png)

    - **fato_enem:** Tabela com os dados de nota, presença, todas as FKs e indicadores
    - **dim_escola:** Tabela com o descritivo de todas as combinações das categorias de escola e uma SK
    - **dim_presenca:** Se liga com as colunas de presenca da fato, usada para colocar a descrição da presença
    - **dim_status_redacao:** Parecida com a dim_presenca, mas para a redação
    - **dim_cor_raca**: Descritivo da etnia do candidato

## Configuração do Ambiente (Linux)

**1. Imagens Docker**<br/><br/>

Foram usadas 2 imagens docker personalizadas para fazer a conexão com o banco de dados:

```
docker pull pixuimpou/airflow:2.4.0-mssql
docker pull pixuimpou/spark-py-mssql
```
**2. Variáveis de Ambiente**<br/><br/>

Defina as variáveis de ambiente:

- DESAFIO_MESHA_HOME
    - Deve ser a pasta raiz do repositorio
    - Exemplo: /home/rafael/desafio_mesha
- DESAFIO_MESHA_SQL_USER
    - Nome do usuário do banco de dados
- DESAFIO_MESHA_SQL_PASS
    - Senha do banco de dados
- AIRFLOW_UID
    - Seu id de usuario
    - AIRFLOW_UID=$(id -u)

**3. Dados**<br/><br/>

Coloque os arquivos *Dicionário_Microdados_Enem_2020.xlsx* e *MICRODADOS_ENEM_2020.csv* na diretorio `./mount/pyspark/data/raw`

## Iniciando o projeto

**1. Dentro da pasta docker execute o comando:**
    `docker compose -f docker-compose-ambiente.yml -f docker-compose-airflow.yaml up`

**2. Execute, no banco de dados que foi criado com o comando anterior, o script db_enem.sql que se encontra dentro da pasta DB**

**3. Acesse a url:** `http://localhost:8080`

**4. Na interface do Airflow, busque a dag "dag_desafio_mesha" e a execute**

**5. Após a execução terminar, o banco de dados já estará carregado**
   
