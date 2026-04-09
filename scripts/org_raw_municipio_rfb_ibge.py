# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')


file = {'namespace':'/oni', 'file_folder':'/rfb/cnpj/tabelas_auxiliares/municipio_rfb_ibge/',
        'file_subfolder':'', 'sheet_name':'',         
        'raw_path':'/usr/oni/rfb/cnpj/tabelas_auxiliares/municipio_rfb_ibge',         
        'prm_path': '',          
        'extension':'csv','column_delimiter':';','encoding':'iso-8859-1','null_value':''}
        
adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = { "folders": { "landing": "/tmp/dev/uld", "error": "/tmp/dev/err", "staging": "/tmp/dev/stg", "log": "/tmp/dev/log", "raw": "/tmp/dev/raw", "archive": "/tmp/dev/ach" }, "systems": { "raw": "usr" }, "path_prefix": "" }

# COMMAND ----------

from cni_connectors import adls_connector as adls_conn

var_adls_uri, notebook_params = adls_conn.connect_adls()

import os
import re
import datetime
from unicodedata import normalize

import pandas as pd
import pyspark.pandas as ps
import crawler.functions as cf
import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index, col, when, explode, concat

from core.string_utils import normalize_replace

# COMMAND ----------

file = notebook_params.var_file
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

uld = dls['folders']['landing']
raw = dls['folders']['raw']
usr = dls['systems']['raw']

# COMMAND ----------

uld_path = "{uld}{namespace}{file_folder}{file_subfolder}".format(uld=uld, namespace=file['namespace'], file_folder=file['file_folder'],file_subfolder=file['file_subfolder'])
#adl_uld = f"{var_adls_uri}{uld_path}"
#print(adl_uld)

for path in cf.list_subdirectory(dbutils, uld_path):
  path_adl_uld = f'/{path}'
  print(path_adl_uld)

adl_uld = f"{var_adls_uri}{path_adl_uld}"
print(adl_uld)


raw_usr_path = "{raw}{raw_path}".format(raw=raw, raw_path=file['raw_path'])
adl_raw = f"{var_adls_uri}{raw_usr_path}"
print(adl_raw)

prm_path = "{prm_path}".format(prm_path=file['prm_path'])
print(prm_path)

# COMMAND ----------

if not cf.directory_exists(dbutils, uld_path):
  dbutils.notebook.exit('Path "%s" not exist or is empty' % uld_path)

def __normalize_str(_str):
    return re.sub(r'[,;{}()\n\t=-]', '', normalize('NFKD', _str)
                  .encode('ASCII', 'ignore')
                  .decode('ASCII')
                  .replace(' ', '_')
                  .replace('-', '_')
                  .replace('/', '_')
                  .replace('.', '_')
                  .replace('$', 'S')
                  .upper())

# COMMAND ----------

df = (spark.read
                .option("delimiter", file['column_delimiter'])
                .option("header", "true")
                .option("encoding", file['encoding'])
                .csv(adl_uld))

# COMMAND ----------

from pyspark.sql import functions as F

def verificar_caracteres_estranhos(df):
    """
    Verifica em todas as colunas string se existem valores com:
    - espaços em branco no início/fim
    - tabulações (\t)
    - quebras de linha (\n, \r)
    - duplos espaços
    
    Retorna um DataFrame com resumo por coluna.
    """
    
    checks = []
    for c in df.columns:
        # apenas colunas de texto
        if dict(df.dtypes)[c] == "string":
            cond = (
                (F.col(c).rlike(r'^\s+')) |   # começa com espaço
                (F.col(c).rlike(r'\s+$')) |   # termina com espaço
                (F.col(c).rlike(r'\t')) |     # contém tabulação
                (F.col(c).rlike(r'\n')) |     # contém quebra de linha
                (F.col(c).rlike(r'\r')) |     # contém retorno de carro
                (F.col(c).rlike(r'  +'))      # contém espaços duplos
            )
            checks.append(
                df.filter(cond)
                  .agg(F.count("*").alias("qtd_com_caracteres_estranhos"))
                  .withColumn("coluna", F.lit(c))
            )
    
    if not checks:
        print("⚠️ Nenhuma coluna do tipo string encontrada.")
        return None
    
    result = checks[0]
    for other in checks[1:]:
        result = result.unionByName(other)
    
    result = result.withColumn(
        "possui_problema",
        F.when(F.col("qtd_com_caracteres_estranhos") > 0, F.lit(True)).otherwise(F.lit(False))
    )
    
    return result.select("coluna", "qtd_com_caracteres_estranhos", "possui_problema")


# COMMAND ----------

resultado = verificar_caracteres_estranhos(df)
resultado.show(truncate=False)

# COMMAND ----------

df.display()

# COMMAND ----------

cols_normalizados = [__normalize_str(c) for c in df.columns]
df = df.toDF(*cols_norm
             alizados)  

# COMMAND ----------

from pyspark.sql.functions import lit

dh_insercao_raw = datetime.datetime.now()

df = (
    cf.append_control_columns(df, dh_insercao_raw=str(dh_insercao_raw))
    .select("*")
)

# COMMAND ----------

print(adl_raw)

# COMMAND ----------

# df.repartition(8)
df.write.mode("overwrite").option("compression", "snappy").parquet(adl_raw)