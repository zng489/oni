# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')


tables =  {"schema":"",
           "table":"",
           "prm_path":"",
           "raw_path":"/usr/oni/rfb/tributacao/renuncias_fiscais_cnpj/dirbi_dic_reg_esp_trib/",
           "trs_path":"/oni/rfb/tributacao/renuncias_fiscais_cnpj/dirbi_dic_reg_esp_trib/"}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}


dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

from cni_connectors import adls_connector as adls_conn

var_adls_uri, notebook_params = adls_conn.connect_adls()

from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index
from trs_control_field import trs_control_field as tcf
import pyspark.sql.functions as f
import crawler.functions as cf
from pyspark.sql import SparkSession
import time
import pandas as pd
from pyspark.sql.functions import col, when, explode, lit
import json
from unicodedata import normalize 
import datetime
import re
from core.string_utils import normalize_replace
from pyspark.sql.functions import concat, lit, col, substring

# COMMAND ----------

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

raw = dls['folders']['raw']
print(raw)

trusted = dls['folders']['trusted']
print(trusted)

prm_path = tables['prm_path']

usr = dls['systems']['raw']
print(usr)

raw_path = "{raw}{schema}{table}{raw_path}".format(raw=raw, schema=tables['schema'], table=tables['table'], raw_path=tables['raw_path'])
adl_raw = f'{var_adls_uri}{raw_path}'
print(adl_raw)

trusted_path = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trs_path'])
adl_trusted = f'{var_adls_uri}{trusted_path}'
print(adl_trusted)

# COMMAND ----------

df = spark.read.parquet(adl_raw)

# COMMAND ----------

col_rename_map = {
    'CAMPO': "CAMPO",
    'NOME_DO_CAMPO': "NM_CAMPO",
    'UNIDADE_DE_MEDIDA': "UN_MEDIDA",
    'VALORES_PERMITIDOS': "VL_PERMITIDOS",
    'DESCRICAO': "DS_CAMPO"
}

df = df.toDF(*[col_rename_map.get(c, c) for c in df.columns])

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# Lazy transformations
df = df.withColumn("CAMPO_ORIGINAL", col("CAMPO"))


# COMMAND ----------


# Suponha que queremos alterar apenas a coluna "col2"
df = df.withColumn("CAMPO", F.regexp_replace(F.col("CAMPO"), " / ", " "))
df = df.withColumn("CAMPO", F.regexp_replace(F.col("CAMPO"), " - ", " "))
df = df.withColumn("CAMPO", F.regexp_replace(F.col("CAMPO"), "/", " "))
df = df.withColumn("CAMPO", F.regexp_replace(F.col("CAMPO"), "_", " "))

# COMMAND ----------

df.display()

# COMMAND ----------

import unicodedata

def remove_accents(s):
    if s:
        return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return s

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

remove_accents_udf = udf(remove_accents, StringType())

df = df.withColumn("CAMPO", F.upper(remove_accents_udf(F.col("CAMPO"))))

# COMMAND ----------

df.display()

# COMMAND ----------

# Filtra apenas linhas onde CAMPO_ORIGINAL não contém "/"
df_filtered = df.filter(col("CAMPO_ORIGINAL").contains("/"))

# Lazy mode: nenhuma ação executada ainda
df_filtered.display()

# COMMAND ----------

from pyspark.sql import functions as F

# Filtra apenas as linhas que contêm "_"
df = df.filter(F.col("CAMPO").contains("_"))

# Exibe as linhas filtradas
df.display()

# COMMAND ----------

df = tcf.add_control_fields(df, adf)
df = df.select(*[c for c in df.columns if c != "dh_insercao_raw"])

# COMMAND ----------

df.display()

# COMMAND ----------

print(adl_trusted)

# COMMAND ----------

df.write.mode("overwrite").option("compression", "snappy").parquet(adl_trusted)

# COMMAND ----------

# MAGIC %md
# MAGIC