# Databricks notebook source
# Configurações de performance
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.broadcastJoinThreshold", 50 * 1024 * 1024)

# COMMAND ----------

dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')


dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')


# Databricks notebook source
tables =  {"schema":"",
           "table":"",
           "prm_path":"",
           "raw_path":"/usr/oni/rfb/cnpj/tabelas_auxiliares/municipio_rfb_ibge/",
           "trs_path":"/oni/rfb/cnpj/tabelas_auxiliares/municipio_rfb_ibge/"}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}


dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

from cni_connectors import adls_connector as adls_conn
var_adls_uri, notebook_params = adls_conn.connect_adls()

from pyspark.sql.functions import udf, from_utc_timestamp, current_timestamp, lit, input_file_name, monotonically_increasing_id, substring_index
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DecimalType, IntegerType
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
import os
from core.string_utils import normalize_replace

# COMMAND ----------

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf


# COMMAND ----------

raw = dls['folders']['raw']
trusted = dls['folders']['trusted']
prm_path = tables['prm_path']

# COMMAND ----------


raw_path = "{raw}{schema}{table}{raw_path}".format(raw=raw, schema=tables['schema'], table=tables['table'], raw_path=tables['raw_path'])
adl_raw = f'{var_adls_uri}{raw_path}'
print(adl_raw)

trusted_path = "{trusted}{schema}{table}{trusted_path}".format(trusted=trusted, schema=tables['schema'], table=tables['table'], trusted_path=tables['trs_path'])
adl_trusted = f'{var_adls_uri}{trusted_path}'
print(adl_trusted)


# COMMAND ----------

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

#prm_path = "{prm_path}".format(prm_path=tables['prm_path'])
#prm_path

# COMMAND ----------

df = spark.read.parquet(adl_raw)

# COMMAND ----------

col_rename_map = {
    "CODIGO_DO_MUNICIPIO___TOM": "CD_MUN_TOM",
    "CODIGO_DO_MUNICIPIO___IBGE": "CD_MUN_IBGE",
    "MUNICIPIO___TOM": "NM_MUN_TOM",
    "MUNICIPIO___IBGE": "NM_MUN_IBGE",
    "UF": "SG_UF"
}
df = df.toDF(*[col_rename_map.get(c, c) for c in df.columns])


# COMMAND ----------

df = tcf.add_control_fields(df, adf)

# COMMAND ----------

df = df.select(
    *[c for c in df.columns if c != "dh_insercao_raw"])

# COMMAND ----------

display(df)

# COMMAND ----------

print(adl_trusted)

# COMMAND ----------

# df.repartition(8)
df.write.mode("overwrite").option("compression", "snappy").parquet(adl_trusted)