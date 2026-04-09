# Databricks notebook source
# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')


file = {'schema':'oni/obrasgov',
 'table':'cipi_intervencao',
 'prm_path':'',
 'raw_path':'/crw/oni/obrasgov/cipi/intervencao/obrasgov_cipi_intervencao/'}
      
adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}


lnd_path = "{lnd}/crw/{schema}__{table}".format(lnd=lnd, schema=file["schema"], table=file["table"])
adl_lnd = f"{var_adls_uri}{lnd_path}"
print(adl_lnd)

# COMMAND ----------

# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')


table = {'schema':'/crw/oni/obrasgov__',
 'table':'cipi_intervencao',
 'prm_path':'',
 'raw_path':'/crw/oni/obrasgov/cipi/intervencao/obrasgov_cipi_intervencao/'}
      
adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}


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

table = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

lnd = dls['folders']['landing']
raw = dls['folders']['raw']
print(lnd)
print(raw)

# COMMAND ----------

lnd_path = "{lnd}{schema}{table}/".format(lnd=lnd, schema=table["schema"], table=table["table"])
adl_lnd = f"{var_adls_uri}{lnd_path}"
print(adl_lnd)

# COMMAND ----------

raw_lnd_path = "{raw}{raw_path}".format(raw=raw, raw_path=file['raw_path'])
adl_raw = f"{var_adls_uri}{raw_lnd_path}"
print(adl_raw)

# COMMAND ----------

if not cf.directory_exists(dbutils, lnd_path):
  dbutils.notebook.exit('Path "%s" not exist or is empty' % lnd_path)

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
                .option("delimiter", ";")
                .option("header", "true")
                .option("encoding", "utf-8")
                .csv(adl_lnd))

# COMMAND ----------

cols_normalizados = [__normalize_str(c) for c in df.columns]
df = df.toDF(*cols_normalizados)  

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

df.write.mode("overwrite").option("compression", "snappy").parquet(adl_raw)