# Databricks notebook source
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

cols_para_manter = [c for c in df.columns if c != "_c0"]
df = df.select(*cols_para_manter)

# COMMAND ----------


cols_normalizados = [__normalize_str(c) for c in df.columns]
df = df.toDF(*cols_normalizados)  

# COMMAND ----------

from pyspark.sql.functions import lit

dh_insercao_raw = datetime.datetime.now()

df = (
    cf.append_control_columns(df, dh_insercao_raw=str(dh_insercao_raw))
    .select("*"))

# COMMAND ----------

print(adl_raw)

# COMMAND ----------

df.write.mode("overwrite").option("compression", "snappy").parquet(adl_raw)
