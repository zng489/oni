# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

adf = {"adf_factory_name":"adf-oni","adf_pipeline_name":"raw_trs_dirbi_dic_reg_esp_trib","adf_pipeline_run_id":"484fc6dd-f9c7-4fb5-887d-0011fdfa77bd","adf_trigger_id":"92c8b2c938104fc4bdb16d4f40c2da26","adf_trigger_name":"Sandbox","adf_trigger_time":"2025-11-11T10:54:45.4572203Z","adf_trigger_type":"Manual"}

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

tables = {"schema":"","table":"","prm_path":"","raw_path":"/usr/oni/rfb/tributacao/renuncias_fiscais_cnpj/dirbi_reg_esp_trib/","trs_path":"/oni/rfb/tributacao/renuncias_fiscais_cnpj/dirbi_reg_esp_trib/"}


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
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.types import *
from pyspark.sql.types import StringType
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


cols_pivot = [c for c in df.columns if '___' in c]

cols_pivot += ["QTDBENEFICIOSCOMVALOR", "VALORCONSOLIDADO", "BEN_18_VAL_CONSOLID"]

expr = "stack({}, {})".format(
    len(cols_pivot),
    ", ".join([f"'{c}', `{c}`" for c in cols_pivot])
)

df_unpivot = (
    df.select(
        "CNPJ", 
        "NOME", 
        "UF", 
        "CNAEPRINCIPAL", 
        "PERIODOAPURACAO", 
        F.expr(expr + " as (tipo_imposto, valor)")
    )
)


string_cols = [c.name for c in df_unpivot.schema.fields if isinstance(c.dataType, StringType)]

df_unpivot_clean = df_unpivot.select(
    *[
        (
            F.upper(  # maiúsculo
                F.regexp_replace(  # substitui "_" por espaço
                    F.regexp_replace(F.col(c), "___", " "),  # substitui "___" por espaço
                    "_", " "
                )
            ).alias(c)
        ) if c in string_cols else F.col(c)
        for c in df_unpivot.columns
    ]
)


# COMMAND ----------

# 🔹 Deixa todas as colunas maiúsculas
df_upper_cols = df_unpivot_clean.toDF(*[c.upper() for c in df_unpivot_clean.columns])

# COMMAND ----------

col_rename_map = {
    "CNPJ": "ID_CNPJ",
    "NOME": "NM_EMPRESA",
    "UF": "SG_UF",
    "CNAEPRINCIPAL": "CNAE_PRINCIPAL",
    "PERIODOAPURACAO": "PER_APURACAO",
    "TIPO_IMPOSTO": "TIPO_IMPOSTO",
    "VALOR": "VL_IMPOSTO",
}

df = df_upper_cols.toDF(*[col_rename_map.get(c, c) for c in df_upper_cols.columns])


# COMMAND ----------


df = df.withColumn(
    "VL_IMPOSTO",
    F.regexp_replace(F.col("VL_IMPOSTO"), ",", ".").cast(DoubleType())
)

# COMMAND ----------


# Mapeamento de tipos desejados
col_dtype_map = {
    "ID_CNPJ": StringType(),
    "NM_EMPRESA": StringType(),
    "SG_UF": StringType(),
    "CNAE_PRINCIPAL": StringType(),
    "PER_APURACAO": StringType(),
    "TIPO_IMPOSTO": StringType(),
    "VL_IMPOSTO": DoubleType(),
}

# Aplica o cast conforme o mapeamento
for col_name, new_type in col_dtype_map.items():
    if col_name in df.columns:
        df = df.withColumn(col_name, F.col(col_name).cast(new_type))


# COMMAND ----------

df = tcf.add_control_fields(df, adf)
df = df.select(*[c for c in df.columns if c != "dh_insercao_raw"])

# COMMAND ----------

print(adl_trusted)

# COMMAND ----------

df.write.mode("overwrite").option("compression", "snappy").parquet(adl_trusted)

# COMMAND ----------

from pyspark.sql import functions as F

df_filtered = (
    df.filter(F.upper(F.col("NM_EMPRESA")).like("EMBRAER%"))
    .groupBy("TIPO_IMPOSTO", "NM_EMPRESA")
    .agg(F.sum("VL_IMPOSTO"))
)

df_filtered.display()

df_filtered = df.filter(F.col("NOME").contains("EMBRAER"))
df_filtered.display()


from pyspark.sql import functions as F

df_filtered = (
    df.filter(F.upper(F.col("NM_EMPRESA")).like("EMBRAER%"))
    .groupBy("TIPO_IMPOSTO", "NM_EMPRESA")
    .agg(F.sum("valor").alias("VL_IMPOSTO"))
)

df_filtered.display()

from pyspark.sql import functions as F

df_filtered = df.filter(F.col("NOME").contains("EMBRAER"))
df_filtered.display()