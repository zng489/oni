# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

tables =  {"schema":"",
           "table":"",
           "prm_path":"",
           "raw_path":"/usr/oni/rfb/tributacao/renuncias_fiscais_cnpj/dirbi_reg_esp_trib/",
           "trs_path":"/oni/rfb/tributacao/renuncias_fiscais_cnpj/dirbi_reg_esp_trib/"}

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

from pyspark.sql import functions as F

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


from pyspark.sql import functions as F
from pyspark.sql.types import StringType

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

df_unpivot_clean.display()

# COMMAND ----------

# 🔹 Deixa todas as colunas maiúsculas
df_upper_cols = df_unpivot_clean.toDF(*[c.upper() for c in df_unpivot_clean.columns])

df_upper_cols.display()

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

df.display()

# COMMAND ----------

from pyspark.sql import functions as F

# Filtra apenas as linhas que contêm "_"
df_filtered = df_unpivot_clean.filter(F.col("tipo_imposto").contains("_"))

# Exibe as linhas filtradas
df_filtered.display()




# COMMAND ----------



col_rename_map = {
    "CNPJ": "ID_CNPJ",
    "NOME": "NM_EMPRESA",
    "UF": "SG_UF",
    "CNAEPRINCIPAL": "CNAE_PRINCIPAL",
    "PERIODOAPURACAO": "PER_APURACAO",
    "QTDBENEFICIOSCOMVALOR": "QT_BENEFICIOS",
    "VALORCONSOLIDADO": "VL_CONSOLIDADO",
}

for n in range(18, 148):
    col_rename_map[f"BEN_{n}_VAL_CONSOLID"] = f"VL_BEN_{n}"
    col_rename_map[f"{n}___COFINS"] = f"COFINS_{n}"
    col_rename_map[f"{n}___PIS_PASEP"] = f"PIS_PASEP_{n}"
    col_rename_map[f"{n}___COFINS___IMPORTACAO"] = f"COFINS_IMP_{n}"
    col_rename_map[f"{n}___PIS_PASEP___IMPORTACAO"] = f"PIS_PASEP_IMP_{n}"

df = df.toDF(*[col_rename_map.get(c, c) for c in df.columns])


# O que é selectExpr()
# No PySpark, selectExpr() é uma forma de usar expressões SQL diretamente dentro de um DataFrame, sem precisar escrever o SQL completo em spark.sql().

df_unpivot = df.select(
    "id",
    F.expr(expr + " as (coluna, valor)")
)

#" as (coluna, valor)") .selectExpr(expr).withColumnRenamed("col0", "coluna").withColumnRenamed("col1", "valor")

df_unpivot.show()


'''
from pyspark.sql import functions as F

df_unpivot = df_unpivot.select(
    *[
        F.upper(F.regexp_replace(F.col(c), "___", " ")).alias(c)
        if "___" in c else F.col(c)
        for c in df_unpivot.columns
    ]
)

df_unpivot.display()
'''



'''
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# 🔹 Identifica todas as colunas string
string_cols = [c.name for c in df_unpivot.schema.fields if isinstance(c.dataType, StringType)]

# 🔹 Cria uma lista de expressões coluna por coluna
columns_to_select = []
for c in df_unpivot.columns:
    if c in string_cols:
        # Aplica upper + regexp_replace nos valores da coluna
        columns_to_select.append(F.upper(F.regexp_replace(F.col(c), "___", " ")).alias(c))
    else:
        # Mantém colunas que não são string intactas
        columns_to_select.append(F.col(c))

# 🔹 Cria o DataFrame final
df_unpivot_clean = df_unpivot.select(*columns_to_select)

# 🔹 Exibe o resultado
df_unpivot_clean.display()
'''

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# 🔹 Identifica todas as colunas string
string_cols = [c.name for c in df_unpivot.schema.fields if isinstance(c.dataType, StringType)]

# 🔹 Cria um novo DataFrame aplicando regexp_replace + upper só nas colunas string
df_unpivot_clean = df_unpivot.select(
    *[
        F.upper(F.regexp_replace(F.col(c), "___", " ")).alias(c)  # transforma valores
        if c in string_cols else F.col(c)  # mantém outras colunas intactas
        for c in df_unpivot.columns
    ]
)

# 🔹 Ação que dispara o cálculo
df_unpivot_clean.display()



# COMMAND ----------

df_unpivot.explain(True)

# COMMAND ----------

df.explain(True)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df = tcf.add_control_fields(df, adf)
df = df.select(*[c for c in df.columns if c != "dh_insercao_raw"])

# COMMAND ----------

print(adl_trusted)

# COMMAND ----------

df.write.mode("overwrite").option("compression", "snappy").parquet(adl_trusted)

# COMMAND ----------

