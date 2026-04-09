# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')


tables = {'schema':'', 'type_raw':'/crw', 'table':'',     
          'business_path_1':'/senai/EP/producao/fechamentos_publicados/',                     
          'destination_1':'/oni/painel_egressos_senai/senai_ep_producao/',                
          'databricks':{'notebook':'/biz/oni/observatorio_nacional/egressos_senai_ep/trs_biz_senai_ep_producao'}, 
          'prm_path':''}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}

# COMMAND ----------

# Biblioteca cni_connectors, que dá acesso aos dados no datalake
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
import os
from core.string_utils import normalize_replace

# COMMAND ----------

tables = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

trusted = dls['folders']['trusted']
business = dls['folders']['business']
sink = dls['folders']['business']

# COMMAND ----------

prm_path = os.path.join(dls['folders']['prm'])
print(prm_path)

prm_path = os.path.join(dls['folders']['prm'])
print(prm_path)

# COMMAND ----------

business_path_1 = "{business}{schema}{table}{business_path}".format(business=dls['folders']['business'], schema=tables['schema'], table=tables['table'], business_path=tables['business_path_1'])
print(business_path_1)

adl_business_1 = f'{var_adls_uri}{business_path_1}'
print(adl_business_1)

# COMMAND ----------

destination_path = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['destination_1'])
adl_destination_path_1 = f'{var_adls_uri}{destination_path}'
print(adl_destination_path_1)

# COMMAND ----------

analitico_ep_oficial = spark.read.parquet(f'{adl_business_1}')

# COMMAND ----------

df_senai_ep_prod = (analitico_ep_oficial.filter(f.col('cd_mes_fechamento') == 12)
                    .select('MES_REFERENCIA', 
'DR', 
'CD_MATRICULA', 
'UNIDADE_ATENDIMENTO', 
'CATEGORIA', 
'MODALIDADE', 
'AREA_ATUACAO', 
'TIPO_ACAO', 
'TIPO_FINANCIAMENTO',
'DT_ENTRADA', 
'DT_SAIDA', 
'MUNICIPIO_ACAO', 
'SITUACAO_MATRICULA', 
'CPF_ALUNO','NOME_ALUNO',
'SEXO_ALUNO',
'RACA_COR_ALUNO', 
'DT_NASC_ALUNO',
'NECESSID_ESPECIAL_ALUNO',
'cd_mes_fechamento',
'cd_ano_fechamento') 
)

# COMMAND ----------


biz_senai_ep_prod = (df_senai_ep_prod.filter(f.col('SITUACAO_MATRICULA') == '2 Concluída')
                    #.withColumn('MODALIDADE', f.translate(f.col('MODALIDADE'),'"',''))
                    #.withColumn('UNIDADE_ATENDIMENTO', f.translate(f.col('UNIDADE_ATENDIMENTO'),'"',''))
                    .withColumn('DR_SIGLA', f.substring('DR',7,5))
                    .withColumn('CD_MODALIDADE', f.split(df_senai_ep_prod['MODALIDADE'],' ')[0])
                    .withColumn('DS_MODALIDADE', f.split(df_senai_ep_prod['MODALIDADE'],' ',limit=2)[1])
                    .withColumn('CD_CATEGORIA', f.split(df_senai_ep_prod['CATEGORIA'],' ')[0])
                    .withColumn('DS_CATEGORIA', f.split(df_senai_ep_prod['CATEGORIA'],' ',limit=2)[1])
                    .withColumn('CD_AREA_ATUACAO', f.split(df_senai_ep_prod['AREA_ATUACAO'],' ')[0])
                    .withColumn('DS_AREA_ATUACAO', f.split(df_senai_ep_prod['AREA_ATUACAO'],' ',limit=2)[1])
                    .withColumn('CD_UNIDADE_ATENDIMENTO', f.split(df_senai_ep_prod['UNIDADE_ATENDIMENTO'],' ')[0])
                    .withColumn('DS_UNIDADE_ATENDIMENTO', f.split(df_senai_ep_prod['UNIDADE_ATENDIMENTO'],' ',limit=2)[1])
                    .withColumn('CD_TIPO_ACAO', f.split(df_senai_ep_prod['TIPO_ACAO'],' ')[0])
                    .withColumn('DS_TIPO_ACAO', f.split(df_senai_ep_prod['TIPO_ACAO'],' ',limit=2)[1])
                    .withColumn('CD_SITUACAO_MATRICULA', f.split(df_senai_ep_prod['SITUACAO_MATRICULA'],' ')[0])
                    .withColumn('DS_SITUACAO_MATRICULA', f.split(df_senai_ep_prod['SITUACAO_MATRICULA'],' ',limit=2)[1])
                    .withColumn('CD_MUNICIPIO_ACAO', f.substring(df_senai_ep_prod['MUNICIPIO_ACAO'],1,8))
                    .withColumn('CD_MUNICIPIO_ACAO',  f.translate(f.col('CD_MUNICIPIO_ACAO'),'(',''))
                    .withColumn('CD_MUNICIPIO_ACAO',  f.col('CD_MUNICIPIO_ACAO').cast('float'))
                    .withColumn('CD_UF_ACAO', f.substring('CD_MUNICIPIO_ACAO',1,2))
                    .withColumn('DS_MUNICIPIO_ACAO', f.split(df_senai_ep_prod['MUNICIPIO_ACAO'],'-',limit=2)[1])
                    .withColumn('CD_RACA_COR_ALUNO', f.split(df_senai_ep_prod['RACA_COR_ALUNO'],' ')[0])
                    .withColumn('DS_RACA_COR_ALUNO', f.split(df_senai_ep_prod['RACA_COR_ALUNO'],' ',limit=2)[1])
                    .withColumn('DS_RACA_COR_ALUNO', f.initcap('DS_RACA_COR_ALUNO'))
                    .withColumn('VL_ANO_NASCIMENTO', f.split(df_senai_ep_prod['DT_NASC_ALUNO'],'/')[2])
                    .withColumn('CD_TIPO_FINANCIAMENTO',f.split(df_senai_ep_prod['TIPO_FINANCIAMENTO'],' ')[0])
                    .withColumn('DS_TIPO_FINANCIAMENTO', f.split(df_senai_ep_prod['TIPO_FINANCIAMENTO'],' ',limit=2)[1])
                    .withColumn('DS_GRATUIDADE',f.when(f.col('CD_TIPO_FINANCIAMENTO').isin(['1','101','102','103','104']), 'Sim').otherwise('Não'))
                    )


# COMMAND ----------

biz_senai_ep_prod2 = (biz_senai_ep_prod
                     .withColumn('VL_IDADE',f.col('cd_ano_fechamento')-f.col('VL_ANO_NASCIMENTO'))
                     .withColumn('DS_FAIXA_ETARIA',f.when(f.col('VL_IDADE')<18,"17 anos ou menos")
                                                   .otherwise(f.when((f.col('VL_IDADE')>=18) & (f.col('VL_IDADE')<25),"18 a 24 anos")
                                                   .otherwise(f.when((f.col('VL_IDADE')>=25) & (f.col('VL_IDADE')<31),"25 a 30 anos")
                                                   .otherwise(f.when((f.col('VL_IDADE')>=31) & (f.col('VL_IDADE')<41),"31 a 40 anos")
                                                   .otherwise(f.when((f.col('VL_IDADE')>=41) & (f.col('VL_IDADE')<51),"41 a 50 anos")
                                                   .otherwise(f.when((f.col('VL_IDADE')>=51) & (f.col('VL_IDADE')<61),"51 a 60 anos")
                                                   .otherwise(f.when((f.col('VL_IDADE')>=61),"61 anos ou mais"))))))))
                     .withColumn('CD_FAIXA_ETARIA',f.when(f.col('VL_IDADE')<18,1)
                                                   .otherwise(f.when((f.col('VL_IDADE')>=18) & (f.col('VL_IDADE')<25),2)
                                                   .otherwise(f.when((f.col('VL_IDADE')>=24) & (f.col('VL_IDADE')<31),3)
                                                   .otherwise(f.when((f.col('VL_IDADE')>=31) & (f.col('VL_IDADE')<41),4)
                                                   .otherwise(f.when((f.col('VL_IDADE')>=41) & (f.col('VL_IDADE')<51),5)
                                                   .otherwise(f.when((f.col('VL_IDADE')>=51) & (f.col('VL_IDADE')<6160),6)
                                                   .otherwise(f.when((f.col('VL_IDADE')>=61),7))))))))
                     .withColumn('DS_SEXO', f.when(f.col('SEXO_ALUNO') == 'M', "Masculino").otherwise('Feminino'))
                     .withColumn('DS_PCD', f.when(f.col('NECESSID_ESPECIAL_ALUNO') == '9 Nenhuma', "Não").otherwise('Sim'))
)

# COMMAND ----------

adl_destination_path_1

# COMMAND ----------

df = tcf.add_control_fields(biz_senai_ep_prod2, adf, layer="biz")

# COMMAND ----------

df.write.mode("overwrite").parquet(adl_destination_path_1)
#df.write.format("delta").mode("overwrite").save(adl_destination_path_1)

# COMMAND ----------

dbutils.fs.rm(adl_destination_path_1, recurse=True)

# COMMAND ----------

df.write.mode("overwrite").parquet(adl_destination_path_1)

# COMMAND ----------

# Total de registros
df.count()

# COMMAND ----------

#layout dos dados
display(df.limit(1000))

# COMMAND ----------

# Número de observações por ano de conculsão do curso
display(df.groupBy('cd_ano_fechamento').count().orderBy('cd_ano_fechamento'))

# COMMAND ----------

