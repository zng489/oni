# Databricks notebook source
# Concessão de acesso em ativos de dados via script Python
from unity_grants import unity_grants
 
# Lista de ativos para os quais o acesso será concedido e revogado
#ativos = ["datalake__raw_usr.oni.oni_observatorio_nacional_oferta_demanda_senai__relacao_cursos_cbo","datalake__trs.oni.oni_observatorio_nacional_oferta_demanda_senai__relacao_cursos_cbo","datalake__biz.oni.oni_bases_referencia__relacao_cursos_cbo"]
ativos = ['datalake__trs.oni.oni_ibge__cnae_subclasses_2_2']
# Lista de usuários para os quais o acesso será concedido e revogado
users = [
      "zhang.yuan@senaicni.com.br"
    #"t-franciele.padoan@cni.com.br"
]
 
# Loop através de cada ativo
for ativo in ativos:
    print(ativo)
    # Loop através de cada usuário
    for user in users:
      try:
        print(user)
        # Concede acesso ao ativo para o usuário
        unity_grants.grant_access(ativo, user)
        # REVOGA acesso ao ativo para o usuário
        # unity_grants.revoke_access(ativo, user)
      except:
        pass


uld_path = "/tmp/dev/trs/oni/ibge/"
for path in cf.list_subdirectory(dbutils, uld_path):
  print(path)



#spark.read.format('delta').load('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/painel_pcd/cota_empresa_pcd/tabela_agreg_empresa').printSchema()


# Copia os arquivos e diretórios do caminho especificado para o destino
source_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/trs/oni/ibge/cnae_subclasses_2_3'
destination_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/trs/oni/ibge/cnae_subclasses_2_3'

# Deleta os arquivos e diretórios no caminho especificado no Data Lake Storage Gen2 em DESENVOLVIMENTO
dbutils.fs.rm(destination_path, recurse=True)

# Copia os arquivos e diretórios
dbutils.fs.cp(source_path, destination_path, recurse=True)


#spark.read.parquet('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/uds/oni/observatorio_nacional/BACKUP/cota_empresa_pcd').printSchema() 


# COMMAND ----------

{"source":"/oni/ibge/cnae_subclasses_2_1","destination":"/oni/bases_referencia/cnae/cnae_21/cnae_subclasse","folder":"/oni/bases_referencia/cnae/cnae_21/","databricks":{"notebook":"/biz/oni/bases_referencia/cnae/trs_biz_cnae20"}}

# COMMAND ----------



# COMMAND ----------

dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# COMMAND ----------

table = {"source":"/oni/ibge/cnae_subclasses_2_1","destination":"/oni/bases_referencia/cnae/cnae_21/cnae_subclasse","folder":"/oni/bases_referencia/cnae/cnae_21/","databricks":{"notebook":"/biz/oni/bases_referencia/cnae/trs_biz_cnae20"}}

adf = { "adf_factory_name": "cnibigdatafactory", "adf_pipeline_name": "raw_trs_tb_email", "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82", "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016", "adf_trigger_time": "2024-05-07T00:58:48.0960873Z", "adf_trigger_type": "PipelineActivity" }

dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}


dls = {"folders":{"landing":"/tmp/dev/lnd","error":"/tmp/dev/err","archive":"/tmp/dev/ach","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","trusted":"/tmp/dev/trs","business":"/tmp/dev/biz","prm":"/tmp/dev/prm","historico":"/tmp/dev/hst","gov":"/tmp/dev/gov"},"path_prefix":"tmp","uld":{"folders":{"landing":"/tmp/dev/uld","error":"/tmp/dev/err","staging":"/tmp/dev/stg","log":"/tmp/dev/log","raw":"/tmp/dev/raw","archive":"/tmp/dev/ach"},"systems":{"raw":"usr"},"path_prefix":"/tmp/dev/"},"systems":{"raw":"usr"}}


# COMMAND ----------

from cni_connectors import adls_connector as adls_conn

var_adls_uri, notebook_params = adls_conn.connect_adls()

# COMMAND ----------

import json
import crawler.functions as cf
import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql import DataFrame
from cni_connectors import adls_connector as adls_conn
from trs_control_field import trs_control_field as tcf
from pyspark.sql.types import *
from pyspark.sql.types import StringType
import re


# COMMAND ----------

def lower_columns(x):
  for i in x.columns:
    x =x.withColumnRenamed(i, i.lower())
  return x

# COMMAND ----------

udf_capitalize = f.udf(lambda x : str(x).capitalize(), StringType())

# COMMAND ----------

table = notebook_params.var_tables
dls = notebook_params.var_dls
adf = notebook_params.var_adf

# COMMAND ----------

raw = dls['folders']['raw']
trs = dls['folders']['trusted']
biz = dls['folders']['business']

print(raw)
print(trs)
print(biz)

# COMMAND ----------

source = "{adl_path}{layer}{origin}".format(adl_path=var_adls_uri, layer=trs, origin=table["source"])
target = "{adl_path}{layer}{origin}".format(adl_path=var_adls_uri, layer=biz, origin=table["destination"])
target_folder = "{adl_path}{layer}{origin}".format(adl_path=var_adls_uri, layer=biz, origin=table["folder"])

print(source)
print(target)
print(target_folder)


# COMMAND ----------

# MAGIC %md
# MAGIC #Create lookup table

# COMMAND ----------

sector_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J' , 'K', 'L', 'M' , 'N', 'O' , 'P' ,'Q', 'R', 'S', 'T', 'U']
sector_ordem = ['1', '2', '3', '4', '4', '5', '6', *list('7'*14) ]
nm_macrosetor = ['Agropecuária', 'Industria Extrativa', 'Industria de Transformação', *(['SIUP']*2), 'Construção Civil', 'Comércio', *(['Serviços']*14)]
sector = zip(sector_list, sector_ordem, nm_macrosetor)

schema = StructType([
  StructField('cd_cnae_secao'.upper(), StringType()),
  StructField('cd_ordem'.upper(), StringType()),
  StructField('nm_macrosetor'.upper(), StringType()),
])

lookup_table =  spark.createDataFrame(sector,schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #Create CNAE TABLES:

# COMMAND ----------

print(source)

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC #Create lookup table

# COMMAND ----------

sector_list = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J' , 'K', 'L', 'M' , 'N', 'O' , 'P' ,'Q', 'R', 'S', 'T', 'U']
sector_ordem = ['1', '2', '3', '4', '4', '5', '6', *list('7'*14) ]
nm_macrosetor = ['Agropecuária', 'Industria Extrativa', 'Industria de Transformação', *(['SIUP']*2), 'Construção Civil', 'Comércio', *(['Serviços']*14)]
sector = zip(sector_list, sector_ordem, nm_macrosetor)

schema = StructType([
  StructField('cd_cnae_secao'.upper(), StringType()),
  StructField('cd_ordem'.upper(), StringType()),
  StructField('nm_macrosetor'.upper(), StringType()),
])

lookup_table =  spark.createDataFrame(sector,schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #Create CNAE TABLES:

# COMMAND ----------

# COMMAND ----------

cnae_origin = spark.read.parquet(source)

# COMMAND ----------

cnae_origin = cnae_origin.withColumn('ds_denominacao', udf_capitalize(f.col('ds_denominacao')))
cnae_origin = cnae_origin.withColumn('CD_GRUPO', f.regexp_replace(f.col('CD_GRUPO'),'[^0-9]+',''))
cnae_origin = cnae_origin.withColumn('CD_CLASSE', f.regexp_replace(f.col('CD_CLASSE'),'[^0-9]+',''))
cnae_origin = cnae_origin.withColumn('CD_SUBCLASSE', f.regexp_replace(f.col('CD_SUBCLASSE'),'[^0-9]+',''))

# COMMAND ----------

# MAGIC %md
# MAGIC ##  secao

# COMMAND ----------

secao = cnae_origin.filter(f.isnull(f.col('CD_DIVISAO')))
secao = secao.join(lookup_table, secao.CD_SECAO == lookup_table.CD_CNAE_SECAO).drop('CD_DIVISAO', 'CD_GRUPO', 'CD_CLASSE', 'CD_SUBCLASSE','CD_CNAE_SECAO')
secao = lower_columns(secao)
secao = secao.withColumnRenamed('cd_secao', 'cd_cnae_secao')
secao = secao.withColumnRenamed('ds_denominacao', 'nm_cnae_secao')
secao = tcf.add_control_fields(secao, adf, layer="biz")

# COMMAND ----------

# MAGIC %md
# MAGIC ## divisao

# COMMAND ----------

divisao = cnae_origin.filter(f.isnull(f.col('CD_DIVISAO'))=='false').filter(f.isnull(f.col('CD_GRUPO'))).drop('CD_GRUPO', 'CD_CLASSE' ,'CD_SUBCLASSE','dh_insercao_trs')
divisao = lower_columns(divisao)
divisao = divisao.withColumnRenamed('cd_secao', 'cd_cnae_secao')
divisao = divisao.withColumnRenamed('cd_divisao', 'cd_cnae_divisao')
divisao = divisao.withColumnRenamed('ds_denominacao', 'nm_cnae_divisao')
divisao = divisao.join(secao, 'cd_cnae_secao')

# COMMAND ----------

# MAGIC %md
# MAGIC ## grupo

# COMMAND ----------

grupo = cnae_origin.filter(f.isnull(f.col('CD_GRUPO'))=='false').filter(f.isnull(f.col('CD_CLASSE'))).select('CD_DIVISAO','CD_GRUPO','DS_DENOMINACAO')
grupo = lower_columns(grupo)
grupo = grupo.withColumnRenamed('cd_grupo', 'cd_cnae_grupo')
grupo = grupo.withColumnRenamed('cd_divisao', 'cd_cnae_divisao')
grupo = grupo.withColumnRenamed('ds_denominacao', 'nm_cnae_grupo')
grupo = grupo.join(divisao, 'cd_cnae_divisao')

# COMMAND ----------

# MAGIC %md
# MAGIC # classe

# COMMAND ----------

classe = cnae_origin.filter(f.isnull('CD_CLASSE')=='false').filter(f.isnull(f.col('CD_SUBCLASSE'))).select('CD_CLASSE', 'DS_DENOMINACAO', 'CD_GRUPO')
classe = lower_columns(classe)
classe = classe.withColumnRenamed('cd_classe', 'cd_cnae_classe')
classe = classe.withColumnRenamed('cd_grupo', 'cd_cnae_grupo')
classe = classe.withColumnRenamed('ds_denominacao', 'nm_cnae_classe')
classe = classe.join(grupo, 'cd_cnae_grupo')

# COMMAND ----------

# MAGIC %md
# MAGIC # sublcasse

# COMMAND ----------

subclasse = cnae_origin.filter(f.isnull('CD_SUBCLASSE')=='false').select('CD_SUBCLASSE', 'DS_DENOMINACAO', 'CD_CLASSE')
subclasse = lower_columns(subclasse)
subclasse = subclasse.withColumnRenamed('cd_subclasse', 'cd_cnae_subclasse')
subclasse = subclasse.withColumnRenamed('cd_classe', 'cd_cnae_classe')
subclasse = subclasse.withColumnRenamed('ds_denominacao', 'nm_cnae_subclasse')
subclasse = subclasse.join(classe, 'cd_cnae_classe')

# COMMAND ----------

# MAGIC %md
# MAGIC # Escrita

# COMMAND ----------

if "20" in source:
    #secao.write.mode('overwrite').parquet(target_folder + '/cnae_secao')
    #divisao.write.mode('overwrite').parquet(target_folder +'/cnae_divisao')
    #grupo.write.mode('overwrite').parquet(target_folder +'/cnae_grupo')
    #classe.write.mode('overwrite').parquet(target_folder +'/cnae_classe')
    #subclasse.write.mode('overwrite').parquet(target_folder +'/cnae_subclasse')
    print('script carregando subclasse 2.0')
else:
    print('script carregando subclasse 2.1, 2.2, 2.3')
    subclasse.write.mode('overwrite').parquet(target_folder +'/cnae_subclasse')
    


# COMMAND ----------

print(secao.count())
print(divisao.count())
print(grupo.count())
print(classe.count())
print(subclasse.count())