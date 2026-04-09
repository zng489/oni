# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')


tables = {'schema':'', 'type_raw':'/crw', 'table':'',
          'trusted_path_1':'/oni/ocde/ativ_econ/int_tec/class_inten_tec/',
          'trusted_path_2':'/oni/mte/rais/identificada/rais_vinculo/',  
          'trusted_path_3':'/ibge/ipca/',       
          'business_path_1':'/oni/mapa_do_trabalho/mapa_trabalho_2024/bases_referencia/cnae/cnae_20/cnae_divisao/',
          'business_path_2':'/oni/mapa_do_trabalho/mapa_trabalho_2024/bases_referencia/municipios/', 
          'business_path_3':'/oni/mapa_do_trabalho/mapa_trabalho_2024/bases_referencia/cbo/cbo4/',
          'business_path_4':'/oni/painel_egressos_senai/senai_ep_producao/',                              
          'destination_1':'/oni/painel_egressos_senai/senai_ep_producao_rais/',                
          'databricks':{'notebook':'/biz/oni/observatorio_nacional/egressos_senai_ep/trs_biz_senai_ep_producao_rais'}, 
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
from pyspark.sql.window import Window
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

trusted_path_1 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_1'])
print(trusted_path_1)
adl_trusted_1 = f'{var_adls_uri}{trusted_path_1}'
print(adl_trusted_1)

trusted_path_2 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_2'])
print(trusted_path_2)
adl_trusted_2 = f'{var_adls_uri}{trusted_path_2}'
print(adl_trusted_2)

trusted_path_3 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_3'])
print(trusted_path_3)
adl_trusted_3 = f'{var_adls_uri}{trusted_path_3}'
print(adl_trusted_3)

# COMMAND ----------

business_path_1 = "{business}{schema}{table}{business_path}".format(business=dls['folders']['business'], schema=tables['schema'], table=tables['table'], business_path=tables['business_path_1'])
print(business_path_1)
adl_business_1 = f'{var_adls_uri}{business_path_1}'
print(adl_business_1)

business_path_2 = "{business}{schema}{table}{business_path}".format(business=dls['folders']['business'], schema=tables['schema'], table=tables['table'], business_path=tables['business_path_2'])
print(business_path_2)
adl_business_2 = f'{var_adls_uri}{business_path_2}'
print(adl_business_2)

business_path_3 = "{business}{schema}{table}{business_path}".format(business=dls['folders']['business'], schema=tables['schema'], table=tables['table'], business_path=tables['business_path_3'])
print(business_path_3)
adl_business_3 = f'{var_adls_uri}{business_path_3}'
print(adl_business_3)

business_path_4 = "{business}{schema}{table}{business_path}".format(business=dls['folders']['business'], schema=tables['schema'], table=tables['table'], business_path=tables['business_path_4'])
print(business_path_4)
adl_business_4 = f'{var_adls_uri}{business_path_4}'
print(adl_business_4)

# COMMAND ----------

# Egressos - base tratada pela Cecilia
df_egressos = spark.read.parquet(adl_business_4)

# RAIS
df_rais_bruto = spark.read.parquet(adl_trusted_2).where(f.col('ANO')>f.lit(2016))

# IPCA
ipca_bruto = spark.read.parquet(adl_trusted_3)

# COMMAND ----------

# Copia os arquivos e diretórios do caminho especificado para o destino
# source_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/biz/oni/mapa_do_trabalho/mapa_trabalho_2024/bases_referencia/cbo/cbo4/'
# destination_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/mapa_do_trabalho/mapa_trabalho_2024/bases_referencia/cbo/cbo4/'

# Copia os arquivos e diretórios
# dbutils.fs.cp(source_path, destination_path, recurse=True)

# COMMAND ----------

## Intensidade tecnologica
tec_bruto = spark.read.parquet(adl_trusted_1)
## CNAE
cnae_bruto = spark.read.parquet(adl_business_1)
## MUNIC
munic_bruto = spark.read.parquet(adl_business_2)
## CBO4
cbo_bruto = spark.read.parquet(adl_business_3)

# COMMAND ----------

destination_path = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['destination_1'])
adl_destination_path_1 = f'{var_adls_uri}{destination_path}'
print(adl_destination_path_1)

# COMMAND ----------

# Tabelas auxiliare spara complementar RAIS

tec = (tec_bruto
  .select(f.col('CD_CNAE20_DIVISAO').cast('integer'),
          f.col('INT_TEC_OCDE').alias('NM_INT_TEC_OCDE'))
  .distinct())

cnae = (cnae_bruto
  .select(f.col('cd_cnae_divisao').alias('CD_CNAE20_DIVISAO'),
          f.col('nm_cnae_divisao').alias('NM_CNAE_DIVISAO'),
          f.col('nm_cnae_secao').alias('CN_CNAE_SECAO'),
          f.col('nm_macrosetor').alias('NM_MACROSSETOR'),
          f.col('nm_macrossetor_agg').alias('NM_MACROSSETOR_AGG'),
          f.col('nm_industria').alias('NM_INDUSTRIA'))
  .distinct()
  .join(tec, 'CD_CNAE20_DIVISAO', 'left'))

munic =  (munic_bruto
  .select(f.col('cd_municipio').alias('CD_MUNICIPIO'),
          f.col('ds_municipio').alias('DS_MUNICIPIO'),
    f.col('nm_regiao').alias('NM_REGIAO'),
          f.col('nm_uf').alias('DS_UF'),
          f.col('cd_uf').alias('CD_UF'),
          f.col('sg_uf').alias('SG_UF'))
  .distinct())

cbo =  (cbo_bruto
       .select(f.col('cd_cbo1').alias('CD_CBO1'),
          f.col('ds_cbo1').alias('DS_CBO1'),
          f.col('cd_cbo2').alias('CD_CBO2'),
          f.col('ds_cbo2').alias('DS_CBO2'),
          f.col('cd_cbo3').alias('CD_CBO3'),
          f.col('ds_cbo3').alias('DS_CBO3'),
          f.col('cd_cbo4').alias('CD_CBO4'),
          f.col('ds_cbo4').alias('DS_CBO4'),
          f.col('ds_grupo_familia').alias('DS_GRUPO_FAMILIA'),
          f.col('ds_area_atuacao_detalhada').alias('DS_AREA_ATUACAO_DETALHADA'),
          f.col('cd_escolaridade_familia').alias('CD_ESCOLARIDADE_FAMILIA'),
          f.col('ds_escolaridade_familia').alias('DS_ESCOLARIDADE_FAMILIA'),
          )
       .distinct())

# COMMAND ----------



window = Window.partitionBy()

ipca = (ipca_bruto
           .where((f.col('DS_MES_COMPETENCIA')==f.lit("dezembro")) & (f.col('NR_ANO')>f.lit(2007)))
           .orderBy("NR_ANO")
           .withColumn('base_fixa',f.last(f.col('VL_IPCA')).over(window))
           .withColumn('INDICE_FIXO_ULTIMO_ANO',f.col('VL_IPCA')/f.col('base_fixa'))
           .select(f.col('NR_ANO').alias('ano'),'INDICE_FIXO_ULTIMO_ANO'))

df_rais_bruto = (df_rais_bruto
                .where(f.col('ANO')>=f.lit(2017))
                .join(ipca,['ANO'],'left')
                .withColumn('VL_REMUN_MEDIA_REAL', f.col('VL_REMUN_MEDIA_NOM')/f.col('INDICE_FIXO_ULTIMO_ANO'))
                .drop('INDICE_FIXO_ULTIMO_ANO')
)

# COMMAND ----------

# Ajustes nas datas de admissao e desligamento
# Para cada CPF por ano, seleciona apenas uma observaçãos, conforme prioridade:
## Vinc ativo em 31/12
## Se mais de 1 ativo, seleciona o de maior salario
## Se ainda sobrar, seleciona o que tem maior tempo de emprego


rais_vinc_select = (df_rais_bruto
                    .withColumn('DT_DESLIGAMENTO',f.when((f.col('FL_VINCULO_ATIVO_3112')==f.lit(0)) & 
                                                         (f.col('CD_MES_DESLIGAMENTO')!='{ñ') &
                                                         (f.col('CD_MES_DESLIGAMENTO')!='00') &
                                                         (f.col('DT_DIA_MES_ANO_DIA_DESLIGAMENTO')!='{ñ'),
                                                  f.concat_ws('-',
                                                              f.col('ANO'),
                                                              f.col('CD_MES_DESLIGAMENTO'),
                                                              f.col('DT_DIA_MES_ANO_DIA_DESLIGAMENTO'))))
                    .withColumn('DT_DESLIGAMENTO', f.to_date(f.col('DT_DESLIGAMENTO'),"yyyy-MM-dd"))
                   .withColumn('ANO_ADM',f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),5,4))
                   .withColumn('DT_ADMISSAO',f.concat_ws('-',
                                                         f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),5,4),
                                                         f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),3,2),
                                                         f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),1,2)))
                    .withColumn('DT_ADMISSAO', f.to_date(f.col('DT_ADMISSAO'),"yyyy-MM-dd"))
                    .select("ID_CPF","ANO","CD_CNAE20_DIVISAO","NR_MES_TEMPO_EMPREGO","FL_VINCULO_ATIVO_3112",
                            'VL_REMUN_MEDIA_REAL','CD_CBO4','CD_MUNICIPIO','CD_TIPO_VINCULO')
                    .where(f.col('ID_CPF')!='00000000000')
                    .orderBy('ID_CPF','ANO',f.desc('FL_VINCULO_ATIVO_3112'),f.desc('VL_REMUN_MEDIA_REAL'),f.desc('NR_MES_TEMPO_EMPREGO'))
                    .dropDuplicates(['ID_CPF','ANO'])
                    .withColumn('CD_FAIXA_SALARIAL',f.when(f.col('VL_REMUN_MEDIA_REAL')<=1500,1)
                                .otherwise(f.when((f.col('VL_REMUN_MEDIA_REAL')>1500) & (f.col('VL_REMUN_MEDIA_REAL')<=3000),2)
                                .otherwise(f.when((f.col('VL_REMUN_MEDIA_REAL')>3000) & (f.col('VL_REMUN_MEDIA_REAL')<=5000),3)
                                .otherwise(f.when((f.col('VL_REMUN_MEDIA_REAL')>5000) & (f.col('VL_REMUN_MEDIA_REAL')<=10000),4)
                                .otherwise(f.when((f.col('VL_REMUN_MEDIA_REAL')>=10000),5))
                               ))))
                     .withColumn('DS_FAIXA_SALARIAL',f.when(f.col('VL_REMUN_MEDIA_REAL')<=1500,"Até R$ 1500")
                                .otherwise(f.when((f.col('VL_REMUN_MEDIA_REAL')>1500) & (f.col('VL_REMUN_MEDIA_REAL')<=3000),"Entre R$ 1500 e R$ 3000")
                                .otherwise(f.when((f.col('VL_REMUN_MEDIA_REAL')>3000) & (f.col('VL_REMUN_MEDIA_REAL')<=5000),"Entre R$ 3000 e R$ 5000")
                                .otherwise(f.when((f.col('VL_REMUN_MEDIA_REAL')>5000) & (f.col('VL_REMUN_MEDIA_REAL')<=10000),"Entre R$ 5000 e R$ 10000")
                                .otherwise(f.when((f.col('VL_REMUN_MEDIA_REAL')>=10000),"Acima de R$ 10000"))
                               ))))
                     .withColumn('DS_VINCULO_APRENDIZ',f.when(f.col('CD_TIPO_VINCULO')=="55",'Sim').otherwise('Não'))
                    .drop('NR_MES_TEMPO_EMPREGO','CD_TIPO_VINCULO')
                    .withColumnRenamed('ID_CPF','CPF_ALUNO') 
                      )

# COMMAND ----------

egressos_select = (df_egressos
                   .select('CPF_ALUNO','NOME_ALUNO','DS_SEXO','VL_IDADE','DS_FAIXA_ETARIA','DS_RACA_COR_ALUNO','DS_PCD','DR','UNIDADE_ATENDIMENTO','CATEGORIA',
                           'MODALIDADE','AREA_ATUACAO',f.col('cd_ano_fechamento').alias('ANO_CONCLUSAO_CURSO'),'DS_GRATUIDADE','CD_TIPO_FINANCIAMENTO','DS_TIPO_FINANCIAMENTO',
                           f.col('cd_mes_fechamento').alias('CD_MES_FECHAMENTO'),
                           'DT_ENTRADA','DT_SAIDA',
                           'DR_SIGLA','CD_MODALIDADE','DS_MODALIDADE','CD_CATEGORIA','DS_CATEGORIA','CD_AREA_ATUACAO','DS_AREA_ATUACAO',
                           'CD_UNIDADE_ATENDIMENTO','DS_UNIDADE_ATENDIMENTO','CD_TIPO_ACAO','DS_TIPO_ACAO','DS_MUNICIPIO_ACAO','CD_MUNICIPIO_ACAO')
                     )


# COMMAND ----------

## Tabela com anos, para viabilizar join por CNPJ e ANO > construção do painel
ano_auxiliar = rais_vinc_select.select('ANO').distinct()

# Acrescenta os dados de UF, CNAE e de vinculos da rais
egressos_rais = (egressos_select
                 .join(ano_auxiliar, how = 'left')
                 .where(f.col('ANO')>=f.col('ANO_CONCLUSAO_CURSO'))
                 .join(rais_vinc_select, on = ['CPF_ALUNO','ANO'], how = 'left')
                 .join(munic, on = 'CD_MUNICIPIO', how = 'left')
                 .join(cnae, on = 'CD_CNAE20_DIVISAO', how = 'left')
                 .join(cbo, on = 'CD_CBO4', how = 'left')
                 .withColumnRenamed('ANO','ANO_RAIS')
                 .withColumn('VL_EMPREGADOS_3112',f.when(f.col('FL_VINCULO_ATIVO_3112')==1,1).otherwise(0))
                 .withColumn('VL_EMPREGADOS_3112_INDUSTRIA',f.when((f.col('FL_VINCULO_ATIVO_3112')==1) & (f.col('NM_INDUSTRIA')=="Indústria"),1).otherwise(0))
                 .withColumn('VL_EMPREGADOS_3112_OUTROS',f.when((f.col('FL_VINCULO_ATIVO_3112')==1) & (f.col('NM_INDUSTRIA')!="Indústria"),1).otherwise(0))
                 .withColumn('VL_SEM_EMPREGO_3112',f.when(f.col('FL_VINCULO_ATIVO_3112')!=1,1).otherwise(0))
                 .withColumn('VL_EMPREGADOS_ANO',f.when(f.col('FL_VINCULO_ATIVO_3112').isNotNull(),1).otherwise(0))
                  .withColumn('VL_EMPREGADOS_ANO_INDUSTRIA',f.when((f.col('FL_VINCULO_ATIVO_3112').isNotNull()) & (f.col('NM_INDUSTRIA')=="Indústria"),1).otherwise(0))
                .withColumn('VL_EMPREGADOS_ANO_OUTROS',f.when((f.col('FL_VINCULO_ATIVO_3112').isNotNull()) & (f.col('NM_INDUSTRIA')!="Indústria"),1).otherwise(0))
                 .withColumn('VL_SEM_EMPREGO_ANO',f.when(f.col('FL_VINCULO_ATIVO_3112').isNull(),1).otherwise(0))
                 .withColumn('VL_TOTAL_EGRESSOS',f.lit(1))
                 .withColumn('ANOS_POS_CONCLUSAO',f.col('ANO_RAIS')-f.col('ANO_CONCLUSAO_CURSO'))
                 .drop('NM_INDUSTRIA')
                 .withColumn('CD_EMPREGO_INDUSTRIAL',
                                f.when((f.col('NM_MACROSSETOR_AGG')=="Indústria") | (f.col('DS_GRUPO_FAMILIA')=="Ocupações industriais"),f.lit(1))
                                        .otherwise(f.lit(0)))
                 .withColumn('DS_EMPREGO_INDUSTRIAL',
                                f.when((f.col('NM_MACROSSETOR_AGG')=="Indústria") | (f.col('DS_GRUPO_FAMILIA')=="Ocupações industriais"),'Sim')
                                        .otherwise('Não'))
                 )

# COMMAND ----------

df = tcf.add_control_fields(egressos_rais, adf, layer="biz")

# COMMAND ----------

adl_destination_path_1

# COMMAND ----------

df.write.mode("overwrite").parquet(adl_destination_path_1)
#df.write.format("delta").mode("overwrite").save(adl_destination_path_1)

# COMMAND ----------

# total de registros
df.count()

# COMMAND ----------

display(df.limit(1000))

# COMMAND ----------

# Layout dos dados para observações com emprego RAIS
display(df.where(f.col('VL_EMPREGADOS_ANO')==1).limit(1000))

# COMMAND ----------

# Número de observvações por ano RAIS
display(df.groupBy('ANO_RAIS').count().orderBy('ANO_RAIS'))

# COMMAND ----------

# Número de observvações por ano RAIS
display(df.groupBy('ANO_RAIS').count().orderBy('ANO_RAIS'))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# Layout dos dados: campos nulls são comuns pq referem-se aos egresssos que não foram encontrados na RAIS
display(df_egressos_rais.limit(1000))

# COMMAND ----------

# Layout dos dados: campos nulls são comuns pq referem-se aos egresssos que não foram encontrados na RAIS
display(df.limit(1000))