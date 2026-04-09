# Databricks notebook source
# MAGIC %md
# MAGIC # Painel de Cotas PcD
# MAGIC - Tabela Agregada por CNPJ raiz
# MAGIC
# MAGIC O seguinte notebook traz os dados de total de vínculos e PcDs (exceto para aprendizes) para o painel de cotas PcD, agregando os valores pelo CNPJ raiz (8 dígitos) e utilizando a base da RFB para obtenção da Razão Social, CNAE, Município e Situação cadastral da Matriz. 
# MAGIC
# MAGIC Data: 13/8/24
# MAGIC Autor: Maria Cecília Ramalho
# MAGIC
# MAGIC Edit - Dez 2024: Anaely Machado
# MAGIC
# MAGIC Card referência: https://trello.com/c/b3qLN9wB/850-painel-de-cotas-pcd

# COMMAND ----------

# Databricks notebook source
dbutils.widgets.text("user_parameters", '{"null": "null"}')

dbutils.widgets.text("env", 'dev')

dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')


tables = {'schema':'', 'type_raw':'/crw', 'table':'',           'trusted_path_1':'/oni/mte/rais/identificada/rais_vinculo/ANO=2023/',           'trusted_path_2':'/oni/mte/rais/identificada/rais_estabelecimento/NR_ANO=2023/',           'trusted_path_3':'/oni/rfb_cnpj/cadastro_empresa_f/',            'trusted_path_4':'/oni/rfb_cnpj/cadastro_estbl_f/',              'trusted_path_5':'/oni/observatorio_nacional/cota_empresa_pcd/',           'business_path_1':'/oni/bases_referencia/municipios/',           'business_path_2':'/oni/bases_referencia/cnae/cnae_20/cnae_divisao/',           'destination_1':'/oni/painel_pcd/cota_empresa_pcd/tabela_agreg_empresa/',           'destination_2':'/oni/painel_pcd/cota_empresa_pcd/tabela_agreg_estab/',           'databricks':{'notebook':'/biz/oni/painel_pcd/trs_biz_e_biz_biz_painel_pcd'}, 'prm_path':''}

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

trusted_path_4 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_4'])
print(trusted_path_4)
adl_trusted_4 = f'{var_adls_uri}{trusted_path_4}'
print(adl_trusted_4)

trusted_path_5 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_5'])
print(trusted_path_5)
adl_trusted_5 = f'{var_adls_uri}{trusted_path_5}'
print(adl_trusted_5)




business_path_1 = "{business}{schema}{table}{business_path}".format(business=dls['folders']['business'], schema=tables['schema'], table=tables['table'], business_path=tables['business_path_1'])
print(business_path_1)

adl_business_1 = f'{var_adls_uri}{business_path_1}'
print(adl_business_1)

business_path_2 = "{business}{schema}{table}{business_path}".format(business=dls['folders']['business'], schema=tables['schema'], table=tables['table'], business_path=tables['business_path_2'])
print(business_path_2)

adl_business_2  = f'{var_adls_uri}{business_path_2}'
print(adl_business_2)

# COMMAND ----------

df_rais_vinculo_0 = spark.read.parquet(f'{adl_trusted_1}')

df_rais_estabelecimento = spark.read.parquet(f'{adl_trusted_2}')

df_rfb_empresa_ori = spark.read.parquet(f'{adl_trusted_3}')

df_rfb_estbl_ori = spark.read.parquet(f'{adl_trusted_4}')

df_munic_0  = spark.read.format("delta").load(f'{adl_trusted_5}')
#df_munic_0 = spark.read.parquet(f'{adl_trusted_5}')

df_ibge_munic = spark.read.parquet(f'{adl_business_1}')

df_cnae_div = spark.read.parquet(f'{adl_business_2}')


# COMMAND ----------

# MAGIC %md
# MAGIC ## Data quality - retira os registros com inconsistência no CNPJ (erro na fonte dos dados originais)

# COMMAND ----------

df_munic = (df_munic_0.withColumn("cd_uf", f.substring("cd_mun_ibge", 1, 2))
            .withColumn('nm_mun_ibge', f.when(f.col('cd_mun_rfb') == '9997', 'Coronel Sapucaia').otherwise(f.col('nm_mun_ibge')))
            .withColumn('CD_MUN_RFB', f.lpad(f.trim(df_munic_0['CD_MUN_RFB']), 4, '0'))
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Tratamento RAIS vínculo
# MAGIC - Data quality
# MAGIC - Identificação do estoque de referência para o cálculo da cota
# MAGIC - Filtra variáveis relevantes
# MAGIC - Seleciona apenas vínculos ativos, empresas ativas, etc
# MAGIC - agregar por EMPRESA (CNPJ raiz) para calcular as faixas dos % de cotas

# COMMAND ----------

##### ATENÇÃO: CORRIGIR NA RAIS VINCULOS PROBLEMAS COM CNPJ_BASICO DIFERENTE DO CNPJ14
df_vinculos_trs = (df_rais_vinculo_0.withColumn('ID_CNPJ_RAIZ_CORR', f.substring("ID_CNPJ_CEI", 1, 8))
                                    .withColumn('conta_cnpj_raiz', f.length(f.trim('ID_CNPJ_RAIZ')))
                                    .withColumn('conta_cnpj_14', f.length(f.trim('ID_CNPJ_CEI')))
                                    .filter((f.col('conta_cnpj_14') == 14) & (f.col('conta_cnpj_raiz') == 8) & (f.col('ID_CNPJ_RAIZ') == f.substring("ID_CNPJ_CEI", 1, 8)))
                    )

# COMMAND ----------


variaveis_vinc = ['ID_CNPJ_RAIZ','ID_CNPJ_CEI', 'ID_RAZAO_SOCIAL', 'FL_VINCULO_ATIVO_3112',
                  'FL_IND_PORTADOR_DEFIC','ID_PIS','ID_CPF', 'CD_TIPO_VINCULO',
                  'FL_IND_TRAB_INTERMITENTE','CD_CNAE20_DIVISAO','CD_TIPO_DEFIC',
                  'DT_DIA_MES_ANO_DATA_ADMISSAO','FL_IND_CEI_VINCULADO','CD_TIPO_ESTAB','CD_UF','CD_MUNICIPIO_TRAB','CD_MUNICIPIO']

filtro_radar = [ '10', '15', '20', '25', '50', '60', '65', '70', '75', '90']

df_vinculos = (df_vinculos_trs
              .select(*variaveis_vinc)
              .filter((f.col('CD_TIPO_ESTAB') == f.lit('01')) &
                       (f.col('FL_VINCULO_ATIVO_3112') == 1) &
                       (f.col('FL_IND_TRAB_INTERMITENTE') != 1)
                     )
              .withColumn('DT_ADMISSAO',f.concat_ws('-',
                                                         f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),5,4),
                                                         f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),3,2),
                                                         f.substring(f.col('DT_DIA_MES_ANO_DATA_ADMISSAO'),1,2)))
              .withColumn('DT_ADMISSAO', f.to_date(f.col('DT_ADMISSAO'),"yyyy-MM-dd"))
              .orderBy("ID_CNPJ_RAIZ", "ID_PIS","DT_ADMISSAO")
              .dropDuplicates(["ID_CNPJ_RAIZ", "ID_PIS"])
              .withColumn('TOTAL_VINC', f.when(f.col('CD_TIPO_VINCULO') != "55",f.lit(1)).otherwise(f.lit(0)))  
              .withColumn('TOTAL_VINC_RADAR', f.when(f.col('CD_TIPO_VINCULO').isin(filtro_radar),f.lit(1))
                                .otherwise(f.lit(0)))
              .withColumn('TOTAL_VINC_PCD', f.when((f.col('TOTAL_VINC') == 1) & 
                                                         (f.col('FL_IND_PORTADOR_DEFIC') == 1),f.lit(1))
                                .otherwise(f.lit(0))) 
              .withColumn('TOTAL_VINC_RADAR_PCD', f.when((f.col('TOTAL_VINC_RADAR') == 1) & (f.col('FL_IND_PORTADOR_DEFIC') == 1),f.lit(1)).otherwise(f.lit(0)))   
              .withColumn('TOTAL_APRENDIZ', f.when(f.col('CD_TIPO_VINCULO') == 55,f.lit(1)).otherwise(f.lit(0)))
              .withColumn('TOTAL_APRENDIZ_PCD', f.when((f.col('CD_TIPO_VINCULO') == 55) & (f.col('FL_IND_PORTADOR_DEFIC') == 1),f.lit(1)).otherwise(f.lit(0)))
              .withColumn('ANO', f.lit(2023))
              .withColumn("FL_SETOR_ADMIN_RAIS", f.when(f.col('CD_CNAE20_DIVISAO') == "84",f.lit(1)).otherwise(f.lit(0)))                     
              )

# COMMAND ----------

# MAGIC %md
# MAGIC # Preparação dos dados RFB

# COMMAND ----------

# MAGIC %md
# MAGIC ## cria o arquivo com divisão CNAE da RAIS para cruzar com os dados da RFB
# MAGIC
# MAGIC a partir da base df_vinculos_trs, agrupa por CNPJ, divisão CNAE e flag de setor administrativo criando df_divisao_rais, que não pode ter CNPJ repetido, ou seja, com CNPJ com mais de uma divisão CNAE.

# COMMAND ----------

# cria a base de CNPJ's da base da RAIS de trabalho (df_vinculos)

df_divisao_rais = df_vinculos.withColumnRenamed('ID_CNPJ_CEI','ID_CNPJ_CEI_AUX').groupBy('ID_CNPJ_CEI_AUX','CD_CNAE20_DIVISAO','FL_SETOR_ADMIN_RAIS').count()

# COMMAND ----------

# PREPARA A BASE DE EMPRESAS DA RFB - PRECISOU RETIRAR ESSE CNPJ BÁSICO QUE ESTAVA DUPLICADO
 
df_rfb_empresa = (df_rfb_empresa_ori
                  .select("CD_CNPJ_BASICO", "NM_RAZAO_SOCIAL",'CD_NATUREZA_JURIDICA')
                  .withColumn("DELETA", f.when((f.col('CD_CNPJ_BASICO') == "07947717") & (f.col('CD_NATUREZA_JURIDICA') == "0000"),f.lit(1)).otherwise(f.lit(0)))
                  .filter(f.col('DELETA') == 0 )
                  .withColumnRenamed('CD_CNPJ_BASICO','CD_CNPJ_BASICO_RFB_EMP')
                  .withColumnRenamed('NM_RAZAO_SOCIAL','NM_RAZAO_SOCIAL_RFB_EMP')
                  .withColumnRenamed('CD_NATUREZA_JURIDICA','CD_NATUREZA_JURIDICA_RFB_EMP')
                  .drop('DELETA')
                  )

# COMMAND ----------

# PREPARA A BASE DE ESTABELECIMENTOS DA RFB
 
df_rfb_estb_aux1 = (df_rfb_estbl_ori
                     .withColumn("CD_CNAE20_DIV_RFB_EST", f.substring("CD_CNAE20_SUBCLASSE_PRINCIPAL", 1, 2))
                     .withColumn("FL_SIT_ATIVA_RFB_EST", f.when(f.col('DS_SIT_CADASTRAL') == "Ativa",f.lit(1)).otherwise(f.lit(0)))
                     .withColumn("FL_SETOR_ADMIN_RFB_EST", f.when(f.col('CD_CNAE20_DIV_RFB_EST') == "84",f.lit(1)).otherwise(f.lit(0)))
                     .withColumnRenamed('CD_CNPJ','CD_CNPJ_RFB_EST')
                     .withColumnRenamed('CD_CNPJ_BASICO','CD_CNPJ_BASICO_RFB')
                     .withColumnRenamed('CD_MATRIZ_FILIAL','CD_MATRIZ_FILIAL_RFB_EST')
                     .withColumnRenamed('CD_SIT_CADASTRAL','CD_SIT_CADASTRAL_RFB_EST')
                     .withColumnRenamed('CD_CEP','CD_CEP_RFB_EST')
                     .withColumnRenamed('SG_UF','SG_UF_RFB_EST')
                     .withColumnRenamed('CD_MUN','CD_MUN_RFB_EST')
                     .select('CD_CNPJ_RFB_EST','CD_CNPJ_BASICO_RFB','CD_MATRIZ_FILIAL_RFB_EST', 'CD_SIT_CADASTRAL_RFB_EST','CD_CEP_RFB_EST','SG_UF_RFB_EST','CD_MUN_RFB_EST','CD_CNAE20_DIV_RFB_EST', 'FL_SIT_ATIVA_RFB_EST', 'FL_SETOR_ADMIN_RFB_EST')
                     )



# COMMAND ----------

# faz o JOIN de RFB-estabelecimento com df_divisao_rais para pegar o CNAE do estabelecimento

df_rfb_estb_aux2 = (
    df_rfb_estb_aux1.join(
    df_divisao_rais,
    df_rfb_estb_aux1.CD_CNPJ_RFB_EST == df_divisao_rais.ID_CNPJ_CEI_AUX,
    'right'
    )
    .drop('count')
)


# COMMAND ----------

# faz o JOIN de RFB-estabelecimento com RFB-empresa pelo CNPJ matriz para buscar os dados da MATRIZ

df_rfb_estb_aux3 = (
    df_rfb_estb_aux2.join(
    df_rfb_empresa,
    df_rfb_estb_aux2.CD_CNPJ_BASICO_RFB == df_rfb_empresa.CD_CNPJ_BASICO_RFB_EMP,
    'left'
    )
    #.drop('CD_CNPJ_BASICO_RFB_EMP')
)

# COMMAND ----------


# faz o JOIN de RFB-estabelecimento com MUNICIPIO

df_rfb_estb = (df_rfb_estb_aux3
                .join(df_munic, df_rfb_estb_aux3.CD_MUN_RFB_EST == df_munic.CD_MUN_RFB,'left')
                .withColumnRenamed('cd_uf','CD_UF')
                .withColumnRenamed('sg_uf','SG_UF_ESTB')
                .withColumnRenamed('cd_mun_ibge','CD_MUNICIPIO')
                .withColumnRenamed('nm_mun_ibge','NM_MUNIC_ESTB')
                .drop('CD_MUN_RFB','NM_MUN_RFB')
                )


# COMMAND ----------

df_rfb_estb_0 = df_rfb_estb.orderBy(f.col('CD_CNPJ_BASICO_RFB'),f.col('CD_MATRIZ_FILIAL_RFB_EST'),f.col('ID_CNPJ_CEI_AUX'))
df_rfb_estb_0 = df_rfb_estb_0.dropDuplicates(['CD_CNPJ_BASICO_RFB'])
#df_rfb_estb_0.count()

# COMMAND ----------

df_rfb_matriz = (df_rfb_estb_0
                 .withColumnRenamed('CD_MATRIZ_FILIAL_RFB_EST','CD_MATRIZ_FILIAL_RFB_MTZ')
                 .withColumnRenamed('CD_CNPJ_RFB_EST','CD_CNPJ_RFB_MTZ')
                 .withColumnRenamed('CD_CNAE20_DIV_RFB_EST','CD_CNAE20_DIV_RFB_MTZ')
                 .withColumnRenamed('FL_SIT_ATIVA_RFB_EST','FL_SIT_ATIVA_RFB_MTZ')
                 .withColumnRenamed('CD_SIT_CADASTRAL_RFB_EST','CD_SIT_CADASTRAL_RFB_MTZ')
                 .withColumnRenamed('CD_MUN_RFB_EST','CD_MUN_RFB_MTZ')
                 .withColumnRenamed('CD_MUNICIPIO','CD_MUN_IBGE_MTZ')
                 .withColumnRenamed('NM_MUNIC_ESTB','NM_MUN_IBGE_MTZ')
                 .withColumnRenamed('SG_UF_RFB_EST','SG_UF_RFB_MTZ')
                 .withColumnRenamed('SG_UF_ESTB','SG_UF_IBGE_MTZ')
                 .withColumnRenamed('CD_UF','CD_UF_RFB_MTZ')
                 .withColumnRenamed('CD_CEP_RFB_EST','CD_CEP_RFB_MTZ')
                 .withColumnRenamed('FL_SETOR_ADMIN_RFB_EST','FL_SETOR_ADMIN_RFB_MTZ')
                 )

   

# COMMAND ----------

# MAGIC %md
# MAGIC # PARTE 1 - Vínculos PCD por matriz

# COMMAND ----------

# SOMA OS TOTAIS POR EMPRESA (CNPJ RAIZ)
# 8 observações são de CNPJs duplicados, que aparecem em muninipios diferentes.  
# A ideia é excliuir uma das linhas, ficando apenas com a que tem maior número de vínculos

#  .groupBy('ID_CNPJ_RAIZ','CD_MUNICIPIO','CD_UF','CD_CNAE20_DIVISAO')  <==== este agg foi feito pela ANAELY
#            'CD_CNPJ_BASICO_RFB','SG_UF_RFB_EST','CD_MUN_RFB_EST'

df_vinc_matriz = (df_vinculos
            .groupBy('ID_CNPJ_RAIZ','ANO')
            .agg(f.count('ID_CPF').alias('TOTAL_TRAB'),  
                 f.sum('TOTAL_VINC').alias('TOTAL_VINC'),
                 f.sum('TOTAL_VINC_PCD').alias('TOTAL_VINC_PCD'),
                 f.sum('TOTAL_VINC_RADAR').alias('TOTAL_VINC_RADAR'),
                 f.sum('TOTAL_VINC_RADAR_PCD').alias('TOTAL_VINC_RADAR_PCD'),
                 f.sum('TOTAL_APRENDIZ').alias('TOTAL_APRENDIZ'),
                 f.sum('TOTAL_APRENDIZ_PCD').alias('TOTAL_APRENDIZ_PCD')
                 )
                )
                    

# COMMAND ----------

# CRIA AS FAIXAS DE TRABALHADORES PARA ATRIBUIR O PERCENTUAL DE PCD DESEJADO

df_vinc_matriz_faixa = (df_vinc_matriz
            .withColumn("PERC_DESEJADO", when(f.col("TOTAL_VINC") < 100, 0)
                .when((f.col("TOTAL_VINC") >= 100) & (f.col("TOTAL_VINC") <= 200), 0.02)
                .when((f.col("TOTAL_VINC") >= 201) & (f.col("TOTAL_VINC") <= 500), 0.03)
                .when((f.col("TOTAL_VINC") >= 501) & (f.col("TOTAL_VINC") <= 1000), 0.04)
                .otherwise(0.05))
            .withColumn("TAMANHO_AGREG",when(f.col("PERC_DESEJADO") == 0, "Até 99 empregados")
                .when(f.col("PERC_DESEJADO") == 0.02, "De 100 a 200 empregados")
                .when(f.col("PERC_DESEJADO") == 0.03, "De 201 a 500 empregados")
                .when(f.col("PERC_DESEJADO") == 0.04, "De 501 a 1.000 empregados")
                .otherwise("Mais de 1.000 empregados"))
            .withColumn("PERC_DESEJADO_RADAR", when(f.col("TOTAL_VINC_RADAR") < 100, 0)
                .when((f.col("TOTAL_VINC_RADAR") >= 100) & (f.col("TOTAL_VINC_RADAR") <= 200), 0.02)
                .when((f.col("TOTAL_VINC_RADAR") >= 201) & (f.col("TOTAL_VINC_RADAR") <= 500), 0.03)
                .when((f.col("TOTAL_VINC_RADAR") >= 501) & (f.col("TOTAL_VINC_RADAR") <= 1000), 0.04)
                .otherwise(0.05))
            .withColumn("TAMANHO_AGREG_RADAR",when(f.col("PERC_DESEJADO_RADAR") == 0, "Até 99 empregados")
                .when(f.col("PERC_DESEJADO_RADAR") == 0.02, "De 100 a 200 empregados")
                .when(f.col("PERC_DESEJADO_RADAR") == 0.03, "De 201 a 500 empregados")
                .when(f.col("PERC_DESEJADO_RADAR") == 0.04, "De 501 a 1.000 empregados")
                .otherwise("Mais de 1.000 empregados"))
            )

# COMMAND ----------

# MAGIC %md
# MAGIC ###  CALCULA OS KPIs: TOTAL DE PCD (TOTAL DEMANDA), FLAG DE EMPRESAS QUE CUMPRIRAM A COTA DE PCD E DÉFICIT DE PCD PARA CUMPRIR A COTA

# COMMAND ----------

# CALCULA O TOTAL DE PCD (TOTAL DEMANDA), FLAG DE EMPRESAS QUE CUMPRIRAM A COTA DE PCD E DÉFICIT DE PCD PARA CUMPRIR A COTA

df_vinc_matriz_demanda = (df_vinc_matriz_faixa
           .withColumn("TOTAL_DEMANDA_RADAR", f.ceil(f.col("TOTAL_VINC_RADAR") * f.col("PERC_DESEJADO_RADAR")))
           .withColumn("TOTAL_DEMANDA", f.ceil(f.col("TOTAL_VINC") * f.col("PERC_DESEJADO")))
           .withColumn("FL_EMPRESA_COTA", f.when(f.col('TOTAL_VINC_PCD') >= f.col('TOTAL_DEMANDA'),f.lit(1)).otherwise(f.lit(0)))
           .withColumn("FL_EMPRESA_RADAR_COTA", f.when(f.col('TOTAL_VINC_RADAR_PCD') >= f.col('TOTAL_DEMANDA_RADAR'),f.lit(1)).otherwise(f.lit(0)))
           .withColumn("SALDO_COTA", (f.col('TOTAL_DEMANDA') - (f.col('TOTAL_VINC_PCD'))))
           .withColumn("SALDO_RADAR_COTA", (f.col('TOTAL_DEMANDA_RADAR') - (f.col('TOTAL_VINC_RADAR_PCD'))))
           .withColumn("DEFICIT_COTA", f.when(f.col('FL_EMPRESA_COTA') == 0, (f.col('TOTAL_DEMANDA') - (f.col('TOTAL_VINC_PCD')))).otherwise(f.lit(0)))
           .withColumn("DEFICIT_RADAR_COTA", f.when(f.col('FL_EMPRESA_RADAR_COTA') == 0, (f.col('TOTAL_DEMANDA_RADAR') - (f.col('TOTAL_VINC_RADAR_PCD')))).otherwise(f.lit(0)))
           )
         

# COMMAND ----------

# MAGIC %md
# MAGIC ## Junta o data set df_vinc_matriz_demanda com os dados da RFB

# COMMAND ----------

# junta base de PCD com a RFB e salva tabela por empresa
# # Realizar o join entre df_vinc_matriz_demanda e df_rfb_matriz
# df_vinc_matriz_demanda.count()

df_demanda_empresa = (df_vinc_matriz_demanda
                    .join(df_rfb_matriz,df_vinc_matriz_demanda.ID_CNPJ_RAIZ == df_rfb_matriz.CD_CNPJ_BASICO_RFB,"left")
                     )

# COMMAND ----------

destination_path = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['destination_1'])
adl_destination_path_1 = f'{var_adls_uri}{destination_path}'
print(adl_destination_path_1)

# COMMAND ----------

df_demanda_empresa = tcf.add_control_fields(df_demanda_empresa, adf, layer="biz")
df_demanda_empresa.write.format("delta").mode("overwrite").save(adl_destination_path_1)

# COMMAND ----------

# MAGIC %md
# MAGIC # PARTE 2 - Vínculos PCD por estabelecimento
# MAGIC - agregar por ESTABELECIMENTO (CNPJ completo) para calcular as distribuições de PCDs nas diferentes dimensões
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Calcula o total de vínculos por estabelecimento

# COMMAND ----------

df_vinc_estab = (df_vinculos
            .groupBy('ID_CNPJ_CEI','ID_CNPJ_RAIZ','ANO')
            .agg(f.count('ID_CPF').alias('TOTAL_TRAB_ESTB'),  
                 f.sum('TOTAL_VINC').alias('TOTAL_VINC_ESTB'),
                 f.sum('TOTAL_VINC_PCD').alias('TOTAL_VINC_ESTB_PCD'),
                 f.sum('TOTAL_VINC_RADAR').alias('TOTAL_VINC_RADAR_ESTB'),
                 f.sum('TOTAL_VINC_RADAR_PCD').alias('TOTAL_VINC_RADAR_ESTB_PCD'),
                 f.sum('TOTAL_APRENDIZ').alias('TOTAL_APRENDIZ_ESTB'),
                 f.sum('TOTAL_APRENDIZ_PCD').alias('TOTAL_APRENDIZ_ESTB_PCD')
                 )
                )
                    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Juntar base de estabelecimentos com base de empresas para pegar o total de vinculos 

# COMMAND ----------

# prepara o arquivo de empresas para fazer join 
df_demanda_empresa_aux = (df_demanda_empresa
                         .select(f.col('ID_CNPJ_RAIZ').alias('ID_CNPJ_RAIZ_aux'),
                                f.col('CD_CNAE20_DIV_RFB_MTZ').alias("CD_CNAE20_DIV_MATRIZ"),
                                f.col('FL_SIT_ATIVA_RFB_MTZ').alias('FL_SIT_ATIVA_MATRIZ'),
                                f.col('SG_UF_RFB_MTZ').alias('SG_UF_RFB_MATRIZ'),
                                f.col('CD_UF_RFB_MTZ').alias('CD_UF_MATRIZ'),
                                f.col('CD_MUN_IBGE_MTZ').alias('CD_MUN_MATRIZ'),
                                f.col('NM_MUN_IBGE_MTZ').alias('NM_MUN_MATRIZ'),
                                f.col('SG_UF_IBGE_MTZ').alias('SG_UF_IBGE_MATRIZ'),
                                f.col('TOTAL_VINC').alias('TOTAL_VINC_MATRIZ'),
                                f.col('TOTAL_VINC_PCD').alias('TOTAL_VINC_PCD_MATRIZ'),
                                f.col('TOTAL_VINC_RADAR').alias('TOTAL_VINC_RADAR_MATRIZ'),
                                f.col('TOTAL_VINC_RADAR_PCD').alias('TOTAL_VINC_RADAR_PCD_MATRIZ'),
                                f.col('TAMANHO_AGREG').alias('TAMANHO_AGREG_MATRIZ'),
                                f.col('TAMANHO_AGREG_RADAR').alias('TAMANHO_AGREG_RADAR_MATRIZ'),
                                )
                         )


# COMMAND ----------

df_vinc_estab_aux = (df_vinc_estab
                 .join(df_demanda_empresa_aux,(df_vinc_estab.ID_CNPJ_RAIZ == df_demanda_empresa_aux.ID_CNPJ_RAIZ_aux),"left")
                                  .drop('ID_CNPJ_RAIZ_aux')
                                  )
                     

# COMMAND ----------

df_demanda_estab = (df_vinc_estab_aux.join(df_rfb_estb,(df_vinc_estab_aux.ID_CNPJ_CEI == df_rfb_estb.ID_CNPJ_CEI_AUX),"left")
                                  .drop('ID_CNPJ_CEI_AUX')
                                  )

                     

# COMMAND ----------

destination_path = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['destination_2'])
adl_destination_path_2 = f'{var_adls_uri}{destination_path}'
print(adl_destination_path_2)

# COMMAND ----------

df = tcf.add_control_fields(df_demanda_estab, adf, layer="biz")
df.write.format("delta").mode("overwrite").save(adl_destination_path_2)
#final_df.write.mode('overwrite').parquet(adl_destination_path, compression='snappy')

# COMMAND ----------

