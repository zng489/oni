# Databricks notebook source
# --- Step 1: Define widgets ---
dbutils.widgets.text("user_parameters", '{"null": "null"}')
dbutils.widgets.text("env", 'dev')
dbutils.widgets.text("storage", '{"url": "https://cnibigdatadlsgen2.dfs.core.windows.net", "container": "datalake"}')

# --- Step 3: Define dls structure (before using it) ---
dls = {
    "folders": {
        "landing": "/tmp/dev/lnd",
        "error": "/tmp/dev/err",
        "archive": "/tmp/dev/ach",
        "staging": "/tmp/dev/stg",
        "log": "/tmp/dev/log",
        "raw": "/tmp/dev/raw",
        "trusted": "/tmp/dev/trs",
        "business": "/tmp/dev/biz",
        "prm": "/tmp/dev/prm",
        "historico": "/tmp/dev/hst",
        "gov": "/tmp/dev/gov"
    },
    "path_prefix": "tmp",
    "uld": {
        "folders": {
            "landing": "/tmp/dev/uld",
            "error": "/tmp/dev/err",
            "staging": "/tmp/dev/stg",
            "log": "/tmp/dev/log",
            "raw": "/tmp/dev/raw",
            "archive": "/tmp/dev/ach"
        },
        "systems": {"raw": "usr"},
        "path_prefix": "/tmp/dev/"
    },
    "systems": {"raw": "usr"}
}

# --- Step 4: Define tables ---
tables = {
    'schema': '',           # Now filled
    'table': '', # Now filled

    'ano': '2024',
    'mes': '12',

    'trusted_path_1': '/oni/ocde/ativ_econ/int_tec/class_inten_tec/',
    'trusted_path_2': '/oni/rfb_cnpj/cadastro_socio_f/ANO_ARQUIVO={ano}/MES_ARQUIVO={mes}',
    'trusted_path_3': '/oni/rfb_cnpj/cadastro_empresa_f/ANO_ARQUIVO={ano}/MES_ARQUIVO={mes}',
    'trusted_path_4': '/oni/rfb_cnpj/cadastro_estbl_f/ANO_ARQUIVO={ano}/MES_ARQUIVO={mes}',
    'trusted_path_5': '/oni/rfb_cnpj/tabelas_auxiliares_f/nat_juridica/ANO_ARQUIVO={ano}/MES_ARQUIVO={mes}',
    'trusted_path_6': '/oni/mte/rais/identificada/rais_estabelecimento/',

    'business_path_1': '/oni/mapa_do_trabalho/mapa_trabalho_2024/bases_referencia/cnae/cnae_20/cnae_divisao/',
    'business_path_2': '/oni/mapa_do_trabalho/mapa_trabalho_2024/bases_referencia/municipios/',
    'business_path_3': '/oni/painel_egressos_senai/senai_ep_producao_rais/',

    'destination_1': '/oni/painel_egressos_senai/egresso_ep_rfb/',

    'databricks': {
        'notebook': '/biz/oni/observatorio_nacional/egressos_senai_ep/trs_biz_egresso_ep_rfb/'
    },

    'prm_path': ''
}

# --- Optional: ADF metadata ---
adf = {
    "adf_factory_name": "cnibigdatafactory",
    "adf_pipeline_name": "raw_trs_tb_email",
    "adf_pipeline_run_id": "61fc4f3c-c592-426d-bb36-c85cb184bb82",
    "adf_trigger_id": "92abb4ec-2b1f-44e0-8245-7bc165f91016",
    "adf_trigger_name": "92abb4ec-2b1f-44e0-8245-7bc165f91016",
    "adf_trigger_time": "2024-05-07T00:58:48.0960873Z",
    "adf_trigger_type": "PipelineActivity"
}

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
from pyspark.sql.window import Window
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
adl_trusted_2 = adl_trusted_2.format(ano=tables['ano'], mes=tables['mes'])
print(adl_trusted_2)



trusted_path_3 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_3'])
print(trusted_path_3)

adl_trusted_3 = f'{var_adls_uri}{trusted_path_3}'
adl_trusted_3 = adl_trusted_3.format(ano=tables['ano'], mes=tables['mes'])
print(adl_trusted_3)




trusted_path_4 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_4'])
print(trusted_path_4)

adl_trusted_4 = f'{var_adls_uri}{trusted_path_4}'
adl_trusted_4 = adl_trusted_4.format(ano=tables['ano'], mes=tables['mes'])
print(adl_trusted_4)




trusted_path_5 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_5'])
print(trusted_path_5)

adl_trusted_5 = f'{var_adls_uri}{trusted_path_5}'
adl_trusted_5 = adl_trusted_5.format(ano=tables['ano'], mes=tables['mes'])
print(adl_trusted_5)




trusted_path_6 = "{trusted}{schema}{table}{trusted_path}".format(trusted=dls['folders']['trusted'], schema=tables['schema'], table=tables['table'], trusted_path=tables['trusted_path_6'])
print(trusted_path_6)

adl_trusted_6 = f'{var_adls_uri}{trusted_path_6}'
adl_trusted_6 = adl_trusted_6.format(ano=tables['ano'], mes=tables['mes'])
print(adl_trusted_6)

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

# COMMAND ----------

# Deleta os arquivos e diretórios no caminho especificado no Data Lake Storage Gen2 em DESENVOLVIMENTO
dbutils.fs.rm('abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/mapa_do_trabalho/mapa_trabalho_2024/bases_referencia/municipios/', recurse=True)

# COMMAND ----------

# Copia os arquivos e diretórios do caminho especificado para o destino
source_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/biz/oni/mapa_do_trabalho/mapa_trabalho_2024/bases_referencia/municipios/'
destination_path = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net/tmp/dev/biz/oni/mapa_do_trabalho/mapa_trabalho_2024/bases_referencia/municipios/'

# Copia os arquivos e diretórios
dbutils.fs.cp(source_path, destination_path, recurse=True)

# COMMAND ----------

destination_path = "{business}{schema}{table}{business_path}".format(business=business, schema=tables['schema'], table=tables['table'], business_path=tables['destination_1'])
adl_destination_path_1 = f'{var_adls_uri}{destination_path}'
print(adl_destination_path_1)

# COMMAND ----------

## Intensidade tecnologica
tec_bruto = spark.read.parquet(adl_trusted_1) 
## CNAE
cnae_bruto = spark.read.parquet(adl_business_1)
## MUNIC
munic_bruto = spark.read.parquet(adl_business_2)

# COMMAND ----------

## Empresas
df_empresas_bruto = spark.read.parquet(adl_trusted_3)
## Socios
df_socios_bruto = spark.read.parquet(adl_trusted_2)
## Estabelecimentos
df_estbl_bruto = spark.read.parquet(adl_trusted_4)
## Receita Federal - nat juridica
nat_jur_bruto = spark.read.parquet(adl_trusted_5)


# COMMAND ----------

## Painel de empregabilidade de egressos
df_egressos_bruto = spark.read.parquet(adl_business_3)

# COMMAND ----------

## Rais estabelecimento
df_establ_rais = spark.read.parquet(adl_trusted_6)

# COMMAND ----------

ano = 2024
mes = 12

# COMMAND ----------

# COMMAND ----------

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

uf =  (munic_bruto
  .select(f.col('nm_regiao').alias('NM_REGIAO'),
          f.col('nm_uf').alias('DS_UF'),
          f.col('cd_uf').alias('CD_UF'),
          f.col('sg_uf').alias('SG_UF'))
  .distinct())

nat_jur = (nat_jur_bruto
           .select(f.col('CD_NAT_JURIDICA').alias('CD_NATUREZA_JURIDICA'),f.col('DS_NAT_JURIDICA').alias('DS_NATUREZA_JURIDICA')))

# COMMAND ----------

# MAGIC %md
# MAGIC # Tratamento - Bases RFB

# COMMAND ----------

df_empresas_bruto = df_empresas_bruto.join(nat_jur, on = 'CD_NATUREZA_JURIDICA', how = 'left')

# COMMAND ----------


# Acrescentei: idade da empresa, considerando o estabelecimento mais antigo
# Já estou considerando apenas as ativas em 2024

window = Window.partitionBy('CD_CNPJ_BASICO')

df_empresas  = df_empresas_bruto.select('CD_CNPJ_BASICO', 'DS_PORTE_EMPRESA','DS_NATUREZA_JURIDICA').distinct()

df_estbl = (df_estbl_bruto
  .withColumn('ANO_INICIO', f.year('DT_INICIO_ATIV'))
  .withColumn('ANO_INICIO_EMPRESA', f.min('ANO_INICIO').over(window))
  .withColumn('VL_IDADE_EMPRESA',  ano - f.col('ANO_INICIO_EMPRESA'))
  .withColumn('CD_CNAE20_DIVISAO', f.substring(f.col('CD_CNAE20_SUBCLASSE_PRINCIPAL'), 0, 2))
  .where(f.col('CD_MATRIZ_FILIAL') == 1)
  .select('CD_CNPJ_BASICO', 
      'CD_CNAE20_DIVISAO',
      'SG_UF',
      'DS_SIT_CADASTRAL',
      'ANO_INICIO_EMPRESA',
      'VL_IDADE_EMPRESA',
  )    
  .distinct()
  )
  # Atenção: a informação de setor é a do estabelecimento que é a matriz

df_empresas_enriquecido = (df_empresas
                             .join(df_estbl, on='CD_CNPJ_BASICO', how='left')
                             .distinct() 
                            )

# COMMAND ----------

accented_chars = "áàäâãéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ"
unaccented_chars = "aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN"

df_socios = (df_socios_bruto
  .withColumn('ANO_ENTRADA_SOCIEDADE',f.year(f.to_date(f.col('DT_ENTRADA_SOCIEDADE'), 'yyyy')))
  .withColumn('PRIMEIRO_NOME',
              f.concat(f.split(f.col("NM_SOCIO_RAZAO_SOCIAL"), " ").getItem(0),
                       f.split(f.col("NM_SOCIO_RAZAO_SOCIAL"), " ").getItem(1)
                      ))
  .withColumn("PRIMEIRO_NOME", f.translate(f.col("PRIMEIRO_NOME"), accented_chars, unaccented_chars))
  .withColumn("PRIMEIRO_NOME", f.lower(f.col("PRIMEIRO_NOME"))) 
  .withColumn('PARTE_CPF', f.substring(f.col('CD_CNPJ_CPF_SOCIO'), 4, 6))
  .select(
  'CD_CNPJ_BASICO', 
  'CD_CNPJ_CPF_SOCIO',
  'PARTE_CPF', 
  'PRIMEIRO_NOME', 
  'NM_SOCIO_RAZAO_SOCIAL',
  'ANO_ENTRADA_SOCIEDADE',
  'DT_ENTRADA_SOCIEDADE'
  )
  .join(df_empresas_enriquecido, on='CD_CNPJ_BASICO', how='left')
  .join(uf, on = 'SG_UF', how = 'left')
  .join(cnae, on = 'CD_CNAE20_DIVISAO', how = 'left')
)

# IMPORTANTE: uma mesma pessoa pode ter sociedade em mais de um empreendimento. Para resolver isso, consoderei somente o mais antigo 

# COMMAND ----------

# Seleciona uma sociedade por pessoa

df_socios_unico = (df_socios
                   .withColumn('empresa_ativa', f.when(f.col('DS_SIT_CADASTRAL') == 'Ativa',f.lit(1)).otherwise(f.lit(0)))
                   .orderBy('PARTE_CPF', 
  'PRIMEIRO_NOME', 
  'NM_SOCIO_RAZAO_SOCIAL',
  f.desc('empresa_ativa'),
  'DT_ENTRADA_SOCIEDADE',
  'VL_IDADE_EMPRESA')
  .dropDuplicates(['PARTE_CPF', 
  'PRIMEIRO_NOME', 
  'NM_SOCIO_RAZAO_SOCIAL'])
  .drop('DT_ENTRADA_SOCIEDADE','empresa_ativa')
  )


# COMMAND ----------

# MAGIC %md
# MAGIC # Tratamento - Base RAIS

# COMMAND ----------

socios_rais = (
    df_establ_rais
    .where(f.col('NR_ANO') == 2024)
    .withColumn('CD_CNPJ_BASICO', f.substring(f.col('ID_CNPJ_CEI'), 1, 8))
    .groupBy('CD_CNPJ_BASICO')
    .agg(
        f.sum(f.col('NR_QTD_VINCULOS_ATIVOS')).alias('VL_EMPREGADOS_RAIS')
    )
    .join(df_socios_unico,on='CD_CNPJ_BASICO', how='right')
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tratamento - Base egressos + RFB

# COMMAND ----------

var =["CPF_ALUNO",
      "NOME_ALUNO",
"DS_SEXO",
"VL_IDADE",
"DS_FAIXA_ETARIA",
"DS_RACA_COR_ALUNO",
"DS_PCD",
"DR",
"UNIDADE_ATENDIMENTO",
"CATEGORIA",
"MODALIDADE",
"AREA_ATUACAO",
"ANO_CONCLUSAO_CURSO",
"DS_GRATUIDADE",
"CD_TIPO_FINANCIAMENTO",
"DS_TIPO_FINANCIAMENTO",
"CD_MES_FECHAMENTO",
"DT_ENTRADA",
"DT_SAIDA",
"DR_SIGLA",
"CD_MODALIDADE",
"DS_MODALIDADE",
"CD_CATEGORIA",
"DS_CATEGORIA",
"CD_AREA_ATUACAO",
"DS_AREA_ATUACAO",
"CD_UNIDADE_ATENDIMENTO",
"DS_UNIDADE_ATENDIMENTO",
"CD_TIPO_ACAO",
"DS_TIPO_ACAO",
"DS_MUNICIPIO_ACAO",
"CD_MUNICIPIO_ACAO",
"CD_EMPREGO_INDUSTRIAL",
"DS_EMPREGO_INDUSTRIAL",
"ANOS_POS_CONCLUSAO",
"VL_EMPREGADOS_ANO",
"VL_EMPREGADOS_ANO_INDUSTRIA",
"VL_EMPREGADOS_ANO_OUTROS",
"VL_SEM_EMPREGO_ANO",
"VL_TOTAL_EGRESSOS",
]

df_egressos_bruto = df_egressos_bruto.select(var).where(f.col('ANO_RAIS') == ano)

# COMMAND ----------

accented_chars = "áàäâãéèëêíìïîóòöôõúùüûçñÁÀÄÂÃÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÇÑ"
unaccented_chars = "aaaaaeeeeiiiiooooouuuucnAAAAAEEEEIIIIOOOOOUUUUCN"

df_egressos = (df_egressos_bruto
  .withColumn('PRIMEIRO_NOME',
              f.concat(f.split(f.col("NOME_ALUNO"), " ").getItem(0),
                       f.split(f.col("NOME_ALUNO"), " ").getItem(1)
                      ))
  .withColumn("PRIMEIRO_NOME", f.translate(f.col("PRIMEIRO_NOME"), accented_chars, unaccented_chars))
  .withColumn("PRIMEIRO_NOME", f.lower(f.col("PRIMEIRO_NOME")))
  .withColumn('PARTE_CPF', f.substring(f.col('CPF_ALUNO'), 4, 6))
  )


# COMMAND ----------

egressos_rfb = (df_egressos
                .join(socios_rais,on=['PARTE_CPF', 'PRIMEIRO_NOME'],how='left')
                )

# COMMAND ----------

# Step 1: Aggregate to get the count of distinct NM_SOCIO_RAZAO_SOCIAL per group
homonimos_df = (
    egressos_rfb
    .select("PARTE_CPF", 'PRIMEIRO_NOME', 'NM_SOCIO_RAZAO_SOCIAL')
    .distinct()
    .groupBy('PARTE_CPF', 'PRIMEIRO_NOME')
    .agg(f.count('NM_SOCIO_RAZAO_SOCIAL').alias('homonimos'))
)

# Step 2: Join back to original DataFrame to filter
egressos_rfb2 = (
    egressos_rfb
    .join(
        homonimos_df,
        on=['PARTE_CPF', 'PRIMEIRO_NOME'],
        how='left'
    )
    .where((f.col('homonimos') < 2) | (f.col('homonimos').isNull()))
    .drop('homonimos')
    .distinct()
)

# COMMAND ----------

egressos_rfb3 = (egressos_rfb2
                 .withColumn('ANO_SAIDA_CURSO',f.year(f.to_date(f.col('DT_SAIDA'), 'dd/MM/yyyy')))
                 .withColumn('ANO_ENTRADA_CURSO',f.year(f.to_date(f.col('DT_ENTRADA'), 'dd/MM/yyyy')))
                 .withColumn('VL_TOTAL_SOCIOS',f.when(f.col('ANO_ENTRADA_SOCIEDADE').isNotNull(),1).otherwise(0))
                 .withColumn('CD_PORTE_EMPRESA_RAIS',
                             f.when((f.col('VL_EMPREGADOS_RAIS') == 0) | 
                                    ((f.col('VL_EMPREGADOS_RAIS').isNull()) & (f.col('VL_TOTAL_SOCIOS')==f.lit(1))), 0)
                              .when(f.col('VL_EMPREGADOS_RAIS')== 1, 1)
                              .when(f.col('VL_EMPREGADOS_RAIS').between(2, 5), 2)
                              .when(f.col('VL_EMPREGADOS_RAIS').between(6, 10), 3)
                              .when(f.col('VL_EMPREGADOS_RAIS') > 10, 4)
                              .otherwise(None)
                            )
                .withColumn('DS_PORTE_EMPRESA_RAIS',
                            f.when((f.col('VL_EMPREGADOS_RAIS') == 0) | 
                                   ((f.col('VL_EMPREGADOS_RAIS').isNull()) & (f.col('VL_TOTAL_SOCIOS')==f.lit(1))), 'Sem empregados')
                            .when(f.col('VL_EMPREGADOS_RAIS')== 1, "1 empregado")
                            .when(f.col('VL_EMPREGADOS_RAIS').between(2, 5), '2 a 5 empregados')
                            .when(f.col('VL_EMPREGADOS_RAIS').between(6, 10), '6 a 10 empregados')
                            .when(f.col('VL_EMPREGADOS_RAIS') > 10, 'Acima de 10 empregados')
                            .otherwise(None)
                            )
                 .withColumn('CD_ENTRADA_SOCIEDADE',f.when((f.col('ANO_ENTRADA_SOCIEDADE')) > (f.col('ANO_SAIDA_CURSO')),2)
                                                 .when(((f.col('ANO_ENTRADA_SOCIEDADE')) >= (f.col('ANO_ENTRADA_CURSO'))) &
                                                       ((f.col('ANO_ENTRADA_SOCIEDADE')) <= (f.col('ANO_SAIDA_CURSO'))),1)
                                                 .when((f.col('ANO_ENTRADA_SOCIEDADE')) < (f.col('ANO_SAIDA_CURSO')),0))
                 .withColumn('DS_ENTRADA_SOCIEDADE',f.when((f.col('ANO_ENTRADA_SOCIEDADE')) > (f.col('ANO_SAIDA_CURSO')),'Depois')
                                                 .when(((f.col('ANO_ENTRADA_SOCIEDADE')) >= (f.col('ANO_ENTRADA_CURSO'))) &
                                                       ((f.col('ANO_ENTRADA_SOCIEDADE')) <= (f.col('ANO_SAIDA_CURSO'))),'Durante')
                                                 .when((f.col('ANO_ENTRADA_SOCIEDADE')) < (f.col('ANO_SAIDA_CURSO')),'Antes'))
                 .withColumn('CD_FUNDACAO_EMPRESA',f.when((f.col('ANO_INICIO_EMPRESA')) >= (f.col('ANO_ENTRADA_SOCIEDADE')),1)
                                                 .when((f.col('ANO_INICIO_EMPRESA')) < (f.col('ANO_ENTRADA_SOCIEDADE')),0))
                 .withColumn('DS_FUNDACAO_EMPRESA',f.when((f.col('ANO_INICIO_EMPRESA')) >= (f.col('ANO_ENTRADA_SOCIEDADE')),'Fundou empresa')
                                                 .when((f.col('ANO_INICIO_EMPRESA')) < (f.col('ANO_ENTRADA_SOCIEDADE')),'Empresa já existia'))            
            )


# COMMAND ----------

df = tcf.add_control_fields(egressos_rfb3, adf, layer="biz")

# COMMAND ----------

adl_destination_path_1

# COMMAND ----------

df.write.mode("overwrite").parquet(adl_destination_path_1)
#df.write.format("delta").mode("overwrite").save(adl_destination_path_1)

# COMMAND ----------

display(egressos_rfb3.where(f.col('DS_SIT_CADASTRAL')=='Ativa').groupBy('CD_PORTE_EMPRESA_RAIS','DS_PORTE_EMPRESA_RAIS').count())

# COMMAND ----------

display(egressos_rfb3.where(f.col('DS_SIT_CADASTRAL')=='Ativa').groupBy('CD_PORTE_EMPRESA_RAIS','DS_PORTE_EMPRESA_RAIS').count())

# COMMAND ----------

