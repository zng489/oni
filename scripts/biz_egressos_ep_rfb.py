# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### EMPREENDEDORISMO - EGRESSOS DO SENAI
# MAGIC
# MAGIC ## Fontes de dados
# MAGIC - Base de empregabilidade dos egresos (Produção + RAIS), que alimenta o paineld e empregabilidade
# MAGIC - Bases da Receita Federal (RFB) - 12/2024

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window

var_adls_uri = 'abfss://datalake@cnibigdatadlsgen2.dfs.core.windows.net'

# COMMAND ----------

# Ano e mes de referência
ano = 2024
mes = 12

# COMMAND ----------

# MAGIC %md
# MAGIC # Fontes de dados

# COMMAND ----------

# Tabelas auxiliares
tec_path = '{uri}/trs/oni/ocde/ativ_econ/int_tec/class_inten_tec'.format(uri = var_adls_uri)
cnae_path = '{uri}/biz/oni/mapa_do_trabalho/mapa_trabalho_2024/bases_referencia/cnae/cnae_20/cnae_divisao'.format(uri = var_adls_uri)
munic_path = '{uri}/biz/oni/mapa_do_trabalho/mapa_trabalho_2024/bases_referencia/municipios'.format(uri = var_adls_uri)

## bases RFB
source_rfb_socios = (f"{var_adls_uri}/trs/oni/rfb_cnpj/cadastro_socio_f/ANO_ARQUIVO={ano}/MES_ARQUIVO={mes}")
source_rfb_empresas = (f"{var_adls_uri}/trs/oni/rfb_cnpj/cadastro_empresa_f/ANO_ARQUIVO={ano}/MES_ARQUIVO={mes}")
source_rfb_estbl = (f"{var_adls_uri}/trs/oni/rfb_cnpj/cadastro_estbl_f/ANO_ARQUIVO={ano}/MES_ARQUIVO={mes}")
source_nat_jur = (f"{var_adls_uri}/trs/oni/rfb_cnpj/tabelas_auxiliares_f/nat_juridica/ANO_ARQUIVO={ano}/MES_ARQUIVO={mes}")
#source_rfb_simples = (f"{var_adls_uri}/trs/oni/rfb_cnpj/cadastro_simples_f/ANO_ARQUIVO={ano}/MES_ARQUIVO={mes}")

# Egressos - base de empregabilidade na RAIS já tratada, com
source_egressos = '{adl_path}/uds/oni/observatorio_nacional/egressos_senai_ep/biz_senai_ep_producao_rais/'.format(adl_path=var_adls_uri)

#Base estabelecimento identificada
estab_path = '{uri}/trs/oni/mte/rais/identificada/rais_estabelecimento'.format(uri=var_adls_uri)

# COMMAND ----------

## Intensidade tecnologica
tec_bruto = spark.read.parquet(tec_path) 
## CNAE
cnae_bruto = spark.read.parquet(cnae_path)
## MUNIC
munic_bruto = spark.read.parquet(munic_path)

# COMMAND ----------

## Empresas
df_empresas_bruto = spark.read.parquet(source_rfb_empresas)
## Socios
df_socios_bruto = spark.read.parquet(source_rfb_socios)
## Estabelecimentos
df_estbl_bruto = spark.read.parquet(source_rfb_estbl)
## Receita Federal - nat juridica
nat_jur_bruto = spark.read.parquet(source_nat_jur)


# COMMAND ----------

## Painel de empregabilidade de egressos
df_egressos_bruto = spark.read.parquet(source_egressos)

# COMMAND ----------

## Rais estabelecimento
df_establ_rais = spark.read.parquet(estab_path)

# COMMAND ----------

# MAGIC %md
# MAGIC # Tratamento - Bases auxiliares

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

#egressos_rfb.write.parquet(var_adls_uri+'/uds/oni/observatorio_nacional/egressos_senai_ep/biz_senai_ep_producao_rfb', mode='overwrite')

egressos_rfb3.write.parquet(var_adls_uri+'/uds/oni/observatorio_nacional/egressos_senai_ep/biz_senai_ep_rfb', mode='overwrite')

# COMMAND ----------

display(egressos_rfb3.where(f.col('DS_SIT_CADASTRAL')=='Ativa').groupBy('CD_PORTE_EMPRESA_RAIS','DS_PORTE_EMPRESA_RAIS').count())

# COMMAND ----------

display(egressos_rfb3.where(f.col('DS_SIT_CADASTRAL')=='Ativa').groupBy('CD_PORTE_EMPRESA_RAIS','DS_PORTE_EMPRESA_RAIS').count())