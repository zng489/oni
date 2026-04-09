# Databricks notebook source
import os
import requests, zipfile
import shutil
import pandas as pd
import glob
import subprocess
from threading import Timer
import shlex
import logging
import json
from core.bot import log_status
from core.adls import upload_file
import re
import pyspark.sql.functions as f
import concurrent.futures
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from core.string_utils import normalize_bool
from core.string_utils import normalize_replace

# COMMAND ----------

params = json.loads(re.sub("\'", '\"', dbutils.widgets.get("params")))
dls = json.loads(dbutils.widgets.get("dls").replace("\'",'\"'))
adf = json.loads(dbutils.widgets.get("adf").replace("\'",'\"'))

# COMMAND ----------

def calculate_chunk_size(file_size: int):
    """
    Define um chunk_size adequado para download multipart baseado no tamanho do arquivo.
    
    Parâmetros:
    - file_size (int): Tamanho do arquivo em bytes.

    Retorno:
    - int | None: chunk_size em bytes ou None se não precisar de chunking
    """
    if file_size < 100 * 1024**2:  # Menor que 100MB
        return None  # Baixar o arquivo inteiro
    elif file_size < 1 * 1024**3:  # Entre 100MB e 1GB
        return 16 * 1024**2  # 16MB
    elif file_size < 3 * 1024**3:  # Entre 1GB e 3GB
        return 32 * 1024**2  # 32MB
    else:  # Maior que 3GB
        return min(64 * 1024**2, file_size // 100)  # 64MB ou 1% do tamanho


def __download_file(session: requests.Session, url_file: str, output_path: str) -> None:
    """
    Baixa um arquivo inteiro e salva no caminho especificado.

    Args:
        session (requests.Session): Sessão HTTP configurada.
        url_file (str): URL do arquivo para download.
        output_path (str): Caminho onde o arquivo será salvo.

    Raises:
        Exception: Se houver falha na requisição ou ao escrever o arquivo.
    """
    response = session.get(url_file, stream=True)
    if response.status_code != 200:
        raise Exception(f"Erro: Status code {response.status_code}. File: {output_path}")

    try:
        with open(output_path, "wb") as f:
            f.write(response.content)
    except Exception as err:
        raise Exception(f"Falha ao escrever arquivo {output_path}. \nErro: {err}")


def __download_part(
    session: requests.Session,
    url_file: str,
    header: dict,
    partfilename: str,
    chunk_size: int,
) -> int:
    """
    Baixa uma parte do arquivo usando Range Requests.

    Args:
        session (requests.Session): Sessão HTTP configurada.
        url_file (str): URL do arquivo.
        header (dict): Cabeçalhos HTTP para a requisição parcial.
        partfilename (str): Nome do arquivo parcial onde a parte será salva.
        chunk_size (int): Tamanho do chunk de cada requisição.

    Returns:
        int: Número total de bytes baixados.

    Raises:
        Exception: Se a resposta HTTP não retornar o código 206 (Partial Content).
    """
    response = session.get(url_file, headers=header, stream=True)
    if response.status_code != 206:
        raise Exception(
            f"Erro: Status code {response.status_code}. Part file: {partfilename}"
        )

    size = 0
    try:
        with open(partfilename, "wb") as f:
            for chunk in response.iter_content(chunk_size):
                if chunk:
                    size += f.write(chunk)
    except Exception as err:
        raise Exception(f"Falha ao escrever parte do arquivo {partfilename}. \nErro: {err}")

    return size


def __make_headers(start: int, chunk_size: int) -> dict:
    """
    Gera cabeçalhos HTTP para Range Requests.

    Args:
        start (int): Byte inicial da parte a ser baixada.
        chunk_size (int): Tamanho do chunk.

    Returns:
        dict: Cabeçalho HTTP contendo a range de bytes a ser baixada.
    """
    end = start + chunk_size - 1
    return {"Range": f"bytes={start}-{end}"}


def __download_multipart(url_file: str, output_path: str) -> None:
    """
    Realiza o download multipart de um arquivo, dividindo-o em partes para otimizar a velocidade.

    Args:
        url_file (str): URL do arquivo para download.
        output_path (str): Caminho onde o arquivo final será salvo.

    Raises:
        Exception: Se houver falha no download ou na escrita do arquivo.
    """
    
    # Configuração de retry para requisições HTTP
    # Tenta até 10 vezes em caso de erro (429, 500, 502, 503, 504) e espera 0.5 segundos para nova tentativa
    retry_strategy = Retry(
        total=10, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)

    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    # Faz uma requisição para obter o tamanho do arquivo
    response = session.head(url_file)
    if response.status_code != 200:
        raise Exception(f"Erro: Status code {response.status_code}.")

    file_size = int(response.headers.get("content-length", 0))
    if file_size == 0:
        raise Exception("Erro: file_size é zero.")

    chunk_size = calculate_chunk_size(file_size)

    # Decide entre fazer download em partes ou de uma vez.
    # Se o arquivo for maior que chunk_size, baixa em partes.
    # Se for menor, baixa normalmente com __download_file.
    if chunk_size:        
        print(f"File Size: {file_size / (1024**3):.2f} GB, Chunk Size: {chunk_size / (1024**2):.2f} MB")        

        # Cria uma lista de cabeçalhos HTTP (Range: bytes=start-end).
        # Define os nomes dos arquivos temporários adicionando (.part0, .part1, ...).
        chunks = range(0, file_size, chunk_size)
        headers_partfiles_list = [
            (__make_headers(chunk, chunk_size), f"{output_path}.part{i}")
            for i, chunk in enumerate(chunks)
        ]

        # max_workers define a quantidade de threads simultâneas para baixar os arquivos em paralelo
        # chama a função __download_part para baixar cada pedaço do arquivo
        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            futures = [
                executor.submit(
                    __download_part, session, url_file, header, partfilename, chunk_size
                )
                for header, partfilename in headers_partfiles_list
            ]

            # Aguarda todas as partes serem baixadas
            concurrent.futures.wait(futures)

        # Junta todas as partes baixadas (.part0, .part1, ...) 
        # Ao criar o arquivo definitivo paga os arquivos temporários
        try:
            with open(output_path, "wb") as outfile:
                for i in range(len(chunks)):
                    chunk_path = f"{output_path}.part{i}"
                    with open(chunk_path, "rb") as s:
                        outfile.write(s.read())
                    os.remove(chunk_path)  
        except Exception as err:
            raise Exception(f"Falha ao escrever arquivo {output_path}. \nErro: {err}")
    else:
        print(f"Arquivo pequeno ({file_size / (1024**2):.2f} MB), baixando sem chunking.")
        __download_file(session, url_file, output_path)

# COMMAND ----------

def __extract_all_zip(path, file_name) -> None:
    print(">>>>> Extract file")
    zipfile.ZipFile(path + file_name, "r").extractall(path)

# COMMAND ----------

try:
  url = "https://arquivos.receitafederal.gov.br/dados/cno/"
  file_name = "cno.zip"
  schema = "rfb_cno"
  table = "cadastro_nacional_de_obras"
  tmp = "/tmp/org_raw_rfb_cno/"
  response = requests.get(url)

  if 200 <= response.status_code <= 299:

    os.makedirs(tmp, mode=0o777, exist_ok=True)

    __download_multipart(
        url_file=f'{url}{file_name}',
        output_path=f'{tmp}{file_name}'
    )

    __extract_all_zip(tmp, file_name)

    print(f">>>>> Get list file in {tmp}")

    file_list = glob.glob(tmp + "/*.csv")

    file_list = list(map(lambda x: x.replace("\\", "/"), file_list))

    print(f">>>>> the size file_list is {len(file_list)}")

    for item in file_list:

        print(f">>>>> Transforme item {item}")
        name = item.split("/")[3].split(".")[0]

        print(f">>>>> Create dataframe in pandas {item}")

        # Lê o arquivo CSV inteiro
        # df = pd.read_csv(item, encoding="ISO-8859-1", dtype=str)
        df = pd.read_csv(item, sep=',', index_col=None, quotechar='"', encoding="ISO-8859-1", dtype=str)

        # Adiciona a coluna de data atual
        df["date"] = pd.to_datetime("today").strftime("%d/%m/%Y")
        
        dict_columns = {}
        for column in df.columns:
            dict_columns.update({column: normalize_replace(column.strip())})
        df = df.rename(columns=dict_columns)
        df = df.where(pd.notnull(df), None)
        df.astype(str)  # Força tudo para string
        df = spark.createDataFrame(df)

        print(">>>>> Iniciando upload do arquivo")
        upload_file(spark, dbutils, df, schema=schema, table=f'{table}/{name}', )  
            
        print(">>>>> End Upload file")       
    log_status("ok")

  else:
      log_status("error")

except Exception as e:
  raise(e)
finally:
   shutil.rmtree(tmp)
