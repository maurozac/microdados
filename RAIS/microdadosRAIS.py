#!/usr/bin/python

"""
Pipeline para coleta e organização de microdados da RAIS
ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/

PROJETO: Avaliação de Impactos da Infraestrura Aeroportuária | ITA & SAC

Os arquivos de microdados podem ser grandes, acima de 10 GB depois de descompactados.
Assim, este pipeline está organizado da seguinte forma:
. baixar localmente o arquivo .7z e descompactar em um diretório temporário
. limpar dados
. aplicar modelo de agregação de dados
. gerar arquivos finais e enviar para persistência (S3 ou dir local)
. apagar arquivos temporários
. processar próximo arquivo .7z

Código para Python 3

Algumas referências:
https://github.com/guilhermejacob/guilhermejacob.github.io/blob/master/scripts/mtps.R
https://github.com/rdahis/clean_RAIS
"""

__version__ = "1.1"
__author__ = "Mauro Zackiewicz"   # codigo
__email__ = "maurozac@gmail.com"
__copyright__ = "Copyright 2020"
__license__ = "New BSD License"
__status__ = "Experimental"


import json
import os

import numpy as np
import pandas as pd
import wget
import py7zr
import boto


UFs = [
    'AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS',
    'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC',
    'SP', 'SE', 'TO',
    ]
ANOS = ['2017', '2010']

# Estes PATHs são do meu ambiente => use os seus aqui
PATH_TEMP = "/Users/tapirus/Desktop/ITA/dados/RAIS/temp/"
PATH_UTIL = "/Users/tapirus/Desktop/ITA/dados/RAIS/util/"


def baixar_raw(uf, ano, path_temp):
    """Baixa e descompacta arquivo de microdados da RAIS
    Retorna o path para do arquivo pronto para ser processado.

    ATENÇÃO: há variação na estrutura dos paths no ftp da RAIS,
    verifique a compatibilidade ao rodar para anos diferentes
    dos que estão aqui.
    """
    url = "ftp://ftp.mtps.gov.br/pdet/microdados/RAIS/"
    url += ano + "/" + uf + ano + ".7z"

    filename = wget.download(url, out=path_temp)

    archive = py7zr.SevenZipFile(filename, mode='r')
    archive.extractall(path=path_temp)
    extraidos = archive.getnames()
    archive.close()
    if len(extraidos) != 1:
        print('[!] Pacote 7z fora do padrão:', uf, ano)
        return None, ano

    os.remove(filename)
    print('[♫] Baixado e descompactado:', uf, ano)

    return path_temp + extraidos[0], ano


# TESTE
path, ano = baixar_raw("AL", "2010", PATH_TEMP)


# DICT com definições para os tipos de dados
# nem todos foram incluidos pq não serão usados
CAMPOS_RAIS = {
    "2010": {
        # "CBO Ocupação 2002": str,
        "CNAE 2.0 Classe": str,
        # "CNAE 2.0 Subclasse": str,
        "Município": str,
        "Tamanho Estabelecimento": str,
        "Escolaridade após 2005": str,
        # "Faixa Etária": str,
        # "Raça Cor": str,
        # "Sexo Trabalhador": str,
        "Vl Remun Dezembro Nom": np.float64,
        "Vl Remun Média Nom": np.float64,
    },
    "2017": {
        # "CBO Ocupação 2002": str,
        "CNAE 2.0 Classe": str,
        # "CNAE 2.0 Subclasse": str,
        "Município": str,
        "Tamanho Estabelecimento": str,
        "Escolaridade após 2005": str,
        # "Faixa Etária": str,
        # "Raça Cor": str,
        # "Sexo Trabalhador": str,
        "Vl Rem Janeiro CC": np.float64,
        "Vl Rem Fevereiro CC": np.float64,
        "Vl Rem Março CC": np.float64,
        "Vl Rem Abril CC": np.float64,
        "Vl Rem Maio CC": np.float64,
        "Vl Rem Junho CC": np.float64,
        "Vl Rem Julho CC": np.float64,
        "Vl Rem Agosto CC": np.float64,
        "Vl Rem Setembro CC": np.float64,
        "Vl Rem Outubro CC": np.float64,
        "Vl Rem Novembro CC": np.float64,
        "Vl Remun Dezembro Nom": np.float64,
        "Vl Remun Média Nom": np.float64,
    }
}


def carregar_dados(path, ano, campos):
    """Importa arquivo .txt RAIS => na verdade um csv jabuticaba.
    Força consistência dos tipos de dados e elimina colunas que não serão usadas.
    Retorna DataFrame pronto para o uso.

    OBS: diferentes anos possuem diferentes campos de dados, certifique-se de
    fornecer o dict de campos correto.

    OBS2: comentar no dict de campos aqueles que não serão usados (isso diminui
    o impacto no processamento)
    """
    frame = pd.read_csv(
                path,
                encoding="ISO-8859-1",
                decimal=",",
                sep=";",
                dtype=campos[ano]
            )

    retirar = [_ for _ in frame.keys() if _ not in campos[ano].keys()]

    return frame.drop(columns=retirar)


# TESTE
df = carregar_dados(path, ano, CAMPOS_RAIS)
df.shape



