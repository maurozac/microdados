"""Baixa e estrutura microdados da PNAD Contínua em dataframes."""

__version__ = "1.0"
__author__ = "Mauro Zackiewicz" 
__email__ = "maurozac@gmail.com"
__license__ = "New BSD License"

import os
from pathlib import Path
from ftplib import FTP
import zipfile
import json

# estes módulos precisam ser instalados
# pip install wget
import wget
# pip install pandas
import pandas as pd


# passo 1: criar diretório local para receber os dados
# ====================================================

home = os.path.expanduser('~')
destino = home + "/dados/PNADc/"
Path(destino).mkdir(parents=True, exist_ok=True)
temp = destino + "temp/"
Path(temp).mkdir(parents=True, exist_ok=True)


# passo 2: acessar FTP do IBGE, recolher nomes dos arquivos e baixar para o diretório /temp
# =========================================================================================

ano = '2020'    # escolher o ano
trimestre = 4   # escolher o trimeste

fonte = "ftp.ibge.gov.br"
diretorio = "/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/"

with FTP(fonte) as ftp:
    ftp.login()
    ftp.cwd(diretorio)
    ftp.cwd(ano)
    arquivos = ftp.nlst()

arquivo = arquivos[trimestre-1]
# baixar
url = 'https://'+fonte+diretorio+ano+'/'+arquivo
filename = wget.download(url, out=temp)
# expandir
zipado = zipfile.ZipFile(temp+arquivo)
nomes = zipado.namelist()
alvo = [n for n in nomes if n[-4:]=='.txt'][0]
txt = zipado.extract(alvo, path=temp)
# apagar arquivo zip
os.remove(temp+arquivo)  


# passo 3: aplicar o parser
# =========================

""" Os aquivos txt descompactados da PNAD são muito grandes e podem gerar overflow
na memória se carregados diretamente para um dataframe. Para evitar isso, aplicamos
o parser diretamente no loop de leitura do arquivo txt e criamos dataframes menores
para o processamento posterior.
"""

# o parser usa o dicionário de campos preparado previamente
# este arquivo deve estar no mesmo diretório do script
with open('campos.json') as f:
    campos = json.load(f)


def parser_pnadc(linha, campos):
    """ Cada linha do arquivo de microdados PNAD corresponde a uma unidade amostral.
    Cada linha deve ser quebrada nos pontos certos para povoar os campos das variáveis.
    """
    caso = []
    for c in campos:
        i = c['inicio']
        f = c['inicio']+c['largura']
        valor = linha[i:f].strip()
        if c['tipo'] == 'cat':
            valor = str(valor)
        if c['tipo'] == 'num':
            if valor.find('.') > -1:
                valor = float(valor)
            else:
                if valor:
                    valor = int(valor)
                else:
                    valor = 0
        label = c['nome']
        caso.append(valor)
    return caso


campos = campos[:-200] # exclui Pesos Replicados para diminuir tamanho (ver docs da PNAD)
variaveis = [x['nome'] for x in campos]  # 220 variaveis

nome = arquivo.split('.')[0][:12]
nome = nome + '_'
with open(txt) as f:
    to_df = []
    c = 0   # controle
    n = 0   # contador de arquivos
    m = 0   # contador de linhas
    for linha in f:
        caso = parser_pnadc(linha, campos)
        to_df.append(caso)
        c += 1
        m += 1
        if c >= 100 * 1000: # cada dataframe terá 100 mil linhas
            df = pd.DataFrame(to_df, columns=variaveis)
            df.to_csv(temp + nome + str(n) + '.csv')
            print(n, m, 'pronto')
            to_df = []
            c = 0
            n += 1
    df = pd.DataFrame(to_df, columns=variaveis)  # exceto este que fica com o residual
    df.to_csv(temp + nome + str(n) + '.csv')
    print(n, m, 'pronto')

# apagar txt
os.remove(txt)
