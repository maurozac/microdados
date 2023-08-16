#!/usr/bin/python
# coding=utf8
"""
 +------------+
 |  Deflator  |
 +------------+

from deflata import IPCA

d = IPCA()

# funciona nos dois sentido no tempo
# anos como int ou str => assume valor do mês de dezembro
d.deflacionar(500.0, 2012, 2018) # 500 em 2012 equivaleria a 707,93 em 2018
d.deflacionar(500.0, '2018', '2012')  # 500 em 2018 equivaleria a 353,14 em 2018

# mas pode usar com mes especifico
import datetime
d.deflacionar(500.0, datetime.datetime(2022,10,23), '2001') 

# salario Fapesp
d.deflacionar(11000, 2006, 2021)


"""
import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

"""
Preços: Índice de Preços ao Consumidor Amplo (IPCA) geral com índice base (dez. 1993 = 100)
http://www.ipeadata.gov.br/ExibeSerie.aspx?serid=36482

"""

def importar_dados_ipea(serie='36482'):
    ''' Scrapper para Series de Dados IPEA 
    FAZER limpeza para ajustar ao formato de dados ao usar com outras series
    '''
    url = 'http://www.ipeadata.gov.br/ExibeSerie.aspx?serid='
    html = requests.get(url+serie)
    print(html)
    soup = BeautifulSoup(html.text, 'html.parser')
    txt = soup.get_text()
    dados = soup.find(id="grd_DXMainTable").get_text()
    dados = dados.split('\n\n')
    #limpeza
    p1 = [_ for _ in dados if _]  # retira strings nulas
    print(p1[1])  # nome da serie
    print(txt.split('Fonte: ')[1].split('Unidade: ')[0]) # Fonte
    print(txt.split('Comentário: ')[1].split('\n')[0]) # Fonte
    p2 = p1[2:]  # retira cabeçalho
    datas = [_[:7] for _ in p2]
    valores = [float(_[7:].replace('.','').replace(',','.')) for _ in p2]
    print('DESDE', datas[0], '|', valores[0])
    print('ATÉ  ', datas[-1], '|', valores[-1])
    df = pd.DataFrame({
        "IPCAib": valores,
        "Data": datas,
        })
    df.index = df['Data']
    return df


class IPCA:
    def __init__(self):
        self.serie = importar_dados_ipea(serie='36482')

    def deflacionar(self, valor, data, data_ref):
        """
        Deflaciona pelo IPCA um valor dado, usando o mês da data de referência.
        
        Se data é ano, usa o mês de dezembro.
        
        valor: float    valor em reais a ser deflacionado
        data: datetime ou YYYY [string ou int]  
        data_ref: datetime ou YYYY [string ou int]  
        """
        valor = float(valor)
        deflacionado, d, dR = None, None, None
        if type(data) is datetime.datetime:
            ano = str(data.year)
            mes = str(data.month)
            if len(mes) == 1: mes = "0" + mes
            d = ano + '.' + mes
        else:
            ano = str(data)
            if len(ano) == 4 and ano.isdigit():
                d = ano + '.12'
        if type(data_ref) is datetime.datetime:
            ano = str(data_ref.year)
            mes = str(data_ref.month)
            if len(mes) == 1: mes = "0" + mes
            dR = ano + '.' + mes
        else:
            ano = str(data_ref)
            if len(ano) == 4 and ano.isdigit():
                dR = ano + '.12'
        if d and dR:
            v = self.serie.loc[d]['IPCAib']
            vR = self.serie.loc[dR]['IPCAib']
            deflacionado = valor * (vR/v)    
        return deflacionado




