#!/usr/bin/python
# coding=utf8
"""
 +----------+
 |  Cambio  |
 +----------+

USO

from cambio import Cambio

c = Cambio()
c.converter_a_dolar(500.0, 2012) # R$500 igual a US$240.70 em 2012
c.converter_a_real(500.0, '2015')  # U$500 igual a RS$1935.25 em 2015

import datetime
c.converter_a_dolar(500.0, datetime.datetime(2020,2,23)) # R$500 igual a US$115,19 em 2020

"""
import datetime

import pandas as pd
import requests
from bs4 import BeautifulSoup

"""
Taxa de câmbio para R$ / US$ referente a taxa comercial para compra na média do período
http://www.ipeadata.gov.br/ExibeSerie.aspx?serid=32098

Comentário: Taxa de câmbio é o preço de uma moeda estrangeira medido em unidades ou 
frações (centavos) da moeda nacional. Neste caso a moeda estrangeira é o dólar. 
Câmbio Comercial ou Livre corresponde à média das taxas efetivas de operações no 
mercado interbancário, ponderada pelo volume de transações de venda do dia. 
Taxa calculada para transações de compra.As taxas médias são calculadas para compra, 
utilizando-se as cotações diárias do período em referência

Elaboração Bacen: média no período, a partir das séries de taxas diárias. 

Elaboração Ipeadata: conversão de moeda para Real (R$) no período entre 1953.01 e 1994.05.

Nota: As transações fechadas em taxas que mais se distanciam da média do mercado (outliers) 
e aquelas que evidenciam formação artificial de preço ou contrárias às práticas regulares 
do mercado são excluídas dos cálculos.

Mais informações: SGS- Sistema de Gerenciamento de Séries Temporais, Banco Central.
"""

def importar_dados_ipea(serie='32098'):
    ''' Scrapper para Series de Dados IPEA 
    31924
    32098
    FAZER limpeza para ajustar ao formato de dados ao usar com outras series
    '''
    url = 'http://www.ipeadata.gov.br/ExibeSerie.aspx?serid='
    html = requests.get(url+serie)
    print(html)
    soup = BeautifulSoup(html.text, 'html.parser')
    txt = soup.get_text()
    data = soup.find(id="grd_DXMainTable").get_text()
    serie = data.split('\n\n')
    #limpeza
    p1 = [_ for _ in serie if _]  # retira strings nulas
    print(p1[1])  # nome da serie
    print(txt.split('Fonte: ')[1].split('Unidade: ')[0]) # Fonte
    print(txt.split('Comentário: ')[1].split('\n')[0]) # Fonte
    p2 = p1[2:]  # retira cabeçalho
    datas = [_[:7] for _ in p2]
    dolar = [float(_[7:].replace(',','.')) for _ in p2]
    print('DESDE', datas[0], '|', dolar[0])
    print('ATÉ  ', datas[-1], '|', dolar[-1])
    df = pd.DataFrame({
        "US$": dolar,
        "Data": datas,
        })
    df.index = df['Data']
    return df


class Cambio:
    """Carrega informações para executar operações de conversão
    entre R$ e US$
    """
    def __init__(self, path=""):
        self.ipea = importar_dados_ipea(serie='32098')
        

    def __repr__(self):
        r = "Câmbio R$|US$\n"
        r =+ 'Dados IPEA de ' + str(self.ipea.index[0]) + ' a ' + str(self.ipea.index[-1])
        return r


    def converter_a_dolar(self, reais, data, media=False):
        """
        Converte Reais a Dolar no mês da data de referência.

        Taxa de câmbio para R$ / US$ referente a taxa comercial para compra na média 
        do período (IpeaData)
            
        Se data é ano, usa o mês de dezembro.
        
        valor: float    valor em reais a ser deflacionado
        data: datetime ou YYYY [string ou int]  
        data_ref: datetime ou YYYY [string ou int]  
        se media=True, calcula a media para o ano
        """
        reais = float(reais)
        dolar, dt = None, None
        if type(data) is datetime.datetime:
            ano = str(data.year)
            mes = str(data.month)
            if len(mes) == 1: mes = "0" + mes
            dt = ano + '.' + mes
        else:
            ano = str(data)
            if len(ano) == 4 and ano.isdigit():
                dt = ano + '.12'
        if dt:
            i = self.ipea.loc[dt]['US$']
            dolar = reais / i    
        if media:
            meses = ['01','02','03','04','05','06',
                '07','08','09','10','11','12']
            dolar = 0.0
            for mes in meses:
                dt = ano + '.' + mes
                i = self.ipea.loc[dt]['US$']
                dolar += reais / i
            dolar = dolar/12
        return dolar


    def converter_a_real(self, dolares, data, media=False):
        """
        Converte Dolares a Real no mês da data de referência.

        Taxa de câmbio para US$ / R$ referente a taxa comercial para compra na média 
        do período (IpeaData)
            
        Se data é ano, usa o mês de dezembro.
        
        valor: float    valor em reais a ser deflacionado
        data: datetime ou YYYY [string ou int]  
        data_ref: datetime ou YYYY [string ou int]  
        se media=True, calcula a media para o ano
        """
        dolares = float(dolares)
        real, dt = None, None
        if type(data) is datetime.datetime:
            ano = str(data.year)
            mes = str(data.month)
            if len(mes) == 1: mes = "0" + mes
            dt = ano + '.' + mes
        else:
            ano = str(data)
            if len(ano) == 4 and ano.isdigit():
                dt = ano + '.12'
        if dt:
            i = self.ipea.loc[dt]['US$']
            real = dolares * i
        if media:
            meses = ['01','02','03','04','05','06',
                '07','08','09','10','11','12']
            real = 0.0
            for mes in meses:
                dt = ano + '.' + mes
                i = self.ipea.loc[dt]['US$']
                real += dolares * i
            real = real/12
        return real


