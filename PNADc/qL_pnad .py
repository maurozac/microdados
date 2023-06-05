"""Constroi quocientes locacionas a partir do dados da PNAD

Parâmetros:
. 146 estratos populacionais
. 68 atividades econômicas (compatível com MIP)
. métrica: VD4019 - Rendim. habitual qq trabalho
"""

import os
import json

import pandas as pd


__version__ = "1.0"
__author__ = "Mauro Zackiewicz" 
__email__ = "maurozac@gmail.com"
__license__ = "New BSD License"


#1 acessar diretorios criadas com parser_pnad.py
home = os.path.expanduser('~')
destino = home + "/dados/PNADc/"
temp = destino + "temp/"

#2 carregar nformações sobre os estratos populacionais da PNAD
est = pd.read_csv('Municipios_por_Estratos.csv', sep=';')
estratos = est['Código do estrato'].unique()   # 146 estratos


#3 setorizar e consolidar os dados brutos
def setorizar68(data):
    """Mapeia dados CNAE da PNAD para 68 setores da MIP
    """
    # primeiro seleciona campos idade e trabalho
    trabalha = data.loc[data['V2009']>=14].loc[data['V4009'].notnull()]
    s0191 = trabalha.loc[data['V4013'].isin(['01101','01102','01103',
        '01104','01105','01106','01107','01108','01109','01110','01111'
        '01112','01113','01114','01115','01116','01117','01118','01119',
        '01401',])]
    s0192 = trabalha.loc[data['V4013'].isin(['01201','01202','01203',
        '01204','01205','01206','01207','01208','01209','01402','01500',
        '01999',])]
    s0280 = trabalha.loc[data['V4013'].isin(['02000','03001','03002'])]
    s0580 = trabalha.loc[data['V4013'].isin(['05000','08001','08002','08009'])]
    s0680 = trabalha.loc[data['V4013'].isin(['06000','09000'])]
    s0791 = trabalha.loc[data['V4013'].isin(['07002'])]
    s0792 = trabalha.loc[data['V4013'].isin(['07001'])]
    s1091 = trabalha.loc[data['V4013'].isin(['10010','10030',])]
    s1092 = trabalha.loc[data['V4013'].isin(['10092'])]
    s1093 = trabalha.loc[data['V4013'].isin(['10021','10022','10091','10093',
        '10099'])]
    s1100 = trabalha.loc[data['V4013'].isin(['11000'])]
    s1200 = trabalha.loc[data['V4013'].isin(['12000'])]
    s1300 = trabalha.loc[data['V4013'].isin(['13001','13002'])]
    s1400 = trabalha.loc[data['V4013'].isin(['14001','14002'])]
    s1500 = trabalha.loc[data['V4013'].isin(['15011','15012','15020'])]
    s1600 = trabalha.loc[data['V4013'].isin(['16001','16002'])]
    s1700 = trabalha.loc[data['V4013'].isin(['17001','17002'])]
    s1800 = trabalha.loc[data['V4013'].isin(['18000'])]
    s1991 = trabalha.loc[data['V4013'].isin(['19010','19020'])]
    s1992 = trabalha.loc[data['V4013'].isin(['19030'])]
    s2091 = trabalha.loc[data['V4013'].isin(['20090'])]
    s2092 = trabalha.loc[data['V4013'].isin(['20010'])]
    s2093 = trabalha.loc[data['V4013'].isin(['20020'])]
    s2100 = trabalha.loc[data['V4013'].isin(['21000'])]
    s2200 = trabalha.loc[data['V4013'].isin(['22010','22020'])]
    s2300 = trabalha.loc[data['V4013'].isin(['23010','23091','23099'])]
    s2491 = trabalha.loc[data['V4013'].isin(['24001'])]
    s2492 = trabalha.loc[data['V4013'].isin(['24002','24003'])]
    s2500 = trabalha.loc[data['V4013'].isin(['25001','25002'])]
    s2600 = trabalha.loc[data['V4013'].isin(['26010','26020','26030','26041',
        '26042'])]
    s2700 = trabalha.loc[data['V4013'].isin(['27010','27090'])]
    s2800 = trabalha.loc[data['V4013'].isin(['28000'])]
    s2991 = trabalha.loc[data['V4013'].isin(['29001',])]
    s2992 = trabalha.loc[data['V4013'].isin(['29002','29003'])]
    s3000 = trabalha.loc[data['V4013'].isin(['30010','30020','30030','30090'])]
    s3180 = trabalha.loc[data['V4013'].isin(['31000','32001','32002','32003',
        '32009'])]
    s3300 = trabalha.loc[data['V4013'].isin(['33001','33002'])]
    s3500 = trabalha.loc[data['V4013'].isin(['35010','35021','35022'])]
    s3680 = trabalha.loc[data['V4013'].isin(['36000','37000','38000','39000'])]
    s4180 = trabalha.loc[data['V4013'].isin(['41000','42000','43000'])]
    s4500 = trabalha.loc[data['V4013'].isin(['45010','45020','45030','45040'])]
    s4680 = trabalha.loc[data['V4013'].isin(['48010','48020','48030','48041',
        '48042','48050','48060','48071','48072','48073','48074','48075','48076',
        '48077','48078','48079','48080','48090','48100'])]
    s4900 = trabalha.loc[data['V4013'].isin(['49010','49030','49040','49090'])]
    s5000 = trabalha.loc[data['V4013'].isin(['50000'])]
    s5100 = trabalha.loc[data['V4013'].isin(['51000'])]
    s5280 = trabalha.loc[data['V4013'].isin(['52010','52020','53001','53002'])]
    s5500 = trabalha.loc[data['V4013'].isin(['55000'])]
    s5600 = trabalha.loc[data['V4013'].isin(['56011','56012','56020'])]
    s5800 = trabalha.loc[data['V4013'].isin(['58000'])]
    s5980 = trabalha.loc[data['V4013'].isin(['59000','60001','60002'])]
    s6100 = trabalha.loc[data['V4013'].isin(['61000'])]
    s6280 = trabalha.loc[data['V4013'].isin(['62000','63000'])]
    s6480 = trabalha.loc[data['V4013'].isin(['64000','65000','66001','66002'])]
    s6800 = trabalha.loc[data['V4013'].isin(['68000'])]
    s6980 = trabalha.loc[data['V4013'].isin(['69000','70000'])]
    s7180 = trabalha.loc[data['V4013'].isin(['71000','72000'])]
    s7380 = trabalha.loc[data['V4013'].isin(['73010','73020','74000','75000'])]
    s7700 = trabalha.loc[data['V4013'].isin(['77010','77020'])]
    s7880 = trabalha.loc[data['V4013'].isin(['78000','79000','81011','81012',
        '81020','82001','82001','82002','82003','82009'])]
    s8000 = trabalha.loc[data['V4013'].isin(['80000'])]
    s8400 = trabalha.loc[data['V4013'].isin(['84011','84012','84013','84014',
        '84015','84016','84017','84020'])]
    # educação => aqui feito sem separar, imputar iguais (para gerar qL)
    s8591 = trabalha.loc[data['V4013'].isin(['85011','85012','85013','85014',
        '85021','85029'])]
    s8592 = trabalha.loc[data['V4013'].isin(['85011','85012','85013','85014',
        '85021','85029'])]
    # saúde => aqui feito sem separar, imputar iguais (para gerar qL)
    s8691 = trabalha.loc[data['V4013'].isin(['86001','86002','86003','86004',
        '86009'])]
    s8692 = trabalha.loc[data['V4013'].isin(['85011','85012','85013','85014',
        '85021','85029'])]
    s9080 = trabalha.loc[data['V4013'].isin(['90000','91000','92000','93011',
        '93012','93020'])]
    s9480 = trabalha.loc[data['V4013'].isin(['94010','94020','94091','94099',
        '95010','95030','96010','96020','96030','96090'])]
    s9700 = trabalha.loc[data['V4013'].isin(['97000'])]

    setores = {
        '0191': s0191,'0192': s0192,'0280': s0280,'0580': s0580,
        '0680': s0680,'0791': s0791,'0792': s0792,'1091': s1091,
        '1092': s1092,'1093': s1093,'1100': s1100,'1200': s1200,
        '1300': s1300,'1400': s1400,'1500': s1500,'1600': s1600,
        '1700': s1700,'1800': s1800,'1991': s1991,'1992': s1992,
        '2091': s2091,'2092': s2092,'2093': s2093,'2100': s2100,
        '2200': s2200,'2300': s2300,'2491': s2491,'2492': s2492,
        '2500': s2500,'2600': s2600,'2700': s2700,'2800': s2800,
        '2991': s2991,'2992': s2992,'3000': s3000,'3180': s3180,
        '3300': s3300,'3500': s3500,'3680': s3680,'4180': s4180,
        '4500': s4500,'4680': s4680,'4900': s4900,'5000': s5000,
        '5100': s5100,'5280': s5280,'5500': s5500,'5600': s5600,
        '5800': s5800,'5980': s5980,'6100': s6100,'6280': s6280,
        '6480': s6480,'6800': s6800,'6980': s6980,'7180': s7180,
        '7380': s7380,'7700': s7700,'7880': s7880,'8000': s8000,
        '8400': s8400,'8591': s8591,'8592': s8592,'8691': s8691,
        '8692': s8692,'9080': s9080,'9480': s9480,'9700': s9700,
    }
    return setores


def consolidar(setores, estratos):
    '''
    VD4020: Rendim. efetivo qq trabalho
    VD4019: Rendim. habitual qq trabalho
    VD4016: Rendim. habitual trab. princ.

    '''
    metrica = 'VD4019'
    to_df = []
    for e in estratos:
        linha = {}
        for s in setores.keys():
            df = setores[s]
            df = df[df['Estrato'].str.contains(r'^'+str(e))]
            linha[s] = df[metrica].multiply(df['V1028']).sum()
        to_df.append(linha)

    consolidado = pd.DataFrame(to_df, index=estratos)
    consolidado.index.name = 'Estratos'
    return consolidado


ano = '2020'
csvs = os.listdir(temp) # lista dos arquivos dentro do diretorio

with open('campos.json') as f:
    campos = json.load(f)[:200]

tipos = {}
for c in campos:
    if c['tipo'] == 'cat':
        tipos[c['nome']] = str
    if c['tipo'] == 'num':
        tipos[c['nome']] = float
    
dados = []  # uma lista de dataframes
for f in csvs:
    if f.split('.')[1] != 'csv': continue
    data = pd.read_csv(temp + '/' + f, index_col=0, dtype=tipos)
    setores = setorizar68(data)
    consolidado = consolidar(setores, estratos)
    dados.append(consolidado)

final = sum(dados)  # os dataframes consolidados em um só
br = final.sum()    # totais Brasil por atividade econoômica
qL = final.divide(br, axis=1)  # qto cada atividade gera de renda em cada estrato
qL.index = [str(i) for i in qLs.index]
qL.index.name = 'Estratos'

qL.to_csv(destino+'/qLpnad'+ano+'.csv')

