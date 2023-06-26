"""
Dados consolidados para todos os aeroportos brasileiros, somando embarques e desembarques, inclusive internacionais, e volume de carga transportada.
"""
import pandas as pd


ano = "2020"
meses = ['-01','-02','-03','-04','-05','-06','-07','-08','-09','-10','-11','-12']
url = "https://www.gov.br/anac/pt-br/assuntos/regulados/empresas-aereas/Instrucoes-para-a-elaboracao-e-apresentacao-das-demonstracoes-contabeis/envio-de-informacoes/combinada/"

E, D = {}, {}
dE, dD = [], []

# para o ano todo
for m in meses:
    print(ano, m)
    comb = pd.read_csv(url+ano+'/combinada'+ano+m+'.zip', encoding="latin_1", sep=';', low_memory=False)

    # E: embarques
    origens = comb['sg_icao_origem'].unique()
    f1 = comb[comb['nm_pais_origem'] == 'BRASIL']

    todf = []
    index = []
    for o in origens:
        f2 = f1[f1['sg_icao_origem'] == o]
        paxP = f2['nr_passag_pagos'].sum() 
        paxG = f2['nr_passag_gratis'].sum()
        pax = paxP + paxG
        bagagem = f2['kg_bagagem_livre'].sum() + f2['kg_bagagem_excesso'].sum()
        cargaP = f2['kg_carga_paga'].sum() 
        cargaG = f2['kg_carga_gratis'].sum()
        carga = cargaP + cargaG
        correio = f2['kg_correio'].sum()
        total = pax*75 + bagagem + carga + correio
        if pax:
            nome = f2['nm_aerodromo_origem'].unique()[0]
            mun = f2['nm_municipio_origem'].unique()[0]
            uf = f2['sg_uf_origem'].unique()[0]
            todf.append([int(paxP), int(paxG), int(bagagem), int(cargaP),
                int(cargaG), int(correio), int(total)])
            index.append(o)
            if o not in E:
                E[o] = [nome, mun, uf]

    p1 = pd.DataFrame(todf, columns=['passageiros pagos', 'passageiros grátis',
        'bagagem (kg)', 'carga paga (kg)', 'carga grátis (kg)', 'correio (kg)',
        'total (kg)'], index=index)
    dE.append(p1)

    # D: desembarques
    destinos = comb['sg_icao_destino'].unique()
    f1 = comb[comb['nm_pais_destino'] == 'BRASIL']

    todf = []
    index = []
    for o in destinos:
        f2 = f1[f1['sg_icao_destino'] == o]
        paxP = f2['nr_passag_pagos'].sum() 
        paxG = f2['nr_passag_gratis'].sum()
        pax = paxP + paxG
        bagagem = f2['kg_bagagem_livre'].sum() + f2['kg_bagagem_excesso'].sum()
        cargaP = f2['kg_carga_paga'].sum() 
        cargaG = f2['kg_carga_gratis'].sum()
        carga = cargaP + cargaG
        correio = f2['kg_correio'].sum()
        total = pax*75 + bagagem + carga + correio
        if pax:
            nome = f2['nm_aerodromo_destino'].unique()[0]
            mun = f2['nm_municipio_destino'].unique()[0]
            uf = f2['sg_uf_destino'].unique()[0]
            #print(o, nome, int(pax), int(total), mun, uf)
            todf.append([int(paxP), int(paxG), int(bagagem), int(cargaP),
                int(cargaG), int(correio), int(total)])
            index.append(o)
            if o not in D:
                D[o] = [nome, mun, uf]

    p2 = pd.DataFrame(todf, columns=['passageiros pagos', 'passageiros grátis',
        'bagagem (kg)', 'carga paga (kg)', 'carga grátis (kg)', 'correio (kg)',
        'total (kg)'], index=index)
    dD.append(p2)

# acertar os indexes para fazer a consolidação
colunas = ['passageiros pagos', 'passageiros grátis','bagagem (kg)', 'carga paga (kg)', 
    'carga grátis (kg)', 'correio (kg)', 'total (kg)']

# total geral requer saber o index final
aeros = list(set(list(E.keys())+list(D.keys())))
aeros.sort()
total = pd.DataFrame(0.0, index=aeros, columns=colunas)
for df in dD:
    fix = df.reindex(aeros).fillna(0.0)
    total += fix
for df in dE:
    fix = df.reindex(aeros).fillna(0.0)
    total += fix

# colunas complementares
dfc = []
for cod in aeros:
    try:
        dfc.append(D[cod])
    except:
        dfc.append(E[cod])

df = pd.DataFrame(dfc, index=aeros, columns=['nome', 'município', 'uf'])

final = pd.concat([total, df], axis=1)
final.index.name = '2020'

final.to_csv('movimento'+ano+'.csv')
