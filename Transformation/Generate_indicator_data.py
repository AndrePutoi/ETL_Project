import pandas as pd
import os
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "Generated_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def extrair_metadados_indicador(codigo_indicador):
    """
    Extrai metadados úteis de um indicador do World Bank Open Data.

    Parâmetros:
        codigo_indicador (str): Código do indicador (ex: 'NY.GDP.MKTP.CD')

    Retorna:
        DataFrame com uma linha contendo:
        - IndicadorID
        - CodigoWB
        - Nome
        - Unidade
        - Descrição
        - Fonte
        - Frequencia
        - Tema
    """
    url = f"https://api.worldbank.org/v2/indicator/{codigo_indicador}?format=json&per_page=1000"
    resposta = requests.get(url)

    if resposta.status_code != 200:
        raise Exception(f"Erro ao acessar o indicador {codigo_indicador}: status {resposta.status_code}")

    dados_json = resposta.json()
    if not dados_json or len(dados_json) < 2 or not dados_json[1]:
        raise Exception(f"Indicador {codigo_indicador} não encontrado ou sem metadados.")

    item = dados_json[1][0]

    dados = {
        "CodigoWB": item.get("id"),
        "Nome": item.get("name"),
        "Unidade": item.get("unit", "não definido"),
        "Descrição": item.get("sourceNote", "não disponível"),
        "Fonte": item.get("source", {}).get("value", "não disponível"),
        "Frequencia": item.get("periodicity", "Anual"),
        "Tema": item.get("topics", [{}])[0].get("value", "não definido")
    }

    return pd.DataFrame([dados])

if __name__ == "__main__":
    short_indicators = {
        'SP.POP.TOTL': 'pop_total',
        'NY.GDP.MKTP.KD.ZG': 'gdp_growth',
        'NY.GDP.PCAP.CD': 'gdp_pc_usd',
        'SM.POP.TOTL': 'mig_stock_total',
        'SM.POP.TOTL.ZS': 'mig_stock_pct',
        'SP.DYN.CBRT.IN': 'birth_rate',
        'SP.DYN.LE00.IN': 'life_exp',
        'SL.UEM.TOTL.ZS': 'unemp_pct',
        'NE.IMP.GNFS.ZS': 'imports_pct_gdp',
        'NE.IMP.GNFS.CD': 'imports_usd',
        'NE.EXP.GNFS.ZS': 'exports_pct_gdp',
        'NE.EXP.GNFS.CD': 'exports_usd',
        'EG.ELC.RNEW.ZS': 'ren_elc_pct',
        'EG.FEC.RNEW.ZS': 'ren_energy_pct',
        'EG.ELC.ACCS.UR.ZS': 'elec_access_pct',
        'SE.PRM.CMPT.ZS': 'edu_prim_comp',
        'SE.TER.CUAT.DO.ZS': 'edu_phd',
        'SE.SEC.CUAT.LO.ZS': 'edu_sec',
        'SE.TER.CUAT.MS.ZS': 'edu_masters',
        'SE.TER.CUAT.BA.ZS': 'edu_bachelor'
    }
    dfs_metadados = []
    for indicator, indicator_name in short_indicators.items():
        print(f"[•] Extraindo metadados para o indicador {indicator} ({indicator_name})")
        df_metadados = extrair_metadados_indicador(indicator)
        dfs_metadados.append(df_metadados)

    # Concatena todos os DataFrames em um único DataFrame
    df_metadados_final = pd.concat(dfs_metadados, ignore_index=True)

    # Salva o DataFrame em um arquivo CSV
    output_path = os.path.join(OUTPUT_DIR, "metadados_indicadores.csv")
    df_metadados_final.to_csv(output_path, index=False)
    print(f"[•] Metadados salvos em {output_path}")
    print(df_metadados_final.head(10))