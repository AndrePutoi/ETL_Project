import pandas as pd
import os
import requests

def get_short_indicators_name():
    short_indicators = {
    "NE.GDI.TOTL.ZS": "GrossCapitalFormation",
    "NY.GNS.ICTR.ZS": "GrossSavings",
    "NE.IMP.GNFS.ZS": "ImportsGDP",
    "NV.IND.TOTL.ZS": "IndustryValueAdded",
    "FP.CPI.TOTL.ZG": "InflationCPI",
    "NY.GDP.DEFL.KD.ZG": "InflationDeflator",
    "NV.MNF.TECH.ZS.UN": "HighTechManufacturing",
    "GC.REV.XGRT.GD.ZS": "RevenueExclGrants",
    "DT.DOD.DSTC.IR.ZS": "ShortTermDebt",
    "DT.TDS.DECT.EX.ZS": "TotalDebtService",
    "FI.RES.TOTL.CD": "TotalReserves",
    "NV.AGR.TOTL.ZS": "AgricultureValueAdded",
    "GC.DOD.TOTL.GD.ZS": "CentralGovDebt",
    "GC.XPN.TOTL.GD.ZS": "ExpenseGDP",
    "NE.EXP.GNFS.ZS": "ExportsGDP",
    "DT.DOD.DECT.GN.ZS": "ExternalDebtStocks",
    "NY.GDP.MKTP.CD": "GDPcurrentUSD",
    "NY.GDP.MKTP.KD.ZG": "GDPgrowth",
    "NY.GDP.PCAP.CD": "GDPperCapita",
    "NY.GDP.PCAP.KD.ZG": "GDPperCapitaGrowth"
}
    return short_indicators
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
        "WB_Code": item.get("id"),
        "Name": item.get("name"),
        "Description": item.get("sourceNote", "não disponível"),
        "Source": item.get("source", {}).get("value", "não disponível"),
        "Periodicity": item.get("periodicity", "Anual"),
        "Topics": item.get("topics", [{}])[0].get("value", "não definido")
    }

    return pd.DataFrame([dados])




if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    OUTPUT_DIR = os.path.join(BASE_DIR, "..", "Generated_data")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    short_indicators = get_short_indicators_name()

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