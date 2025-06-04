import os
import time
import pandas as pd
import requests
import sys
import logging

# ===================================
# CONFIGURAÇÃO DO LOGGER
# ===================================
logger = logging.getLogger("extraction")
logger.setLevel(logging.DEBUG)  # Define o nível de log

# Handler para ficheiro
file_handler = logging.FileHandler("extraction.log")
file_handler.setLevel(logging.DEBUG)

# Formato
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Mostrar no terminal também
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
# ===================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "Api_Economy_Data")
os.makedirs(DATA_DIR, exist_ok=True)

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

def get_country_codes_with_iso2_mapping():
    url = "http://api.worldbank.org/v2/country"
    params = {"format": "json", "per_page": 1000}

    response = requests.get(url, params=params)
    data = response.json()[1]

    country_codes = {}          # ISO3 → Nome
    iso2_to_iso3 = {}           # ISO2 → ISO3

    for item in data:
        if item["region"]["value"] != "Aggregates":
            iso3 = item["id"]
            iso2 = item["iso2Code"]
            name = item["name"]

            country_codes[iso3] = name
            iso2_to_iso3[iso2] = iso3

    logger.info(f"Total de países encontrados: {len(country_codes)}")
    return country_codes, iso2_to_iso3

def get_dataset_country_topic(topic_code, country_code, iso2_to_iso3):
    url = f"http://api.worldbank.org/v2/country/{country_code}/indicator/{topic_code}"
    params = {"format": "json", "date": "1990:2021", "per_page": 100}

    response = requests.get(url, params=params)
    data = response.json()[1]

    rows = []
    for item in data:
        iso2 = item["country"]["id"]
        iso3 = iso2_to_iso3.get(iso2, iso2)  # Se não encontrar, mantém ISO2

        rows.append({
            "ano": item["date"],
            "valor": item["value"],
            "codigo_pais": iso3,
            "indicador": topic_code,
        })

    df = pd.DataFrame(rows)
    return df

def save_dataset_to_csv(df, filename, indicator='Other', path=DATA_DIR):
    indicador_dir = os.path.join(path, indicator)
    os.makedirs(indicador_dir, exist_ok=True)

    file_path = os.path.join(indicador_dir, f"{filename}_{indicator}.csv")
    df.to_csv(file_path, index=False)
    logger.info(f"Arquivo {file_path} salvo com sucesso.")

if __name__ == "__main__":

    # Obtem códigos e o mapeamento ISO2 → ISO3
    country_codes, iso2_to_iso3 = get_country_codes_with_iso2_mapping()
    short_indicators = get_short_indicators_name()

    for indicator, indicator_name in short_indicators.items():
        for country_code, country_name in country_codes.items():
            file_path = os.path.join(DATA_DIR, indicator_name, f"{country_code}_{indicator_name}.csv")
            if os.path.exists(file_path):
                logger.debug(f"Arquivo {file_path} já existe. Pulando coleta de dados.")
                continue

            logger.info(f"Coletando dados para o indicador {indicator} ({indicator_name}) "
                        f"do país {country_name} ({country_code})")

            df = get_dataset_country_topic(indicator, country_code, iso2_to_iso3)
            if not df.empty:
                save_dataset_to_csv(df, country_code, indicator=indicator_name)
            else:
                logger.warning(f"Sem dados para o indicador {indicator} ({indicator_name}) "
                               f"do país {country_name} ({country_code})")

            time.sleep(0.5)

        time.sleep(5)
