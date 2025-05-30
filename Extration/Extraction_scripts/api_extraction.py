import os
import time
import pandas as pd
import requests
import sys
import logging

# Ajusta caminho para encontrar módulos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

# ===================================
# CONFIGURAÇÃO DO LOGGER AQUI MESMO!
# ===================================
logger = logging.getLogger("extraction")
logger.setLevel(logging.INFO)

# Handler para ficheiro
file_handler = logging.FileHandler("extraction.log")
file_handler.setLevel(logging.INFO)

# Formato
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# Mostrar no terminal também
stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.INFO)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
# ===================================


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "Api_Economy_Data")
os.makedirs(DATA_DIR, exist_ok=True)


def get_indicator_metadata(indicator_code):
    url = f"http://api.worldbank.org/v2/indicator/{indicator_code}"
    params = {"format": "json"}

    response = requests.get(url, params=params)
    data = response.json()[1][0]

    return {
        "indicator_id": data["id"],
        "name": data["name"],
        "topics": [t["value"] for t in data.get("topics", [])],
        "source": data["source"]["value"]
    }


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


def get_country_codes():
    url = "http://api.worldbank.org/v2/country"
    params = {"format": "json", "per_page": 1000}

    response = requests.get(url, params=params)
    data = response.json()[1]

    country_codes = {
        item["id"]: item["name"]
        for item in data
        if item["region"]["value"] != "Aggregates"
    }

    logger.info(f"Total de países encontrados: {len(country_codes)}")
    return country_codes


def get_dataset_country_topic(topic_code, country_code):
    metadata = get_indicator_metadata(topic_code)
    topic_names = ", ".join(metadata["topics"])

    url = f"http://api.worldbank.org/v2/country/{country_code}/indicator/{topic_code}"
    params = {"format": "json", "date": "2000:2023", "per_page": 100}

    response = requests.get(url, params=params)
    data = response.json()[1]

    df = pd.DataFrame([{
        "ano": item["date"],
        "valor": item["value"],
        "pais": item["country"]["value"],
        "codigo_pais": item["country"]["id"],
        "indicador": topic_code,
        "categoria": topic_names
    } for item in data])

    return df


def save_dataset_to_csv(df, filename, indicator='Outher', path=DATA_DIR):
    indicador_dir = os.path.join(path, indicator)
    os.makedirs(indicador_dir, exist_ok=True)

    file_path = os.path.join(indicador_dir, f"{filename}_{indicator}.csv")
    df.to_csv(file_path, index=False)
    logger.info(f"Arquivo {file_path} salvo com sucesso.")


if __name__ == "__main__":
    country_codes = get_country_codes()
    short_indicators = get_short_indicators_name()

    for indicator, indicator_name in short_indicators.items():
        for country_code, country_name in country_codes.items():
            file_path = os.path.join(DATA_DIR, indicator_name, f"{country_code}_{indicator_name}.csv")
            if os.path.exists(file_path):
                logger.debug(f"Arquivo {file_path} já existe. Pulando coleta de dados.")
                continue

            logger.info(f"Coletando dados para o indicador {indicator} ({indicator_name}) "
                        f"do país {country_name} ({country_code})")

            df = get_dataset_country_topic(indicator, country_code)
            if not df.empty:
                save_dataset_to_csv(df, country_code, indicator=indicator_name)
            else:
                logger.warning(f"Sem dados para o indicador {indicator} ({indicator_name}) "
                               f"do país {country_name} ({country_code})")

            time.sleep(0.5)

        time.sleep(5)
