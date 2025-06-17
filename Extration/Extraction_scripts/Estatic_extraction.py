import os
import time
import pandas as pd
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
# Caminho da raiz: Extration/Estatic_data/ids-csv-zip-3-mb-/IDSData.csv
DATA_DIR = os.path.join(BASE_DIR, "..", "Estatic_data", "ids-csv-zip-3-mb-")
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "Estatic_Economy_Data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

## TODO:

# 1º Extrair os indicadores desejados e verificar os em falta (indicadores não presentes no dataset estático)
# 2º Formatar os dados no formato desejado

def get_short_indicators_name():
    """
    Obtém os nomes curtos dos indicadores de economia do World Bank, personalizados.
    """
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

def extract_util_indicators_data():
    """
    Extrai os dados de utilidade pública do dataset estático.
    """
    try:
        # Lê o arquivo CSV
        file_path = os.path.join(DATA_DIR, "IDSData.csv")
        df = pd.read_csv(file_path)
        logger.info(f"Arquivo lido: {file_path}")

        # Remover colunas desnecessárias
        columns_to_drop = ["Indicator Name"]
        df.drop(columns=columns_to_drop, inplace=True, errors='ignore')

        # Filtra os indicadores desejados
        short_indicators = get_short_indicators_name()
        df_filtered = df[df['Indicator Code'].isin(short_indicators.keys())]

        # Filtrar pelos anos de interesse (1970 a 2010)
        df_filtered = df_filtered.drop(columns=[f'{year}' for year in range(2022,2026)]+[f'{year}' for year in range(1970,1990)])

        # Salva o DataFrame filtrado em um novo arquivo CSV
        output_file_path = os.path.join(OUTPUT_DIR, "filtered_util_indicator_data.csv")
        df_filtered.to_csv(output_file_path, index=False)

        logger.info(f"Dados extraídos e salvos em {output_file_path}")
        return df_filtered

    except Exception as e:
        logger.error(f"Erro ao extrair dados: {e}")
        return None

def extract_missing_indicators():
    """
    Extrai indicadores que estão faltando no dataset estático.
    """
    try:
        # Lê o arquivo CSV
        file_path = os.path.join(DATA_DIR, "IDSData.csv")
        df = pd.read_csv(file_path)
        logger.info(f"Arquivo lido: {file_path}")

        # Obtém os indicadores já presentes
        existing_indicators = set(df['Indicator Code'].unique())
        short_indicators = set(get_short_indicators_name().keys())

        # Identifica os indicadores que estão faltando
        missing_indicators = short_indicators - existing_indicators

        if missing_indicators:
            logger.info(f"Indicadores faltando: {missing_indicators}")
            # Cria um DataFrame com os indicadores faltantes
            missing_data = {
                "Indicator Code": list(missing_indicators),
                "Indicator Name": [get_short_indicators_name()[code] for code in missing_indicators]
            }
            missing_df = pd.DataFrame(missing_data)
            # Salva os indicadores faltantes em um novo arquivo CSV
            missing_file_path = os.path.join(OUTPUT_DIR, "missing_indicators.csv")
            missing_df.to_csv(missing_file_path, index=False)
            return missing_indicators
        else:
            logger.info("Todos os indicadores estão presentes.")
            return None

    except Exception as e:
        logger.error(f"Erro ao extrair indicadores faltantes: {e}")
        return None

if __name__ == "__main__":
    logger.info("Iniciando extração de indicadores de economia...")

    # Extrai os dados de utilidade pública
    util_data = extract_util_indicators_data()

    if util_data is not None:
        logger.info("Dados de utilidade pública extraídos com sucesso.")
    else:
        logger.error("Falha ao extrair dados de utilidade pública.")

    # Extrai indicadores que estão faltando
    missing_indicators = extract_missing_indicators()

    if missing_indicators is not None:
        logger.info(f"Indicadores faltantes extraídos: {missing_indicators}")
    else:
        logger.info("Nenhum indicador faltante encontrado.")

    logger.info("Extração concluída.")



