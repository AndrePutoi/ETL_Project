import pandas as pd
import os
import logging

# Configuração do logger
logger = logging.getLogger("transformation")
logger.setLevel(logging.DEBUG)

file_handler = logging.FileHandler("transformation.log")
file_handler.setLevel(logging.DEBUG)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
# ===================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "Concatenation", "Concatenated_data")
FILTERED_INFO_DIR = os.path.join(BASE_DIR, "..", "Verification_Process", "Verification_files")
OUTPUT_DIR = os.path.join(BASE_DIR, "Filtered_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def get_short_indicators_name():
    return {
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


def find_invalid_countries():
    file_path = os.path.join(FILTERED_INFO_DIR, "countries_problematic_indicators.csv")
    if not os.path.exists(file_path):
        logger.error(f"Arquivo não encontrado: {file_path}")
        return []

    logger.info(f"Lendo arquivo de países problemáticos: {file_path}")
    df = pd.read_csv(file_path)
    threshold = len(get_short_indicators_name()) * 0.5
    logger.debug(f"Threshold para invalidação de países: {threshold} indicadores")

    invalid_countries = []
    for country in df['Country']:
        indicators_missing = df[df['Country'] == country]['IndicatorsWithMissingData'].iloc[0].strip().split(',')
        logger.debug(f"País {country} tem {len(indicators_missing)} indicadores faltando")
        if len(indicators_missing) > threshold:
            invalid_countries.append(country)

    logger.info(f"Países inválidos identificados: {invalid_countries}")
    return invalid_countries


def filter_data(indicator_code, indicator_name, invalid_countries):
    file_path = os.path.join(DATA_DIR, f"{indicator_name}.csv")
    if not os.path.exists(file_path):
        logger.warning(f"Arquivo não encontrado: {file_path}")
        return

    logger.info(f"Lendo dados do indicador: {indicator_name}")
    df = pd.read_csv(file_path)
    original_count = len(df)
    df_filtered = df[~df['codigo_pais'].isin(invalid_countries)]
    filtered_count = len(df_filtered)

    if df_filtered.empty:
        logger.warning(f"Nenhum dado válido encontrado para {indicator_name} após filtragem.")
        return

    output_path = os.path.join(OUTPUT_DIR, f"{indicator_name}.csv")
    df_filtered.to_csv(output_path, index=False)
    logger.info(f"Dados filtrados para {indicator_name} salvos em {output_path} ({original_count} linhas → {filtered_count} linhas)")


if __name__ == "__main__":
    logger.info("Iniciando processo de filtragem de dados.")
    invalid_countries = find_invalid_countries()
    if not invalid_countries:
        logger.warning("Nenhum país inválido encontrado, filtragem não será aplicada.")
    else:
        indicators = get_short_indicators_name()
        for code, indicator_name in indicators.items():
            filter_data(code, indicator_name, invalid_countries)
        logger.info("Filtragem concluída para todos os indicadores.")