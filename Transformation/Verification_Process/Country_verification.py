import pandas as pd
import os
import logging

# ===================================
# CONFIGURAÇÃO DO LOGGER
# ===================================
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
OUTPUT_DIR = os.path.join(BASE_DIR, "Verification_files")
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

def main():
    short_indicators = get_short_indicators_name()
    country_problematic_indicators = {}

    logger.info("Iniciando verificação dos indicadores problemáticos por país.")

    for indicator_code, indicator_name in short_indicators.items():
        file_path = os.path.join(DATA_DIR, f"{indicator_name}.csv")
        if not os.path.exists(file_path):
            logger.warning(f"Arquivo não encontrado: {file_path}")
            continue

        logger.info(f"Lendo dados do indicador: {indicator_name}")
        df = pd.read_csv(file_path)

        if 'codigo_pais' not in df.columns or 'valor' not in df.columns:
            logger.error(f"Colunas 'codigo_pais' ou 'valor' não encontradas no arquivo {file_path}")
            continue

        for country, group in df.groupby('codigo_pais'):
            total = len(group)
            missing = group['valor'].isna().sum()
            missing_ratio = missing / total

            logger.debug(f"País {country} - Indicador {indicator_name}: {missing} missing de {total} ({missing_ratio:.2%})")

            if missing_ratio > 0.5:
                if country not in country_problematic_indicators:
                    country_problematic_indicators[country] = []
                country_problematic_indicators[country].append(indicator_name)

    if country_problematic_indicators:
        logger.info(f"Países com indicadores problemáticos identificados: {list(country_problematic_indicators.keys())}")
    else:
        logger.info("Nenhum país com indicadores problemáticos encontrado.")

    # Salvar resultados
    output_df = pd.DataFrame([
        {'Country': country, 'IndicatorsWithMissingData': ', '.join(indicators)}
        for country, indicators in country_problematic_indicators.items()
    ])
    output_file = os.path.join(OUTPUT_DIR, "countries_problematic_indicators.csv")
    output_df.to_csv(output_file, index=False)

    logger.info(f"Resultados salvos em: {output_file}")

if __name__ == "__main__":
    main()
