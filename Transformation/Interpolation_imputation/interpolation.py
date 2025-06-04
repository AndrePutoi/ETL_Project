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
DATA_DIR = os.path.join(BASE_DIR, "..", "Filtragem_process", "Filtered_data")
OUTPUT_DIR = os.path.join(BASE_DIR, "Imputed_data")
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


def interpolate_linear_ffill_bfill(indicator_name):
    file_path = os.path.join(DATA_DIR, f"{indicator_name}.csv")
    if not os.path.exists(file_path):
        logger.warning(f"Arquivo não encontrado: {file_path}")
        return

    df = pd.read_csv(file_path)

    if df.empty or 'valor' not in df.columns:
        logger.warning(f"Dados ausentes ou coluna 'valor' não encontrada em {indicator_name}.")
        return

    logger.info(f"Iniciando interpolação para indicador: {indicator_name}")

    df_imputed_list = []

    for pais in df['codigo_pais'].unique():
        df_pais = df[df['codigo_pais'] == pais].copy()

        # Interpolação linear
        df_pais['valor_interpolated'] = df_pais['valor'].interpolate(method='linear')

        # Preenche NaNs no início com bfill
        df_pais['valor_interpolated'] = df_pais['valor_interpolated'].fillna(method='bfill')

        # Preenche NaNs no fim com ffill
        df_pais['valor_interpolated'] = df_pais['valor_interpolated'].fillna(method='ffill')

        df_imputed_list.append(df_pais)

        logger.debug(f"Interpolação concluída para país {pais} no indicador {indicator_name}")

    df_imputed = pd.concat(df_imputed_list)

    output_path = os.path.join(OUTPUT_DIR, f"{indicator_name}_imputed.csv")
    df_imputed.to_csv(output_path, index=False)
    logger.info(f"Dados interpolados e preenchidos para {indicator_name} salvos em {output_path}")


if __name__ == "__main__":
    indicators = get_short_indicators_name()
    logger.info("Iniciando interpolação linear + ffill + bfill para todos os indicadores")
    for code, indicator_name in indicators.items():
        interpolate_linear_ffill_bfill(indicator_name)
    logger.info("Interpolação concluída para todos os indicadores!")
