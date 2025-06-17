import pandas as pd
import logging
import os

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

def transform_and_split_indicators(input_file: str, output_folder: str = "./output"):
    try:
        logger.info("Leitura do arquivo CSV")
        df = pd.read_csv(input_file)

        # Remover colunas "Unnamed"
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
        logger.debug("Colunas 'Unnamed' removidas")

        # Renomear colunas
        df = df.rename(columns={
            "Country Code": "ISO3_Code",
            "Indicator Code": "WB_Code"
        })
        logger.debug("Colunas renomeadas")

        # Remover coluna "Country Code"
        logger.debug("Coluna 'Country Code' removida")

        # Encontrar colunas de ano
        year_columns = [col for col in df.columns if col.isdigit()]
        logger.debug(f"Colunas de ano identificadas: {year_columns[:5]}...")

        # Melt dos anos
        df_melted = df.melt(id_vars=["ISO3_Code", "WB_Code"],
                            value_vars=year_columns,
                            var_name="YEAR",
                            value_name="Value")
        df_melted["YEAR"] = df_melted["YEAR"].astype(int)
        logger.info("Transformação (melt) concluída")

        # Obter nomes curtos dos indicadores
        short_names = get_short_indicators_name()

        # Separar e salvar por indicador
        for code in df_melted["WB_Code"].unique():
            df_ind = df_melted[df_melted["WB_Code"] == code]
            short_name = short_names.get(code, code)  # usa o nome original se não estiver no dicionário
            file_name = f"{output_folder}/{short_name}.csv"
            df_ind.to_csv(file_name, index=False)
            logger.info(f"Indicador '{code}' salvo como '{file_name}'")

        logger.info("Processo concluído com sucesso.")

    except Exception as e:
        logger.error(f"Erro durante a transformação: {e}", exc_info=True)

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, "..", "..","Extration", "Estatic_Economy_Data")
    OUTPUT_DIR = os.path.join(BASE_DIR, "..", "Raw_formatted_Economy_Data")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    input_file = os.path.join(DATA_DIR, "filtered_util_indicator_data.csv")
    transform_and_split_indicators(input_file, OUTPUT_DIR)
    logger.info("Arquivo de dados transformado e salvo com sucesso.")