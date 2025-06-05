import pandas as pd
import os


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


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "Interpolation_imputation", "Imputed_data")
OUTPUT_DIR = os.path.join(BASE_DIR, "Final_Aggregation")
os.makedirs(OUTPUT_DIR, exist_ok=True)

indicator_map = get_short_indicators_name()

merged_df = None
missing_files = []

for indicator_code, short_name in indicator_map.items():
    file_path = os.path.join(DATA_DIR, f"{short_name}_imputed.csv")

    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        missing_files.append(short_name)
        continue

    df = pd.read_csv(file_path)
    df = df[['codigo_pais', 'ano', 'valor_interpolated']]  # Seleciona apenas colunas necessárias
    df = df.rename(columns={'valor_interpolated': short_name})

    if merged_df is None:
        merged_df = df[['codigo_pais', 'ano', short_name]]
    else:
        merged_df = pd.merge(merged_df, df[['codigo_pais', 'ano', short_name]], on=['codigo_pais', 'ano'], how='outer')

# Renomear colunas
merged_df = merged_df.rename(columns={'codigo_pais': 'ISO3_Code', 'ano': 'year'})

# 🔗 Juntar com metadados
metadata_file = os.path.join(BASE_DIR, "..", "..", "Extration/Generated_data/Pais_metadados.csv")
metadata_df = pd.read_csv(metadata_file)
metadata_df = metadata_df.rename(columns={'codigo_pais': 'ISO3_Code'})

required_metadata_cols = {'ISO3_Code'}
if required_metadata_cols.issubset(metadata_df.columns):
    merged_df = pd.merge(merged_df, metadata_df, on='ISO3_Code', how='left')
else:
    print(f"⚠️ Colunas ausentes nos metadados: {required_metadata_cols - set(metadata_df.columns)}")

# 🔗 Juntar com anos enriquecidos
years_file = os.path.join(BASE_DIR, "..", "..", "Extration/Generated_data/anos_enriquecidos.csv")
years_df = pd.read_csv(years_file)

required_years_cols = {'year'}
if required_years_cols.issubset(years_df.columns):
    merged_df = pd.merge(merged_df, years_df, on='year', how='left')
else:
    print(f"⚠️ Colunas ausentes no arquivo de anos: {required_years_cols - set(years_df.columns)}")

# Ordena por país e ano
merged_df = merged_df.sort_values(by=['ISO3_Code', 'year'])

# Exporta CSV final
output_file = os.path.join(OUTPUT_DIR, "Aggregated_Indicators.csv")
merged_df.to_csv(output_file, index=False)

print(f"\n✅ Arquivo salvo em: {output_file}")

if missing_files:
    print(f"⚠️ Indicadores com arquivos ausentes: {', '.join(missing_files)}")
