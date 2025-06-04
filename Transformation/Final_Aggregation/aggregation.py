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
DATA_DIR = os.path.join(BASE_DIR, "..", "Interpolation_imputation", "Imputed_data")
OUTPUT_DIR = os.path.join(BASE_DIR, "Final_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def concatenate_all_indicators():
    logger.info(f"Iniciando concatenação dos CSVs em {DATA_DIR}")

    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
    if not csv_files:
        logger.warning(f"Nenhum arquivo CSV encontrado em {DATA_DIR}")
        return

    dataframes = []
    for csv_file in csv_files:
        file_path = os.path.join(DATA_DIR, csv_file)
        try:
            df = pd.read_csv(file_path)
            dataframes.append(df)
            logger.info(f"Arquivo {csv_file} carregado com {len(df)} linhas")
        except Exception as e:
            logger.error(f"Erro ao ler {csv_file}: {e}")

    if dataframes:
        concatenated_df = pd.concat(dataframes, ignore_index=True)
        concatenated_df.drop(columns=['valor'], inplace=True, errors='ignore')
        concatenated_df.rename(columns={'codigo_pais': 'country_code','valor_interpolated':'value','indicador':'indicator','ano':'year'}, inplace=True, errors='ignore')
        output_file = os.path.join(OUTPUT_DIR, "all_indicators_concatenated.csv")
        concatenated_df.to_csv(output_file, index=False)
        logger.info(f"Concatenação concluída, arquivo salvo em: {output_file}")
    else:
        logger.warning("Nenhum dataframe para concatenar.")

if __name__ == "__main__":
    concatenate_all_indicators()
