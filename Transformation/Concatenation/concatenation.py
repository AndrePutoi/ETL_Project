import os
import pandas as pd
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


# Base será sempre onde está este ficheiro (Transformation/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Diretório onde estão as pastas por indicador (já com nomes curtos)
API_DATA_DIR = os.path.join(BASE_DIR, "..", "..", "Extration", "Api_Economy_Data")

# Diretório onde vais guardar os datasets concatenados
OUTPUT_DIR = os.path.join(BASE_DIR, "Concatenated_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def concatenate_csvs_in_folder(indicator_folder):
    """Concatena todos os CSVs dentro da pasta de um indicador"""
    folder_path = os.path.join(API_DATA_DIR, indicator_folder)
    if not os.path.isdir(folder_path):
        logger.warning(f"Ignorado (não é pasta): {indicator_folder}")
        return

    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    if not csv_files:
        logger.warning(f"Sem CSVs encontrados na pasta: {indicator_folder}")
        return

    dfs = []
    for csv_file in csv_files:
        file_path = os.path.join(folder_path, csv_file)
        try:
            df = pd.read_csv(file_path)
            dfs.append(df)
            logger.debug(f"Lido CSV: {file_path} com {len(df)} linhas")
        except Exception as e:
            logger.error(f"Erro ao ler {csv_file} em {indicator_folder}: {e}")

    if dfs:
        concatenated = pd.concat(dfs, ignore_index=True)
        output_path = os.path.join(OUTPUT_DIR, f"{indicator_folder}.csv")
        try:
            concatenated.to_csv(output_path, index=False)
            logger.info(f"{indicator_folder}: {len(dfs)} arquivos concatenados e salvos em {output_path}")
        except Exception as e:
            logger.error(f"Erro ao salvar arquivo concatenado {output_path}: {e}")
    else:
        logger.warning(f"Nenhum CSV válido para concatenar na pasta: {indicator_folder}")


if __name__ == "__main__":
    logger.info(f"Iniciando concatenação de CSVs em {API_DATA_DIR}")
    indicator_folders = os.listdir(API_DATA_DIR)
    for folder in indicator_folders:
        concatenate_csvs_in_folder(folder)
    logger.info("Processo de concatenação concluído.")
