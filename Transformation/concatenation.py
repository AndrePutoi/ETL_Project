import os
import pandas as pd

# Base será sempre onde está este ficheiro (Transformation/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Diretório onde estão as pastas por indicador (já com nomes curtos)
API_DATA_DIR = os.path.join(BASE_DIR, "..", "Extration", "Api_Data")

# Diretório onde vais guardar os datasets concatenados
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "Transformation", "Semi_complete_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def concatenate_csvs_in_folder(indicator_folder):
    """Concatena todos os CSVs dentro da pasta de um indicador"""
    folder_path = os.path.join(API_DATA_DIR, indicator_folder)
    if not os.path.isdir(folder_path):
        print(f"[!] Ignorado (não é pasta): {indicator_folder}")
        return

    csv_files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
    if not csv_files:
        print(f"[!] Sem CSVs: {indicator_folder}")
        return

    dfs = []
    for csv_file in csv_files:
        file_path = os.path.join(folder_path, csv_file)
        try:
            df = pd.read_csv(file_path)
            dfs.append(df)
        except Exception as e:
            print(f"[x] Erro em {csv_file}: {e}")

    if dfs:
        concatenated = pd.concat(dfs, ignore_index=True)
        output_path = os.path.join(OUTPUT_DIR, f"{indicator_folder}.csv")
        concatenated.to_csv(output_path, index=False)
        print(f"[✓] {indicator_folder}: {len(dfs)} ficheiros → {output_path}")

if __name__ == "__main__":
    indicator_folders = os.listdir(API_DATA_DIR)
    for folder in indicator_folders:
        concatenate_csvs_in_folder(folder)
