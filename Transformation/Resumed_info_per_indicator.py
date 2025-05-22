import pandas as pd
import os

# Base será sempre onde está este ficheiro (Transformation/)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Diretório onde estão os datasets concatenados
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "Semi_complete_data")

def read_csv_file(file_path):
    """Lê um ficheiro CSV e retorna um DataFrame"""
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"[x] Erro ao ler {file_path}: {e}")
        return None

def get_info_from_df(df):
    """Extrai informações úteis de um DataFrame"""
    if df is None or df.empty:
        print("[!] DataFrame vazio ou inválido\n")
        return None
    print("[✓] DataFrame lido com sucesso\n")
    print("-" * 100, "\n")

    print(f"[✓] Preciew do DataFrame:\n{df.head()}\n")
    print("-" * 100, "\n")

    print(f"[✓] Informações do DataFrame:\n{df.info()}\n")
    print("-" * 100, "\n")

    print(f"[✓] Descrição do DataFrame:\n{df.describe()}\n")
    print("-" * 100, "\n")

    print(f"[✓] Valores nulos:\n{df.isnull().sum()}\n")
    print("-" * 100, "\n")

    print(f"[✓] Existencia de duplicados:\n{df.duplicated().sum()}\n")
    print("-" * 100, "\n")

if __name__ == "__main__":
    # Diretório onde estão os datasets concatenados
    OUTPUT_DIR = os.path.join(BASE_DIR, "Semi_complete_data")

    # Lista todos os ficheiros CSV no diretório
    csv_files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith(".csv")]

    # Lê e extrai informações de cada ficheiro CSV
    for csv_file in csv_files:
        file_path = os.path.join(OUTPUT_DIR, csv_file)
        df = read_csv_file(file_path)
        get_info_from_df(df)




