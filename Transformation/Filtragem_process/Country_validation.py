import pandas as pd
import os

# Diretório base onde está este script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Diretório onde estão os CSVs filtrados
DATA_DIR = os.path.join(BASE_DIR, "Filtered_data")

# Nome do arquivo CSV que você quer processar (exemplo: "GrossCapitalFormation.csv")
csv_filename = "GrossCapitalFormation.csv"  # altere aqui para o arquivo desejado

# Caminho completo do arquivo
csv_path = os.path.join(DATA_DIR, csv_filename)

# Verifica se o arquivo existe
if not os.path.exists(csv_path):
    print(f"[!] Arquivo não encontrado: {csv_path}")
else:
    # Lê o CSV
    df = pd.read_csv(csv_path)

    # Verifica se a coluna 'codigo_pais' existe
    if 'codigo_pais' not in df.columns:
        print("[!] Coluna 'codigo_pais' não encontrada no arquivo.")
    else:
        # Extrai a lista única de países
        unique_countries = df['codigo_pais'].unique()

        # Cria um DataFrame para salvar
        df_countries = pd.DataFrame(unique_countries, columns=['codigo_pais'])

        # Salva o arquivo CSV com os códigos dos países
        output_path = "../../dags/lista_paises_extraida.csv"
        df_countries.to_csv(output_path, index=False)

        print(f"[✓] Lista de países salva em: {output_path}")
