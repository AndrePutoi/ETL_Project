import pandas as pd
import os

# Diretório base onde está este script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Diretório onde estão os CSVs a serem limpos
DATA_DIR = os.path.join(BASE_DIR, "Semi_complete_data")

# Diretório de saída para guardar os ficheiros limpos
OUTPUT_DIR = os.path.join(BASE_DIR, "Cleaned_data")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def read_csv_file(file_path):
    """Lê um ficheiro CSV e retorna um DataFrame"""
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception as e:
        print(f"[x] Erro ao ler {file_path}: {e}")
        return None


def validate_country_data(df):
    """Devolve lista de países com todos os valores nulos"""
    list_countries_invalid = []
    for country in df['pais'].unique():
        country_df = df[df['pais'] == country]
        if country_df['valor'].isnull().all():
            list_countries_invalid.append(country)
    return list_countries_invalid


def validate_indicator_data(csv_files):
    """Devolve lista de indicadores com mais de metade dos valores nulos"""
    dict_indicators_invalid = {csv_file: False for csv_file in csv_files}
    for csv_file in csv_files:
        df = read_csv_file(os.path.join(DATA_DIR, csv_file))
        if df is None:
            dict_indicators_invalid[csv_file] = True
            continue

        # Verifica se o indicador tem mais de 50% de valores nulos
        total_values = len(df)
        null_values = df['valor'].isnull().sum()
        if null_values / total_values > 0.80:
            dict_indicators_invalid[csv_file] = True

    return dict_indicators_invalid


def interpolate_imputation(df):
    """Preenche valores nulos com interpolação, ffill e bfill"""
    # Garante que 'ano' é numérico para ordenar corretamente
    df['ano'] = pd.to_numeric(df['ano'], errors='coerce')
    df = df.sort_values(by=["pais", "ano"])

    df['valor'] = (
        df.groupby('pais')['valor']
        .transform(lambda g: g.interpolate()).fillna(method='ffill').fillna(method='bfill')
    )
    return df


if __name__ == "__main__":
    csv_files = [f for f in os.listdir(DATA_DIR) if f.endswith(".csv")]
    dict_invalid_countries = {}

    dict_invalid_indicators = validate_indicator_data(csv_files)
    list_countries = []


    for csv_file in csv_files:
        print(f"\n[•] A processar: {csv_file}")
        file_path = os.path.join(DATA_DIR, csv_file)
        df = read_csv_file(file_path)

        if df is None:
            continue

        # 1. Valida os países
        invalid_countries = validate_country_data(df)
        dict_invalid_countries[csv_file] = invalid_countries
        print(f"[✓] Países com todos os valores nulos: {invalid_countries}")


        # 3. Remove países inválidos
        df = df[~df['pais'].isin(invalid_countries)]

        # 4. Imputação por interpolação e preenchimento
        df = interpolate_imputation(df)

        # 5. Guardar ficheiro limpo
        output_path = os.path.join(OUTPUT_DIR, csv_file)
        df.to_csv(output_path, index=False)
        print(f"[✓] Guardado: {output_path}")

    print("\nResumo dos países inválidos por ficheiro:")
    for k, v in dict_invalid_countries.items():
        print(f"{k}: {v}")

    print("\nResumo dos indicadores inválidos por ficheiro:")
    for k, v in dict_invalid_indicators.items():
        if v:
            print(f"[x] {k} tem mais de 80% de valores nulos.")





