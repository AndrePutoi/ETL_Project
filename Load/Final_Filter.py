import pandas as pd
import os


# Caminho para o diretório de dados
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "Extration", "Generated_data")

# Verificar ficheiro dos dados sobre os paises
def country_dataset_verification():
    print('--' * 20)
    csv_file_path = os.path.join(DATA_DIR, "Pais_metadados.csv")
    df_countries = pd.read_csv(csv_file_path)

    # Valores nulos
    print(f"\nValores nulos no DataFrame: \n{df_countries.isnull().sum()}")

    # Retirar a coluna 'SubRegion
    if 'SubRegion' in df_countries.columns:
        df_countries.drop(columns=['SubRegion'], inplace=True)
        print("\nColuna 'SubRegion' removida do DataFrame.")
        df_countries.to_csv(csv_file_path, index=False)

    # Verificar os paises com falta das coordenadas
    missing_coordinates = df_countries[df_countries['Latitude'].isnull() | df_countries['Longitude'].isnull()]

    if not missing_coordinates.empty:
        print("\nPaíses com coordenadas faltando:")
        print(missing_coordinates[['Country', 'Latitude', 'Longitude']])
        print("\nA seguir, serão substituídas as coordenadas manualmente.")
        # Substituindo manualmente pelas coordenadas
        df_countries.loc[df_countries['Country'] == 'Channel Islands', ['Latitude', 'Longitude']] = [49.3723, -2.3644]
        df_countries.loc[df_countries['Country'] == 'Curacao', ['Latitude', 'Longitude']] = [12.1696, -68.9900]
        df_countries.loc[df_countries['Country'] == 'Gibraltar', ['Latitude', 'Longitude']] = [36.1408, -5.3536]
        df_countries.loc[df_countries['Country'] == 'St. Martin (French part)', ['Latitude', 'Longitude']] = [18.0708,-63.0501]
        df_countries.loc[df_countries['Country'] == 'West Bank and Gaza', ['Latitude', 'Longitude']] = [32.0000,35.3833]
        df_countries.loc[df_countries['Country'] == 'Sint Maarten (Dutch part)', ['Latitude', 'Longitude']] = [18.0425,-63.0548]
    else:
        print("\nTodos os países possuem coordenadas geográficas.")



    # Verificar quais paises com Capital em falta
    missing_capitals = df_countries[df_countries['Capital'].isnull()]
    if not missing_capitals.empty:
        print("\nPaíses com capital faltando:")
        print(missing_capitals[['Country', 'Capital']])
        print("\nA seguir, serão substituídas as capitais manualmente.")
        # Substituindo manualmente as capitais
        df_countries.loc[df_countries['Country'] == 'Channel Islands', 'Capital'] = 'St. Helier'
        df_countries.loc[df_countries['Country'] == 'Gibraltar', 'Capital'] = 'Gibraltar'
        df_countries.loc[df_countries['Country'] == 'Hong Kong SAR, China', 'Capital'] = 'Hong Kong'
        df_countries.loc[df_countries['Country'] == 'Israel', 'Capital'] = 'Jerusalem'
        df_countries.loc[df_countries['Country'] == 'Macao SAR, China', 'Capital'] = 'Macao'
        df_countries.loc[df_countries['Country'] == 'West Bank and Gaza', 'Capital'] = 'Ramallah'
    else:
        print("\nTodos os países possuem capital definida.")




    # Guardando as alterações no ficheiro CSV
    df_countries.to_csv(csv_file_path, index=False)

    print('\n','--' * 20)



    return

def dates_dataset_verification():
    print('\n','--' * 20)
    csv_file_path = os.path.join(DATA_DIR, "anos_enriquecidos.csv")
    df_dates = pd.read_csv(csv_file_path)

    # Valores nulos
    print(f"\nValores nulos no DataFrame: \n{df_dates.isnull().sum()}")



    print('\n','--' * 20)

    return

def indicators_dataset_verification():
    print('\n','--' * 20)
    csv_file_path = os.path.join(DATA_DIR, "metadados_indicadores.csv")
    df_indicators = pd.read_csv(csv_file_path)

    # Valores nulos
    print(f"\nValores nulos no DataFrame: \n{df_indicators.isnull().sum()}")
    print('\n','--' * 20)

    return

if __name__ == "__main__":
    country_dataset_verification()
    dates_dataset_verification()
    indicators_dataset_verification()
    print("Verificação concluída!")
